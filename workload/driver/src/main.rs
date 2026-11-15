//! Antithesis workload driver for Allegro Hermes.
//!
//! Focus (from Hermes' bug history): at-least-once delivery across consumer
//! rebalances, retries and offset commits (#1040, #1053, #1862, #1891, #1908,
//! #1983), and subscription lifecycle changes during delivery (suspend /
//! activate / policy update / topic update; #1755, #1038, #1146).
//!
//! The driver publishes uniquely-numbered JSON messages through the frontends,
//! receives pushed deliveries on its own HTTP server (it is the subscriber
//! endpoint of every subscription), and checks deliveries against its model.

use antithesis_instrumentation as _;
use antithesis_sdk::{assert_always, assert_reachable, assert_sometimes, assert_unreachable, lifecycle};
use hegel::generators as gs;
use hegel::TestCase;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

const GROUP: &str = "at";
const TOPICS: [&str; 2] = ["at.t0", "at.t1"];
/// (topic index, subscription name, batch?)
const SUBS: [(usize, &str, bool); 4] = [(0, "s0", false), (0, "b0", true), (1, "s1", false), (1, "b1", true)];
const OWNER: &str = "antithesis";

/// A delivery to a subscription suspended longer ago than this is a violation.
const SUSPEND_GRACE: Duration = Duration::from_secs(60);
/// How long an eventually waits for every acknowledged message to arrive.
const DELIVERY_DEADLINE: Duration = Duration::from_secs(300);

// Endpoint modes: how the subscriber endpoint answers a subscription's deliveries.
const MODE_OK: u8 = 0;
const MODE_FAIL: u8 = 1; // 500
const MODE_SLOW: u8 = 2; // answer after 1.5 s (longer than the serial requestTimeout)
const MODE_FLAKY: u8 = 3; // every other request gets 503
const MODE_NAMES: [&str; 4] = ["ok", "fail", "slow", "flaky"];

fn log(msg: impl AsRef<str>) {
    println!("[driver] {}", msg.as_ref());
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum PubStatus {
    Pending,
    Acked,
    Unknown,
    Probe,
}

struct MsgRec {
    topic: usize,
    status: PubStatus,
}

#[derive(Clone, Copy, Debug)]
enum SubState {
    Active,
    /// Suspension confirmed by management at this instant.
    Suspended(Instant),
    /// A state change whose outcome we don't know.
    Unknown,
}

struct Model {
    msgs: HashMap<u64, MsgRec>,
    /// Per subscription: message id -> number of 2xx-answered deliveries.
    delivered: Vec<HashMap<u64, u32>>,
    sub_state: Vec<SubState>,
}

struct Shared {
    model: Mutex<Model>,
    modes: Vec<AtomicU8>,
    flaky_counter: AtomicU64,
}

struct Env {
    management: String,
    frontends: Vec<String>,
    consumers: Vec<String>,
    receiver_url: String,
}

fn agent(timeout: Duration) -> ureq::Agent {
    ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_millis(800))
        .timeout(timeout)
        .build()
}

/// Status code of a request, or None for a transport error.
fn status_of(r: Result<ureq::Response, ureq::Error>) -> (Option<u16>, String) {
    match r {
        Ok(resp) => {
            let s = resp.status();
            (Some(s), resp.into_string().unwrap_or_default())
        }
        Err(ureq::Error::Status(s, resp)) => (Some(s), resp.into_string().unwrap_or_default()),
        Err(e) => (None, e.to_string()),
    }
}

fn sub_index(topic: &str, name: &str) -> Option<usize> {
    SUBS.iter().position(|(t, n, _)| TOPICS[*t] == topic && *n == name)
}

// ---------------------------------------------------------------- receiver

fn start_receiver(addr: &str, shared: Arc<Shared>) {
    let server = Arc::new(tiny_http::Server::http(addr).expect("bind receiver"));
    for _ in 0..24 {
        let server = server.clone();
        let shared = shared.clone();
        std::thread::spawn(move || loop {
            let req = match server.recv() {
                Ok(r) => r,
                Err(_) => continue,
            };
            handle_delivery(req, &shared);
        });
    }
    log(format!("receiver listening on {addr}"));
}

fn handle_delivery(mut req: tiny_http::Request, shared: &Shared) {
    let path = req.url().to_string();
    let parts: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let sub = if parts.len() == 3 && parts[0] == "sub" { sub_index(parts[1], parts[2]) } else { None };
    let Some(sub) = sub else {
        let _ = req.respond(tiny_http::Response::empty(200));
        return;
    };
    let mut body = String::new();
    let _ = req.as_reader().read_to_string(&mut body);
    let retry_count: u32 = req
        .headers()
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case("Hermes-Retry-Count"))
        .and_then(|h| h.value.as_str().parse().ok())
        .unwrap_or(0);

    let mode = shared.modes[sub].load(Ordering::SeqCst);
    let status = match mode {
        MODE_FAIL => 500,
        MODE_FLAKY => {
            if shared.flaky_counter.fetch_add(1, Ordering::SeqCst) % 2 == 0 { 503 } else { 200 }
        }
        MODE_SLOW => {
            std::thread::sleep(Duration::from_millis(1500));
            200
        }
        _ => 200,
    };

    if status == 200 {
        record_delivery(shared, sub, &body, retry_count);
    }
    let _ = req.respond(tiny_http::Response::empty(status));
}

fn record_delivery(shared: &Shared, sub: usize, body: &str, retry_count: u32) {
    let (topic_idx, sub_name, is_batch) = SUBS[sub];
    let parsed: Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(e) => {
            assert_unreachable!("deliveries carry well-formed JSON", &json!({"sub": sub_name, "error": e.to_string(), "body": body.chars().take(300).collect::<String>()}));
            return;
        }
    };
    let items: Vec<Value> = match parsed {
        Value::Array(a) => a,
        other => vec![other],
    };
    assert_sometimes!(is_batch && items.len() > 1, "a batch delivery carried several messages", &json!({"sub": sub_name, "n": items.len()}));
    assert_sometimes!(retry_count > 0, "a delivery was a retry (Hermes-Retry-Count > 0)", &json!({"sub": sub_name, "retry": retry_count}));

    let now = Instant::now();
    let mut m = lock(&shared.model);
    if let SubState::Suspended(since) = m.sub_state[sub] {
        let late = now.duration_since(since);
        assert_always!(late < SUSPEND_GRACE, "no delivery reaches a subscription suspended for more than 60s", &json!({"sub": sub_name, "suspended_for_ms": late.as_millis() as u64, "batch": is_batch, "messages": items.len()}));
    }
    for item in items {
        // Batch deliveries wrap each message as {"message": ..., "metadata": ...}.
        let item = match item.get("message") {
            Some(inner) if item.get("id").is_none() => inner.clone(),
            _ => item,
        };
        let id = item.get("id").and_then(Value::as_u64);
        let t = item.get("t").and_then(Value::as_str).unwrap_or("");
        let known = id.and_then(|id| m.msgs.get(&id)).map(|r| r.topic);
        assert_always!(known == Some(topic_idx) && t == TOPICS[topic_idx], "every delivered message was published to the subscription's topic", &json!({"sub": sub_name, "item": item, "known_topic": known}));
        if let Some(id) = id {
            let c = m.delivered[sub].entry(id).or_insert(0);
            *c += 1;
            let dup = *c > 1;
            assert_sometimes!(dup, "a message was delivered more than once", &json!({"sub": sub_name, "id": id}));
        }
    }
}

// ---------------------------------------------------------------- setup

fn wait_until(what: &str, mut f: impl FnMut() -> Result<(), String>) {
    let mut last = String::new();
    loop {
        match f() {
            Ok(()) => {
                log(format!("{what}: ok"));
                return;
            }
            Err(e) => {
                if e != last {
                    log(format!("waiting for {what}: {e}"));
                    last = e;
                }
                std::thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

fn get_ok(url: &str) -> Result<(), String> {
    match status_of(agent(Duration::from_secs(2)).get(url).call()) {
        (Some(200), _) => Ok(()),
        (s, b) => Err(format!("GET {url} -> {s:?} {}", b.chars().take(200).collect::<String>())),
    }
}

fn post_json(url: &str, body: &Value) -> (Option<u16>, String) {
    status_of(agent(Duration::from_secs(5)).post(url).send_json(body.clone()))
}

fn topic_body(topic: &str) -> Value {
    json!({
        "name": topic, "description": "antithesis topic", "contentType": "JSON",
        "ack": "ALL", "retentionTime": {"duration": 1},
        "owner": {"source": "Plaintext", "id": OWNER}
    })
}

fn serial_policy(inflight: u32, backoff: u32) -> Value {
    json!({"rate": 400, "messageTtl": 3600, "messageBackoff": backoff, "requestTimeout": 1000,
           "socketTimeout": 0, "inflightSize": inflight, "backoffMultiplier": 1.0,
           "backoffMaxIntervalInSec": 600, "retryClientErrors": false})
}

fn batch_policy(size: u32, time: u32, backoff: u32) -> Value {
    json!({"messageTtl": 3600, "messageBackoff": backoff, "requestTimeout": 2000,
           "batchSize": size, "batchTime": time, "batchVolume": 64000, "retryClientErrors": false})
}

fn sub_body(env: &Env, sub: usize) -> Value {
    let (t, name, batch) = SUBS[sub];
    let topic = TOPICS[t];
    let mut b = json!({
        "topicName": topic, "name": name, "description": "antithesis subscription",
        "endpoint": format!("{}/sub/{}/{}", env.receiver_url, topic, name),
        "owner": {"source": "Plaintext", "id": OWNER}, "contentType": "JSON",
    });
    if batch {
        b["deliveryType"] = json!("BATCH");
        b["subscriptionPolicy"] = batch_policy(10, 500, 200);
    } else {
        b["deliveryType"] = json!("SERIAL");
        b["subscriptionPolicy"] = serial_policy(10, 200);
    }
    b
}

fn setup(env: &Env, driver: &Driver) {
    wait_until("management", || get_ok(&format!("{}/groups", env.management)));
    for f in &env.frontends {
        wait_until(&format!("frontend {f}"), || get_ok(&format!("{f}/status/health")));
    }
    for c in &env.consumers {
        wait_until(&format!("consumers {c}"), || get_ok(&format!("{c}/status/health")));
    }
    wait_until("group", || {
        let _ = post_json(&format!("{}/groups", env.management), &json!({"groupName": GROUP}));
        get_ok(&format!("{}/groups/{GROUP}", env.management))
    });
    for topic in TOPICS {
        wait_until(&format!("topic {topic}"), || {
            let (s, b) = post_json(&format!("{}/topics", env.management), &topic_body(topic));
            if !matches!(s, Some(201)) {
                log(format!("create topic {topic}: {s:?} {}", b.chars().take(300).collect::<String>()));
            }
            get_ok(&format!("{}/topics/{topic}", env.management))
        });
    }
    for sub in 0..SUBS.len() {
        let (t, name, _) = SUBS[sub];
        let topic = TOPICS[t];
        wait_until(&format!("subscription {topic}${name}"), || {
            let (s, b) = post_json(&format!("{}/topics/{topic}/subscriptions", env.management), &sub_body(env, sub));
            if !matches!(s, Some(201)) {
                log(format!("create subscription {name}: {s:?} {}", b.chars().take(300).collect::<String>()));
            }
            get_ok(&format!("{}/topics/{topic}/subscriptions/{name}", env.management))
        });
    }
    // The pipeline works end to end: a probe on each topic reaches each subscription.
    wait_until("probe deliveries to every subscription", || {
        for t in 0..TOPICS.len() {
            let fe = &env.frontends[t % env.frontends.len()];
            driver.publish_raw(fe, t, 0, true);
        }
        let m = lock(&driver.shared.model);
        let missing: Vec<&str> = (0..SUBS.len()).filter(|s| m.delivered[*s].is_empty()).map(|s| SUBS[s].1).collect();
        if missing.is_empty() { Ok(()) } else { Err(format!("no delivery yet to {missing:?}")) }
    });
}

// ---------------------------------------------------------------- driver

struct Driver {
    env: Env,
    shared: Arc<Shared>,
    next_id: AtomicU64,
    last_disruptive: Mutex<Option<Instant>>,
    last_publish: Mutex<Option<Instant>>,
    eventually_lock: Mutex<()>,
    eventually_enabled: bool,
    eventually_after: Option<Duration>,
    started: Instant,
}

impl Driver {
    /// Publish one message; returns its final status.
    fn publish_raw(&self, frontend: &str, topic: usize, pad: usize, probe: bool) -> PubStatus {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        lock(&self.shared.model).msgs.insert(id, MsgRec { topic, status: if probe { PubStatus::Probe } else { PubStatus::Pending } });
        let body = json!({"id": id, "t": TOPICS[topic], "pad": "x".repeat(pad)});
        let mut req = agent(Duration::from_secs(3))
            .post(&format!("{frontend}/topics/{}", TOPICS[topic]))
            .set("Content-Type", "application/json");
        if id % 3 == 0 {
            req = req.set("Partition-Key", &format!("k{}", id % 7));
        }
        let (s, _) = status_of(req.send_string(&body.to_string()));
        let status = if probe { PubStatus::Probe } else if s == Some(201) { PubStatus::Acked } else { PubStatus::Unknown };
        if let Some(r) = lock(&self.shared.model).msgs.get_mut(&id) {
            r.status = status;
        }
        status
    }

    /// Disruptive actions share a cooldown so they don't fire every round.
    fn disruptive_ready(&self, cooldown: Duration) -> bool {
        let mut last = lock(&self.last_disruptive);
        let now = Instant::now();
        if last.map_or(true, |l| now.duration_since(l) >= cooldown) {
            *last = Some(now);
            true
        } else {
            false
        }
    }

    fn set_state(&self, sub: usize, suspend: bool) -> bool {
        let (t, name, _) = SUBS[sub];
        let url = format!("{}/topics/{}/subscriptions/{}/state", self.env.management, TOPICS[t], name);
        let body = if suspend { "\"SUSPENDED\"" } else { "\"ACTIVE\"" };
        lock(&self.shared.model).sub_state[sub] = SubState::Unknown;
        let (s, b) = status_of(agent(Duration::from_secs(3)).put(&url).set("Content-Type", "application/json").send_string(body));
        let ok = matches!(s, Some(200..=299));
        let pending_acked = {
            let mut m = lock(&self.shared.model);
            if ok {
                m.sub_state[sub] = if suspend { SubState::Suspended(Instant::now()) } else { SubState::Active };
            }
            if ok && !suspend { undelivered(&m, sub).len() } else { 0 }
        };
        if ok {
            log(format!("set state {name} suspend={suspend}: confirmed"));
        } else {
            log(format!("set state {name} suspend={suspend}: {s:?} {}", b.chars().take(200).collect::<String>()));
        }
        assert_sometimes!(ok && !suspend && pending_acked > 0, "a subscription was reactivated with acknowledged messages still undelivered", &json!({"sub": name, "pending": pending_acked}));
        ok
    }

    fn eventually(&self) {
        let _g = lock(&self.eventually_lock);
        log("eventually: stopping faults");
        if let Ok(cmd) = std::env::var("ANTITHESIS_STOP_FAULTS") {
            let _ = std::process::Command::new(cmd).arg("3600").status();
        }
        for s in 0..SUBS.len() {
            self.shared.modes[s].store(MODE_OK, Ordering::SeqCst);
        }
        let deadline = Instant::now() + DELIVERY_DEADLINE;
        let mut not_recovered: Vec<String> = vec![];
        for url in self.env.frontends.iter().chain(self.env.consumers.iter()) {
            let u = format!("{url}/status/health");
            loop {
                match get_ok(&u) {
                    Ok(()) => break,
                    Err(e) if Instant::now() > deadline => { not_recovered.push(e); break; }
                    Err(_) => std::thread::sleep(Duration::from_secs(1)),
                }
            }
            log(format!("eventually: {u} checked"));
        }
        for s in 0..SUBS.len() {
            while !self.set_state(s, false) {
                if Instant::now() > deadline { not_recovered.push(format!("reactivate {}", SUBS[s].1)); break; }
                std::thread::sleep(Duration::from_secs(1));
            }
        }
        let all_up = not_recovered.is_empty();
        log(format!("eventually: recovered={all_up} {not_recovered:?}"));
        assert_always!(all_up, "Hermes recovers after faults stop: nodes healthy and subscriptions reactivated", &json!({"not_recovered": not_recovered}));

        // Judge the messages acknowledged up to now; rules may keep publishing meanwhile.
        let mut missing: Vec<(usize, Vec<u64>)> = {
            let m = lock(&self.shared.model);
            (0..SUBS.len()).map(|s| (s, undelivered(&m, s))).collect()
        };
        loop {
            {
                let m = lock(&self.shared.model);
                for (s, v) in missing.iter_mut() {
                    v.retain(|id| !m.delivered[*s].contains_key(id));
                }
                missing.retain(|(_, v)| !v.is_empty());
            }
            if missing.is_empty() || Instant::now() > deadline { break; }
            let counts: Vec<(&str, usize)> = missing.iter().map(|(s, v)| (SUBS[*s].1, v.len())).collect();
            log(format!("eventually: waiting for deliveries {counts:?}"));
            std::thread::sleep(Duration::from_secs(5));
        }
        log("eventually: assertions start");
        // Holes below the highest delivered id point at loss rather than tail lag.
        let summary: Vec<Value> = {
            let m = lock(&self.shared.model);
            missing.iter().map(|(s, v)| {
                let max_delivered = m.delivered[*s].keys().max().copied().unwrap_or(0);
                let holes = v.iter().filter(|id| **id < max_delivered).count();
                json!({"sub": SUBS[*s].1, "missing": v.len(), "holes_below_max_delivered": holes,
                       "max_delivered": max_delivered, "first": v.iter().take(20).collect::<Vec<_>>()})
            }).collect()
        };
        let acked = lock(&self.shared.model).msgs.values().filter(|r| r.status == PubStatus::Acked).count();
        log(format!("eventually: acked={acked} missing={summary:?}"));
        assert_always!(missing.is_empty(), "every message acknowledged with 201 is eventually delivered to every subscription of its topic", &json!({"missing": summary, "acked": acked}));
        log("eventually: ending the timeline");
        std::process::exit(0);
    }
}

fn undelivered(m: &Model, sub: usize) -> Vec<u64> {
    let topic = SUBS[sub].0;
    let mut v: Vec<u64> = m.msgs.iter()
        .filter(|(id, r)| r.topic == topic && r.status == PubStatus::Acked && !m.delivered[sub].contains_key(id))
        .map(|(id, _)| *id)
        .collect();
    v.sort();
    v
}

#[hegel::concurrent_state_machine]
impl Driver {
    #[rule(weight = 20)]
    fn publish(&self, tc: TestCase) {
        // Keep the publish rate (~25/s) well under what a serial subscription drains,
        // so backlog can't masquerade as loss in the eventually.
        {
            let mut last = lock(&self.last_publish);
            let now = Instant::now();
            if let Some(l) = *last {
                if now.duration_since(l) < Duration::from_millis(40) { return; }
            }
            *last = Some(now);
        }
        let fe = tc.draw(gs::integers::<usize>().min_value(0).max_value(self.env.frontends.len() - 1));
        let topic = tc.draw(gs::integers::<usize>().min_value(0).max_value(TOPICS.len() - 1));
        let pad = tc.draw(gs::integers::<usize>().min_value(0).max_value(600));
        let st = self.publish_raw(&self.env.frontends[fe], topic, pad, false);
        assert_sometimes!(st == PubStatus::Acked, "a publish was acknowledged with 201", &json!({}));
        assert_sometimes!(st == PubStatus::Unknown, "a publish failed or timed out", &json!({}));
    }

    #[rule(weight = 3)]
    fn set_endpoint_mode(&self, tc: TestCase) {
        let sub = tc.draw(gs::integers::<usize>().min_value(0).max_value(SUBS.len() - 1));
        // Healthy most of the time; failure modes drive retries and out-of-order acks.
        let mode = if tc.draw(gs::weighted_booleans(0.5)) { MODE_OK } else { tc.draw(gs::integers::<u8>().min_value(1).max_value(3)) };
        self.shared.modes[sub].store(mode, Ordering::SeqCst);
        tc.note(&format!("endpoint {} -> {}", SUBS[sub].1, MODE_NAMES[mode as usize]));
    }

    #[rule(weight = 2)]
    fn suspend_or_activate(&self, tc: TestCase) {
        if !self.disruptive_ready(Duration::from_secs(4)) { return; }
        let sub = tc.draw(gs::integers::<usize>().min_value(0).max_value(SUBS.len() - 1));
        let suspend = tc.draw(gs::weighted_booleans(0.5));
        if self.set_state(sub, suspend) {
            assert_reachable!("a subscription state change was accepted", &json!({}));
        }
    }

    #[rule(weight = 1)]
    fn update_subscription_policy(&self, tc: TestCase) {
        if !self.disruptive_ready(Duration::from_secs(4)) { return; }
        let sub = tc.draw(gs::integers::<usize>().min_value(0).max_value(SUBS.len() - 1));
        let (t, name, batch) = SUBS[sub];
        let policy = if batch {
            let size = tc.draw(gs::sampled_from(vec![1u32, 5, 20]));
            let time = tc.draw(gs::sampled_from(vec![100u32, 500, 2000]));
            let backoff = tc.draw(gs::sampled_from(vec![100u32, 500]));
            batch_policy(size, time, backoff)
        } else {
            let inflight = tc.draw(gs::sampled_from(vec![1u32, 5, 20]));
            let backoff = tc.draw(gs::sampled_from(vec![100u32, 500]));
            serial_policy(inflight, backoff)
        };
        let url = format!("{}/topics/{}/subscriptions/{}", self.env.management, TOPICS[t], name);
        let (s, b) = status_of(agent(Duration::from_secs(3)).put(&url).send_json(json!({"subscriptionPolicy": policy})));
        if matches!(s, Some(200..=299)) {
            assert_reachable!("a subscription policy update was accepted", &json!({}));
        } else {
            log(format!("update policy {name}: {s:?} {}", b.chars().take(200).collect::<String>()));
        }
    }

    #[rule(weight = 1)]
    fn update_topic_max_message_size(&self, tc: TestCase) {
        if !self.disruptive_ready(Duration::from_secs(4)) { return; }
        let t = tc.draw(gs::integers::<usize>().min_value(0).max_value(TOPICS.len() - 1));
        let size = tc.draw(gs::sampled_from(vec![10_240u32, 20_480, 51_200]));
        let url = format!("{}/topics/{}", self.env.management, TOPICS[t]);
        let (s, b) = status_of(agent(Duration::from_secs(3)).put(&url).send_json(json!({"maxMessageSize": size})));
        if matches!(s, Some(200..=299)) {
            assert_reachable!("a topic maxMessageSize update was accepted", &json!({}));
        } else {
            log(format!("update topic {}: {s:?} {}", TOPICS[t], b.chars().take(200).collect::<String>()));
        }
    }

    #[rule]
    fn maybe_eventually(&self, tc: TestCase) {
        if let Some(after) = self.eventually_after {
            if self.started.elapsed() >= after { self.eventually(); }
            return;
        }
        if !self.eventually_enabled { return; }
        if tc.draw(gs::weighted_booleans(0.003)) {
            self.eventually();
        }
    }
}

#[hegel::main(phases = [hegel::Phase::Generate])]
fn main(tc: TestCase) {
    antithesis_sdk::antithesis_init();
    let split = |v: String| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect::<Vec<_>>();
    let env = Env {
        management: std::env::var("MANAGEMENT_URL").unwrap_or("http://management:8090".into()),
        frontends: split(std::env::var("FRONTEND_URLS").unwrap_or("http://frontend1:8080".into())),
        consumers: split(std::env::var("CONSUMER_URLS").unwrap_or("http://consumers1:8070".into())),
        receiver_url: std::env::var("RECEIVER_URL").unwrap_or("http://driver:9000".into()),
    };
    let shared = Arc::new(Shared {
        model: Mutex::new(Model {
            msgs: HashMap::new(),
            delivered: vec![HashMap::new(); SUBS.len()],
            sub_state: vec![SubState::Active; SUBS.len()],
        }),
        modes: (0..SUBS.len()).map(|_| AtomicU8::new(MODE_OK)).collect(),
        flaky_counter: AtomicU64::new(0),
    });
    start_receiver(&std::env::var("RECEIVER_ADDR").unwrap_or("0.0.0.0:9000".into()), shared.clone());

    let mut driver = Driver {
        env,
        shared,
        next_id: AtomicU64::new(1),
        last_disruptive: Mutex::new(None),
        last_publish: Mutex::new(None),
        eventually_lock: Mutex::new(()),
        eventually_enabled: std::env::var("DISABLE_EVENTUALLY").is_err(),
        eventually_after: std::env::var("EVENTUALLY_AFTER_SECS").ok().and_then(|s| s.parse().ok()).map(Duration::from_secs),
        started: Instant::now(),
    };
    setup(&driver.env, &driver);

    lifecycle::setup_complete(&json!({"topics": TOPICS, "subscriptions": SUBS.len()}));
    log("setup complete; waiting for /workload-active");
    while !std::path::Path::new("/workload-active").exists() {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_reachable!("the workload starts", &json!({}));
    driver.started = Instant::now();
    log("workload active");

    hegel::stateful::machine(driver)
        .steps(i64::MAX)
        .min_concurrency(1)
        .max_concurrency(4)
        .run_concurrent(tc);
}
