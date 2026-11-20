//! ADR-0051 step 2a at the REALM layer: a job's temporary realm touches nothing
//! of its parent, cannot be handed the parent's store, and cannot nest.
//!
//! The key test is the fjall sweep, and it is a dump rather than a promise: a
//! real keyspace is swept before and after a job that registers a declaration,
//! writes through the plan the declaration resolved, and publishes an event —
//! and the two key/value sets must be IDENTICAL. Everything the job did went into
//! a memory engine that was dropped, and the sweep is what proves it.
//!
//! Registration here goes through `DeclaredRegistrar` (the declaration door:
//! `Realm::register_definition`, the same body `Engine::register_into` ends at)
//! because this crate cannot name the engine — `engine/tests/dispatch_job.rs`
//! asserts the engine's door.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_in_result,
    clippy::panic_in_result_fn
)]

use aura_booth::{Body, StoreOp, StoreOpKind};
use aura_realm::dispatch::{BoothDecl, Boundary, DeclaredRegistrar, Dispatch};
use aura_realm::mq::MqStore;
use aura_realm::realm_set::{RealmSet, MAIN_REALM};
use aura_realm::Realm;
use okm_core::storage::VirtualStorage;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

/// The storage declaration a type upload carries — one collection, key `id`, one
/// hot field `count` (the shape `StorePlan::from_schema` parses).
fn storage_schema() -> Value {
    json!({
        "storage": { "collections": { "counters": { "schema": {
            "key_len": 8,
            "key_fields": [
                { "name": "id", "ty": "U64", "width": 8, "offset": 0, "tag": 0 }
            ],
            "layout_version": 1,
            "hot_width": 8,
            "payload_header_len": 3,
            "hot_fields": [
                { "name": "count", "ty": "U64", "width": 8, "offset": 0, "tag": 0 }
            ],
            "cold_fields": [],
            "slots": { "primary": 0, "dynamic": 1, "field_vocab_id": 2,
                       "field_vocab_name": 3, "declared_index_base": 16385,
                       "declared_reduce_base": 20481, "junction_base": 12288 }
        } } } }
    })
}

/// A booth that does everything a job's booth is supposed to be able to do —
/// write through its declared storage, publish into its realm's event plane —
/// so that "the parent did not change" is a statement about a realm that DID
/// something, not about one that did nothing.
fn writer(log: Arc<Mutex<Vec<String>>>) -> Body {
    Body::Rust(Arc::new(move |ctx, _args: Value| {
        let log = log.clone();
        Box::pin(async move {
            ctx.store()
                .emit(StoreOp {
                    collection: "counters".into(),
                    op: StoreOpKind::PutDocument {
                        key: json!({ "id": 1 }),
                        doc: json!({ "count": 42 }),
                    },
                })
                .map_err(|e| anyhow::anyhow!("store write inside the job's realm: {e}"))?;
            let read = ctx
                .store()
                .emit(StoreOp {
                    collection: "counters".into(),
                    op: StoreOpKind::GetDocument {
                        key: json!({ "id": 1 }),
                    },
                })
                .map_err(|e| anyhow::anyhow!("store read inside the job's realm: {e}"))?;
            // The event plane of the realm it is running in. If this reached the
            // parent, the parent's keyspace would gain rows — which is what the
            // sweep below measures.
            ctx.emit("job:test", json!({ "from": "writer" })).await?;
            log.lock()
                .map_err(|_| anyhow::anyhow!("the log lock was poisoned"))?
                .push("wrote+emitted".to_string());
            Ok(read)
        })
    }))
}

/// Every key/value pair the engine holds, ordered — a dump of the whole
/// keyspace, root band and realm slices alike.
fn dump(store: &MqStore) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows: Vec<(Vec<u8>, Vec<u8>)> = store
        .root()
        .scan_range(&[], None)
        .map(|item| item.unwrap_or_else(|e| panic!("scan: {e}")))
        .collect();
    rows.sort();
    rows
}

fn job(executor: &str, needs: Value) -> effector_protocol::Job {
    serde_json::from_value(json!({
        "id": "job-keyspace",
        "booth": "gravity/turn-1",
        "source": { "kind": "ops", "ops": [{
            "call": {
                "call_id": "call-1",
                "executor": executor,
                "entry": "execute",
                "language": "rust",
                "args": { "n": 1 },
                "code": { "url": "http://plane.invalid/code/00", "sha256": "00" }
            },
            "needs": needs
        }] }
    }))
    .expect("the job is a valid envelope")
}

fn scratch_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "aura-dispatch-{tag}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

/// THE PROOF: a full job — mint, register a declaration with storage, write
/// through its plan, publish an event, destroy — leaves a real fjall keyspace
/// byte-identical, and leaves the parent realm's own registry and queues
/// untouched.
#[test]
fn a_job_leaves_the_parent_keyspace_byte_identical() {
    let dir = scratch_dir("job");
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let root = MqStore::fjall(okm_core::FjallStore::open(&dir, "aura_mq").expect("fjall"));
        let set = RealmSet::with_mq(root.clone());
        let main = set.open(MAIN_REALM).await.expect("main");
        // A persistent tenant with real rows: "before" is not the empty keyspace,
        // so an equality of two dumps means something.
        main.store()
            .put(b"probe".to_vec(), b"value".to_vec())
            .expect("probe row");
        let before = dump(&root);
        assert!(!before.is_empty(), "the store must hold the persisted realm");
        let live_before = set.live().await.expect("live");

        let log = Arc::new(Mutex::new(Vec::new()));
        let decls = vec![BoothDecl::new("writer", writer(log.clone())).with_storage(storage_schema())];
        let out = Dispatch::new(&set, &DeclaredRegistrar)
            .run(&job("writer/run-1", json!({ "store": true })), &decls)
            .await
            .expect("the job runs");

        // IT REALLY RAN, and its work really landed — inside the realm. The
        // document it wrote came back out of the plan the DECLARATION resolved
        // (ns = BOOTH_NS_BASE + 1, from the realm's own dictionary row).
        assert_eq!(*log.lock().unwrap(), vec!["wrote+emitted".to_string()]);
        match &out.ops[0].result {
            aura_realm::dispatch::OpResult::Done { value } => {
                assert_eq!(value["count"], 42, "the job's storage answered: {value}");
            }
            other => panic!("the op must have answered, got {other:?}"),
        }
        assert!(out.destroyed);

        // THE REALM IS GONE: not listed, no name, nothing to re-open.
        assert!(
            !set.live().await.expect("live").contains(&out.realm.to_string()),
            "the job's realm is still listed after the job ended"
        );
        assert_eq!(set.name_of(out.realm).await.expect("name_of"), None);

        // …and the PARENT realm never learned the type, never queued the event.
        {
            let parent_realm = main.realm();
            let parent = parent_realm.lock().await;
            assert!(
                parent.booth_type("writer").is_none(),
                "the job's type leaked into the parent's registry"
            );
            assert!(parent.plan_of("writer").is_none());
        }
        assert_eq!(
            aura_realm::mq::depth(
                &main.store(),
                "job:test",
                &aura_realm::mq::InstanceKey::Singleton
            )
            .expect("depth"),
            0,
            "the job published into the parent's event plane"
        );

        // THE SWEEP, restated for a world where frames exist. ADR-0051 §4 lets
        // frames cross, so the parent's QUEUE PLANE may gain exactly the frame it
        // was addressed — and nothing else, anywhere.
        //
        // The plane is excluded by NAMESPACE, not by trusting the frame to look
        // distinctive: the mq tables are a fixed set of `ok_ns` numbers, so a job
        // that managed to write state anywhere in the parent is caught here.
        let after = dump(&root);
        let mq_ns = [20u16, 21, 22, 23, 25]; // mq.rs's `ok_ns` — the queue plane
        // Key layout: the realm's 4-byte id, then okm's own entry header
        // (`0xFF, version`) when the row went through a collection, then the
        // namespace. A raw put (the probe row) has no header.
        let ns_of = |key: &[u8]| -> Option<u16> {
            let at = if key.get(4) == Some(&0xFF) { 6 } else { 4 };
            key.get(at..at + 2)
                .map(|b| u16::from_be_bytes([b[0], b[1]]))
        };
        let state = |rows: Vec<(Vec<u8>, Vec<u8>)>| -> Vec<(Vec<u8>, Vec<u8>)> {
            rows.into_iter()
                .filter(|(k, _)| !matches!(ns_of(k), Some(ns) if mq_ns.contains(&ns)))
                .collect()
        };
        let (state_before, state_after) = (state(before.clone()), state(after.clone()));
        assert_eq!(
            state_before, state_after,
            "a job changed the parent's STATE, not just its queue plane"
        );
        // Inside the plane, exactly the frame: the name the job emitted never
        // became an event of the parent's own (asserted above as depth 0), and the
        // one row added is the frame the dispatcher is addressed with.
        let frames = aura_realm::mq::backlog(
            &main.store(),
            aura_realm::frame_path::JOB_FRAME_EVENT,
            &aura_realm::mq::InstanceKey::Named("turn-1".to_string()),
            0,
        )
        .expect("the parent's frame queue");
        assert_eq!(frames.len(), 1, "one emit, one frame: {frames:?}");
        assert_eq!(
            frames[0].1["path"],
            json!([{ "job": "job-keyspace", "op": 0 }]),
            "the frame carries the path of the job and op that spoke"
        );
        assert_eq!(frames[0].1["event"], "job:test");
        // The set's memory half is back to exactly what it was.
        assert_eq!(set.live().await.expect("live"), live_before);
    });
    let _ = std::fs::remove_dir_all(&dir);
}

/// BOUNDARY (a), refused by name. A booth inside a job's realm holds that
/// realm's store; the two doors that could hand it the PARENT's engine say why
/// they will not.
#[test]
fn a_booths_realm_cannot_be_bound_to_the_parent_store() {
    let dir = scratch_dir("boundary-store");
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    rt.block_on(async {
        let parent = MqStore::fjall(okm_core::FjallStore::open(&dir, "aura_mq").expect("fjall"));
        let set = RealmSet::with_mq(parent.clone());
        let _main = set.open(MAIN_REALM).await.expect("main");
        let eph = set.create_ephemeral().await.expect("a temporary realm");

        // Door 1: bind a slice of the engine's keyspace to the job's id.
        let err = MqStore::for_realm(&parent, eph.id)
            .err()
            .expect("an ephemeral realm has no slice of the parent's keyspace");
        assert!(
            err.to_string().contains("in-memory engine of its own"),
            "the refusal must say why, got: {err}"
        );

        // Door 2: build the realm over the parent's engine (its `main` slice —
        // the handle a booth would hold if it could reach out of its realm).
        let parent_slice =
            MqStore::for_realm(&parent, aura_realm::RealmId::Persisted(1)).expect("main's slice");
        let err = Realm::with_mq(eph.id, parent_slice)
            .err()
            .expect("a temporary realm is memory and only memory");
        assert!(
            err.to_string().contains("memory and only memory"),
            "the refusal must say why, got: {err}"
        );

        // And the realm it actually got is its own engine: memory, no prefix.
        assert!(eph.store().is_memory() && eph.store().prefix().is_empty());
        set.destroy(eph.id).await.expect("destroy");
    });
    let _ = std::fs::remove_dir_all(&dir);
}

/// BOUNDARY (c): realms do not nest, and the door that would express it names
/// the refusal instead of minting.
#[tokio::test]
async fn a_realm_cannot_open_a_child_realm() {
    let set = RealmSet::with_mq(MqStore::mem());
    let eph = set.create_ephemeral().await.expect("a temporary realm");
    let live = set.live().await.expect("live");

    let refusal = eph
        .realm()
        .lock()
        .await
        .create_child_realm()
        .expect_err("a realm holds no RealmSet: there is no door to a child");

    assert_eq!(refusal.boundary, Boundary::ChildRealm);
    assert_eq!(refusal.realm, eph.id);
    assert!(
        refusal.to_string().contains("ADR-0051 boundary (c)"),
        "the refusal names its record and clause: {refusal}"
    );
    assert!(
        refusal.to_string().contains("do not nest"),
        "the refusal says the rule, not just 'no': {refusal}"
    );
    // Nothing was created by the attempt.
    assert_eq!(set.live().await.expect("live"), live);
    set.destroy(eph.id).await.expect("destroy");
}
// ---------------------------------------------------------------------------
// The attachment the declarations ride (ADR-0051 §Attachments)
// ---------------------------------------------------------------------------

fn wire_decl(name: &str) -> aura_booth::persist::PersistedBooth {
    aura_booth::persist::PersistedBooth {
        name: name.to_string(),
        language: "python".to_string(),
        source: "pass".to_string(),
        encoding: Default::default(),
        idle_ttl_secs: None,
        schema: Some(json!({ "storage": { "collections": {} } })),
        concurrency: Default::default(),
    }
}

fn job_with(attachments: Vec<effector_protocol::Attachment>) -> effector_protocol::Job {
    effector_protocol::Job {
        id: "job-attach".to_string(),
        booth: "gravity/turn-1".to_string(),
        scope: Default::default(),
        source: effector_protocol::JobSource::Ops { ops: Vec::new() },
        resources: Vec::new(),
        placement: Default::default(),
        attachments,
    }
}

fn cbor(bytes: &[aura_booth::persist::PersistedBooth]) -> Vec<u8> {
    let mut out = Vec::new();
    ciborium::ser::into_writer(&bytes.to_vec(), &mut out).expect("cbor");
    out
}

#[test]
fn both_media_decode_to_the_same_declarations() {
    let decls = vec![wire_decl("a"), wire_decl("b")];
    let from_cbor = aura_realm::dispatch::decls_of(&job_with(vec![effector_protocol::Attachment {
        name: "boothset".to_string(),
        media: "application/cbor".to_string(),
        bytes: cbor(&decls),
    }]))
    .expect("cbor decodes");

    let json_bytes = serde_json::to_vec(&decls).expect("json");
    let from_json = aura_realm::dispatch::decls_of(&job_with(vec![effector_protocol::Attachment {
        name: "boothset".to_string(),
        media: "application/json".to_string(),
        bytes: json_bytes,
    }]))
    .expect("json decodes");

    let names = |d: &[BoothDecl]| d.iter().map(|d| d.name.clone()).collect::<Vec<_>>();
    assert_eq!(names(&from_cbor), vec!["a".to_string(), "b".to_string()]);
    assert_eq!(names(&from_json), names(&from_cbor), "one payload, two codecs");
    assert!(
        from_cbor[0].storage.is_some(),
        "the declaration's own schema came through — that is what the store plan resolves from"
    );
    assert!(
        from_cbor[0].receives.is_empty(),
        "routes are assembled from the schema's `receives` at registration, not restated here"
    );
}

#[test]
fn every_way_the_declarations_can_fail_to_arrive_is_refused_by_name() {
    let decls = vec![wire_decl("a")];
    let refusal = |job: effector_protocol::Job| match aura_realm::dispatch::decls_of(&job) {
        Err(aura_realm::dispatch::DispatchError::Attachment { why }) => why,
        Err(other) => panic!("expected an attachment refusal, got {other}"),
        Ok(decls) => panic!("expected a refusal, got {} declarations", decls.len()),
    };

    // 1. Nothing arrived.
    let why = refusal(job_with(Vec::new()));
    assert!(why.contains("no `boothset` attachment"), "{why}");

    // 2. A name this door does not read: skipping it would let a job run without what it brought.
    let why = refusal(job_with(vec![effector_protocol::Attachment {
        name: "something-else".to_string(),
        media: "application/cbor".to_string(),
        bytes: cbor(&decls),
    }]));
    assert!(why.contains("something-else"), "{why}");

    // 3. A media it cannot decode.
    let why = refusal(job_with(vec![effector_protocol::Attachment {
        name: "boothset".to_string(),
        media: "application/yaml".to_string(),
        bytes: cbor(&decls),
    }]));
    assert!(why.contains("application/yaml"), "{why}");

    // 4. Two sets: one job has one declaration set, and merging them would make order decide
    //    which definition wins.
    let why = refusal(job_with(vec![
        effector_protocol::Attachment {
            name: "boothset".to_string(),
            media: "application/cbor".to_string(),
            bytes: cbor(&decls),
        },
        effector_protocol::Attachment {
            name: "boothset".to_string(),
            media: "application/cbor".to_string(),
            bytes: cbor(&decls),
        },
    ]));
    assert!(why.contains("twice"), "{why}");

    // 5. Over the cap: the envelope is not a transport for bulk data.
    let why = refusal(job_with(vec![effector_protocol::Attachment {
        name: "boothset".to_string(),
        media: "application/cbor".to_string(),
        bytes: vec![0; aura_realm::dispatch::ATTACHMENT_CAP + 1],
    }]));
    assert!(why.contains("exceed"), "{why}");

    // 6. Bytes that are not the payload they claim to be.
    let why = refusal(job_with(vec![effector_protocol::Attachment {
        name: "boothset".to_string(),
        media: "application/cbor".to_string(),
        bytes: vec![0xff, 0x00],
    }]));
    assert!(why.contains("CBOR"), "{why}");
}
