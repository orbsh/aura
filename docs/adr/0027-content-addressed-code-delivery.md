# 0027 — Content-addressed code delivery: the Inline form retires, one payload shape

> **Languages:** [English](0027-content-addressed-code-delivery.md) (primary) · [中文](0027-content-addressed-code-delivery.zh-CN.md)

**Status:** Accepted (2026-09-25) — design; implementation pending, see Consequences

## Context

A remote call delivers its code as `CodePayload::Inline { bytes }` — the whole function
source rides the control frame, every call, through the gateway (and, once Prism mounts
`/probe/<alias>`, through Prism's extra hop). `CodePayload::Link { url, version,
expected_sha256 }` exists in the protocol and the probe already fetches + verifies it
(mismatch = error, never a silent accept), but aura constructs no Link anywhere: Link is
a dead arm today, and Inline is the only live shape.

Three facts make Inline the wrong default rather than a merely untidy one:

- **The control plane carries the data plane.** Inline is the one exception to the
  ruling that the control plane ships instructions and small results while bulk artifacts
  stay on the execution side. AI-generated function bodies are exactly the payload that
  grows.
- **Every call re-delivers what never changes.** A resident session loads its source once;
  re-delivery per call is only needed on cold start (and after eviction) — which is exactly
  what a content-addressed cache keys on.
- **The definition IS the bytes.** `ActorDef` persists the full `source` string inline;
  "version" of code is currently implicit in that string. The hash is the natural version
  token and is not yet stored at all.

## Decision

### 1. One payload shape: `CodeRef { url, sha256 }`

The `CodePayload` enum is deleted; `ToolCall.code` becomes `CodeRef { url, sha256 }`.
One option means no option to choose — the Inline form is retired, not deprecated.

- `version` is dropped: a content-hash URL is its own invalidation policy; a human label
  on the same content has no consumer (Windmill criterion).
- `sha256` is asserted by the frame, not parsed from the URL: a CDN may rewrite the path,
  and deriving verification from a transport detail builds integrity on the wrong side.

### 2. The blob is content-addressed, immutable, and owned by the meta plane

`CodeBlob` lives in `realm/src/meta.rs` beside the definition it serves (ns 42, after
TypeName 40 / ActorDef 41). Key = 32-byte sha256; value = the bytes. No name, no version,
no foreign key — the row is pure content. `mq.rs` is the event-queue domain and has no
claim on it; sharing the `MqStore` engine handle is a plumbing fact, not an ownership one.

`ActorDef.source: String` becomes `code_sha256: [u8; 32]` (fixed-width, key-discipline
friendly). The upload lifecycle (`register_inner`) hashes the source, writes the blob,
and persists the hash in the definition — the version fact moves from "the string is right
here" to "the string at this hash is right here".

Re-registration with unchanged code dedups for free (same hash → same row); changed code
produces a new hash the new definition version points at. There is no second version
counter, and none is wanted: the content hash IS the version identity.

### 3. Delivery sends the reference; the probe fetches on demand and caches by hash

The remote dispatch arm builds `CodeRef { url, sha256 }` from the definition's hash and
the deployment-declared prefix and sends the frame — nothing else. The probe resolves the
code exactly as it resolves Inline bytes today: cache hit on the sha256 → source; miss →
GET the URL, verify against the asserted hash (mismatch = error, never silent), insert,
proceed; the resolved source feeds the existing `with_session` cold-start load. The
per-hash cache is a discardable hot layer: same legitimacy tier as the resident session
itself — an execution node still holds nothing it would need to recover. (No loader seam
is invented: the probe's resolve-then-load path already exists; only the resolver's input
shape changes.)

The URL rides a deployment-declared prefix: `code_base_url` in the `node {}` config block
(KDL). No default. Only deployments that mount remote types ever need the value; a missing
prefix surfaces as an error value at delivery — an error face, never a startup dependency
for in-process-only nodes. The value points at Prism's static export or a real CDN
prefix; aura does not guess and does not proxy.

### 4. The serving endpoint is Prism's surface; no auth, no code ACL

`GET /code/{sha256}` — plain static download (ADR-0017 §8's asset rule: static bytes
carrying no event semantics), served by Prism (a resident component, same-process read of
aura's meta plane), `Cache-Control: immutable`, content served ONLY under its own
`/{hash}` path so the URL is self-verifying by construction.

The hash is the capability: unguessable, and the URL only ever appears inside the
control-plane-signed call. A reader of the CDN sees what an Inline reader on the same
wire already had — the code. Confidentiality is a deployment choice (private static source
inside the trust boundary, or signed URLs with CDN cache-key normalization so signatures
do not defeat caching) — and is deliberately NOT a new per-node code ACL: per-node
authorization, if it ever becomes real, extends ADR-0015's node identity (a registered
node may pull), it does not mint a second authorization home.

## Honest semantic cost

- **Remote delivery gains a dependency: a reachable source.** Until Prism's `/code`
  export exists, a remote deployment must place a static source over the blob storage.
  Inline had no such dependency. Recorded honestly: the only live consumer of the remote
  path today is tests (which stand up a test-local HTTP source), so no production shape
  regresses — but the first real deployment needs the endpoint, not the enum arm.
- **A cold remote start is two round trips (fetch + call) instead of one.** Bytes left
  the control frame and entered the data path; the cold start pays for it — cache hits
  (same code, later calls) absorb the cost entirely.
- **The definition no longer self-contains its bytes.** A bare ActorDef row is not enough
  to resurrect an actor — the blob must still exist under its hash. Accepted: both live
  in the same node's storage, and GC of unreferenced blobs (if ever needed) is a
  scan-the-definitions sweep, not a reference count.

## Consequences

- **probe-protocol**: `CodePayload` deleted; `ToolCall.code: CodeRef { url, sha256 }`.
- **probe**: `fetch_link` becomes the only path (cache keyed by sha256; verification
  unchanged); no other consumer changes.
- **aura**: `meta.rs` gains `CodeBlob` (ns 42) and `ActorDef.code_sha256`;
  `register_inner` writes hash + blob; the remote dispatch arm builds
  `CodeRef` from the stored hash; `EngineConfig`/KDL gains `code_base_url` (no default, error at delivery);
  `PersistedActor.source` → `code_sha256` (the seam struct follows). Boot reload
  rehydrates bytes from the local blob by hash.
- **prism PLAN**: `GET /code/{sha256}` static export entry (beside Phase 1.8); signed-URL
  + cache-key normalization recorded as the confidentiality option, not the default.
- **Tests**: remote_probe e2e gains a test-local static source and drives the real
  fetch + hash-cache path (an upgrade: the wire shape it locks becomes the production
  shape).
