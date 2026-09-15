# Actor API (Script Language Reference)

> Script contracts and host functions per language. English primary;
> Chinese counterpart: [actor-api.md](actor-api.md).
> Execution model and design background: [realm.md](realm.md);
> introspection mechanism: [§5.4](realm.md#54-interface_schema).

## Lifecycle (three separate lines)

```
UPLOAD (set)     its own lifecycle; may never execute
  └─ host introspects once (calls interface_schema(), or derives from @on decorators)
  └─ metadata (receives/wildcard_receives/lifecycle) extracted and persisted to the meta store
  └─ receives derives the delivery routes (event → type + key field)
EXECUTION        per message: load script (latest version) → address handler by event name → run
  └─ interface_schema is NEVER called — it is already a static record in the meta store
VERSION CHANGE   a new `set` re-introspects once and updates the persisted metadata and routes;
                 until then the old metadata governs
```

## Event-Driven Model (Multi-Entry)

An Actor type is **multi-entry**: `@on` decorators (steel's `on` function,
wasm's export convention) declare which event each handler listens to,
and the event name is the handler's addressing name. There is no single
entry — the single-entry model was asymmetric (one way in, many ways out
via emits) and forced actors with several handlers to split apart,
duplicating shared logic.

```python
@on("add_to_cart", key="user_id")   # partition key declared on the decorator
def add(args): ...

@on("remove_from_cart")             # no key → singleton consumer
def remove(args): ...

@on("order.*")                      # prefix wildcard → wildcard_receives, singleton
def audit(args): ...
```

**Delivery semantics: event queues, not actor mailboxes**. An event
belongs to no actor — `emit("add_to_cart", data)` writes the event into
the `add_to_cart` event queue; a queue for a handler with a declared
`key` partitions by `(event, partition)` (the key field's value comes
from the event data), a queue without a key is one queue per event. A
queue can have **multiple subscribers** (several actor types listening
to one event — one-to-many is structural, not a fan-out simulation).
Actor instances subscribe to queues per their `@on` declarations; a
per-subscription cursor keeps each instance's consumption serial — the
instance does not own the queue.

**Emits are never declared, collected, or validated (ADR-0012)**: the
receiver set of an event is a runtime fact — an emit with no subscriber
lands in the dead-event ring, which is the observable audit surface.
Source-level emit collection is deferred until it serves a real consumer
(the Windmill criterion: parse when the parse drives an action only the
parse can make correct).

**`interface_schema` is assembled implicitly and merged with the explicit
part**: the carrier builds an implicit `interface_schema` at module
assembly (`receives` from `@on` parameters, wildcards into
`wildcard_receives`) and merges it field-wise with a partial explicit
declaration the script writes — decorators own receives, the script
contributes lifecycle and anything else the decorators cannot express.
The single merged function is the only thing aura calls. rust/wasm get
the same implicit function from `#[on(x)]` annotations; steel/nushell
hand-write this one function.

## Common Contract (All Languages)

A script Actor = **one source file** + **a set of handler functions**:

| Function | Required | Purpose |
|----------|----------|---------|
| handlers (multiple `@on` functions) | yes | Message-handling entries; the event name maps to the function argument. Event delivery addresses the handler by event name; a direct call (`ctx.invoke` / engine `invoke`) declares in its payload which handler to call |
| `interface_schema(args)` | optional | Hand-written metadata (lifecycle); derived from decorators when absent |

**Execution contract**: JSON in (one argument, already decoded into a
structured value), JSON-serializable value out; failures surface as the
language's native exceptions/errors, which the host converts to error
values — never a panic.

**Host functions** (the ctx bridge, Phase 2.5): scripts may call the
following — each takes one JSON argument and returns a JSON value:

- `ctx_state_get(field)` → `{"present": bool, "value": ...}` (reads this
  instance's state field; **this instance only** — cross-instance access
  is not expressible)
- `ctx_state_set({"field": ..., "value": ...})` → `{"ok": true}`
- `ctx_state_delete(field)` → `{"ok": true}`
- `ctx_invoke({"type": ..., "key": ..., "handler": ..., "args": ...})` → the target
  Actor's return value (blocking wait through the unified call model;
  timeout = failure value)

**Language capability matrix**:

| | python | steel | nushell | wasm |
|---|---|---|---|---|
| In-process | ✅ | ✅ | ❌ subprocess | ✅ VM |
| ctx host functions | ✅ | ✅ | ❌ (explicit error) | via frame up-call (Phase 6.6) |
| Resident VM (Phase 2.6) | ✅ | ✅ | ❌ one-shot | ✅ |
| `@on` multi-entry | ✅ decorator | `on` fn | one handler for now (direct calls declare the handler name) | export convention |
| Best for | business logic | AI-generated ops | pipeline/CLI shape | heavily-isolated 3rd-party code |

**`interface_schema` is transparent to the execution path**: the probe
execution carrier only does "load source → call entry → serialize
result" and never touches `interface_schema`. Declaration collection
(python `@on` injection, steel `on` builtin) is language-shape; the
assembly and merge semantics live in the one `carrier::introspect` call
that aura makes at upload and persists. The same script handed to probe
execution has an `interface_schema` nobody calls; handed to aura upload,
it becomes the type definition's metadata source.

---

## Python

[中文](#python)

```python
@on("add_to_cart", key="user_id")
def add(args):
    # args: the decoded JSON value (dict/list/...), not a string
    ctx_state_set('{"field": "visits", "value": 1}')   # host fns take a JSON string
    got = ctx_state_get('{"field": "visits"}')
    echo = ctx_invoke('{"type": "echo", "key": "k1", "args": {"x": 1}}')
    return {"stored": got["value"], "echo": echo["x"]}

@on("remove_from_cart")
def remove(args):
    return {"removed": True}

@on("order.*")          # wildcard: listens to a class of events, singleton instance
def audit(args):
    return None

# optional explicit partial declaration: merged field-wise with the
# decorator-derived receives (decorators own receives; this adds lifecycle etc.)
def interface_schema(args=None):
    return {"lifecycle": {"idle_ttl": "5m"}}   # number = seconds; string needs a unit s/m/h
```

Notes:

- host-function arguments are **one JSON string** (decoded at the
  carrier boundary) — build them with `json.dumps(...)`; return values
  are already native dicts (no `json.loads` needed)
- the `@on` decorator is injected by the carrier; scripts never define
  `on` themselves; the decorator is an identity transform — decorated
  functions remain directly callable
- wildcards are prefix-only (`prefix.*`, etcd-style), matching
  `order.created` but not `order`; wildcard-declared handlers route to
  the singleton instance
- **a direct call declares which function to call**: the `ctx_invoke`
  payload must carry a `handler` field (the function name), and so must
  engine `invoke(target, handler, args)` — no reserved function names, no
  implicit entry; when registered without an entry, a module-level
  `result` variable also works

## Steel

[中文](#steel)

```scheme
;; Multi-entry: the carrier-injected `on` builtin declares listeners
;; (collected while the body runs). Args: event, key field (empty string =
;; singleton), handler.
(on "add_to_cart" "user_id"
  (lambda (args)
    (ctx_state_set "{\"field\": \"visits\", \"value\": 1}")
    (let* ((got (ctx_state_get "{\"field\": \"visits\"}"))
           (echoed (ctx_invoke "{\"type\": \"echo\", \"key\": \"k1\", \"handler\": \"execute\", \"args\": {\"x\": 1}}")))
      (hash "visits" (hash-ref got "value")
            "echo" (hash-ref echoed "x")))))

(on "order.*" "" (lambda (args) #t))   ;; wildcard → wildcard_receives

;; Optional explicit partial declaration: merged field-wise with the
;; collector-derived receives (collector owns receives; this adds lifecycle)
;; — use hash, not alist (pairs have no JSON mapping)
(define (interface_schema args)
  (hash "lifecycle" (hash "idle_ttl" "5m")))
```

Host-function arguments are JSON strings (native steel values marshal
automatically); **return values are native steel values** — hashes,
numbers, and booleans are directly usable, no JSON string re-parsing.
Alists (`'((k . v))`) have no JSON marshal — declaration structures use
`hash` exclusively.

No-entry semantics: define a `*result*` variable in the source.

## Nushell

[中文](#nushell-2)

```nu
# Subprocess execution: no ctx host functions (cannot call back into the
# host — script actors needing ctx must use an in-process carrier);
# no VM residency (one-shot, no memory space)
export def execute [args] {
    { sum: ($args.items | math sum) }
}
```

- The entry must be `export def <name>`; a bare `main` is not addressable
  through module import and is explicitly rejected
- The argument is one parsed value (record/list), not a string; the return
  value must survive `to json --raw`
- nushell carries a **single direct-call handler** (`execute`; the
  subprocess shape does not match multi-entry addressing);
  `interface_schema` declarations work through the generic wrapper
  (`export def interface_schema [args]` can declare a lifecycle TTL);
  scenarios needing `@on` multi-entry + ctx use python/steel

## Wasm (written in Rust)

[中文](#wasm-rust-用-rust-编写)

The only release form for Rust services: compile to `.wasm` and upload at
runtime (`set(lang="wasm", bytes)`), not into the host binary — compiling
them in would fork the platform per app, collapsing the platform into a
framework. Storage never enters the sandbox: the OKM schema compiles into
the wasm unchanged, with the `VirtualStorage` implementation swapped for
a frame up-call — the host-side NestStorage executor carries the physical
store under a registry-allocated app ns prefix (ADR-0007 storage-carriage
split). Static OKM derives; no okm-dynamic needed.

Convention (Phase 4 `link` payload lands pointer marshalling):

```rust
// Build target wasm32-wasi; the module exports one of:
// 1. A WASI command: export _start (args via WASI)
// 2. A typed export: execute(i64) -> i64 (args JSON pointer in, result JSON pointer out)
#[no_mangle]
pub extern "C" fn execute(args_ptr: i64) -> i64 {
    // Linear-memory marshalling lands with Phase 4 link payloads
    // (MB-scale bytes, hash-verified before execution)
    todo!()
}
```

- Multi-entry export convention: each handler exports as a function
  named after its event (`add_to_cart(i64) -> i64`) — the event name is
  the export name, and `interface_schema` derives from the export list
  (lands with Phase 4.5c step 3)
- The `interface_schema` declaration path matches python/steel: export a
  function of the same name returning JSON (pointer convention) — it
  takes effect at upload-time introspection
- Host imports are deliberately minimal: no fs, no network — the
  capability surface (Phase 5) decides what is granted
- The aura engine ships **no in-process Rust actor** — framework
  mechanics (the evictor class) are plain realm logic; Rust code becomes
  an actor through exactly one channel: compile to wasm and upload

---

## Relationship to probe (restated)

probe = **the operation execution plane**: `ToolCall` in → `execute()` →
`ToolResult` out. It knows nothing about Actors, events, or the semantics
of `interface_schema` — all of these are **aura's field-layer concepts**.
The same python file: as a probe operation only `execute` is called; as an
aura Actor the introspection runs first and every `@on` handler becomes
one of the instance's message entries. One file, two hosts, transparent
contracts.
