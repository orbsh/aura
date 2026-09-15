# Actor API (Script Language Reference)

> Script contracts and host functions per language. English primary;
> Chinese counterpart linked at each section end.
> Execution model and design background: [realm.md](realm.md);
> introspection mechanism: [§5.4](realm.md#54-interface_schema).

## Common Contract (All Languages)

A script Actor = **one source file** + **two conventional entry points**:

| Entry | Required | Purpose |
|-------|----------|---------|
| `interface_schema(args)` | optional | Called once by the host at registration; declares metadata (event contract, residency policy). Pure function, no side effects; the argument is ignored |
| `execute(args)` (or a custom entry) | yes | Message-handling entry; the event name maps to the function argument |

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
- `ctx_invoke({"type": ..., "key": ..., "args": ...})` → the target
  Actor's return value (blocking wait through the unified call model;
  timeout = failure value)

**Language capability matrix**:

| | python | steel | nushell | wasm |
|---|---|---|---|---|
| In-process | ✅ | ✅ | ❌ subprocess | ✅ VM |
| ctx host functions | ✅ | ✅ | ❌ (explicit error) | via frame up-call (Phase 6.6) |
| Best for | business logic | AI-generated ops | pipeline/CLI shape | heavily-isolated 3rd-party code |

**`interface_schema` is transparent to probe**: a probe carrier only does
"load source → call entry → serialize result" — `interface_schema` is to
it just another function call with no special meaning. Aura is the only
party that gives it meaning: the host calls it once at registration for
metadata introspection (event contract, `lifecycle.idle_ttl`). The same
script handed to probe execution has a dead `interface_schema` that nobody
calls; handed to aura registration, it becomes the type definition's
metadata source.

---

## Python

[中文](#python)

```python
# Metadata declaration (optional; called once at registration)
def interface_schema(args=None):
    return {
        "receives": {
            "add_to_cart": {"key": "user_id"}
        },
        "emits": ["cart_updated"],
        "lifecycle": {"idle_ttl": "5m"}   # number = seconds; string needs a unit s/m/h
    }

# Message entry
def execute(args):
    # args: the decoded JSON value (dict/list/...), not a string
    ctx_state_set('{"field": "visits", "value": 1}')
    got = ctx_state_get(json.dumps("visits"))   # host fns take a JSON string
    echo = ctx_invoke('{"type": "echo", "key": "k1", "args": {"x": 1}}')
    return {"stored": got["value"], "echo": echo["x"]}
```

Note: host-function arguments are **one JSON string** (decoded at the
carrier boundary) — build them with `json.dumps(...)`; return values are
already native dicts (no `json.loads` needed).

No-entry semantics: setting a module-level `result` variable also works
(when registered without an entry).

## Steel

[中文](#steel)

```scheme
;; Metadata declaration (optional)
(define (interface_schema args)
  '((lifecycle . ((idle_ttl . "5m")))))

;; Message entry
(define (execute args)
  (ctx_state_set "{\"field\": \"visits\", \"value\": 1}")
  (let* ((got (ctx_state_get "\"visits\""))
         (echoed (ctx_invoke "{\"type\": \"echo\", \"key\": \"k1\", \"args\": {\"x\": 1}}")))
    (hash "visits" (hash-ref got "value")
          "echo" (hash-ref echoed "x"))))
```

Host-function arguments are JSON strings (native steel values marshal
automatically); **return values are native steel values** — hashes,
numbers, and booleans are directly usable, no JSON string re-parsing.

No-entry semantics: define a `*result*` variable in the source.

## Nushell

[中文](#nushell-2)

```nu
# Subprocess execution: no ctx host functions (cannot call back into the
# host — script actors needing ctx must use an in-process carrier)
export def execute [args] {
    { sum: ($args.items | math sum) }
}
```

- The entry must be `export def <name>`; a bare `main` is not addressable
  through module import and is explicitly rejected
- The argument is one parsed value (record/list), not a string; the return
  value must survive `to json --raw`
- `interface_schema` declarations do not take effect on nushell (the
  registration-time subprocess introspection is technically feasible but
  not wired; nushell Actors declare TTL host-side)

## Wasm (written in Rust)

[中文](#wasm-rust-用-rust-编写)

The carrier for third-party untrusted code: hardware-grade isolation
(Wasmtime), distinct from the soft isolation of in-process VMs.

Convention (skeleton; pointer marshalling lands with Phase 4 `link`
payloads):

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

- Host imports are deliberately minimal: no fs, no network — the
  capability surface (Phase 5) decides what is granted
- **The only release form for Rust services** — storage-bearing services
  of the k10r/gravity class compile to `.wasm` and upload at runtime
  (`set(lang="wasm", bytes)`), not into the host binary: compiling them
  in would fork the platform per app (every new service = repackage),
  collapsing the platform into a framework. OKM schema code compiles into
  the wasm unchanged, and storage goes through a `VirtualStorage` frame
  up-call — the host's NestStorage executor (Phase 6.6) carries the
  physical store under a registry-allocated app ns prefix (ADR-0007,
  storage-carriage split). Static OKM derives; no okm-dynamic needed
- `ActorType::simple` (in-process Rust closure) is **builtin-only** —
  framework mechanics (evictor-class) and tests; not a service release
  path. The channel for loading Rust services is wasm, not dylibs or
  compile-time
- The `interface_schema` declaration path matches python/steel (export a
  function of the same name returning JSON) and takes effect at
  registration

---

## Relationship to probe (restated)

probe = **the operation execution plane**: `ToolCall` in → `execute()` →
`ToolResult` out. It knows nothing about Actors, events, or the semantics
of `interface_schema` — all of these are **aura's field-layer concepts**.
The same python file: as a probe operation only `execute` is called; as an
aura Actor the `interface_schema` runs first and `execute` becomes the
instance's message entry. One file, two hosts, transparent contracts.
