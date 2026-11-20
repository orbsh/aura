# 0015 — Node identity: a token at the dial, approval by digest

> **Languages:** [English](0015-node-identity-a-token-at-the-dial.md) (primary) · [中文](0015-node-identity-a-token-at-the-dial.zh-CN.md)

**Status:** Accepted (2026-09-20). **Rewritten 2026-11-20 to the scheme it implements** — the record first accepted a per-node ed25519 keypair with a signed handshake; that design is gone, and *Evolution* records what replaced it and why. Landed: the trust-mode switch and its disclosure, the registry of digests with its approval endpoints, the token at the dial, and machine binding (prism ADR-0020). Folding the records into account auth pends Prism's account session surface.

## Context

A node dials the gateway and claims an alias. Two things have to hold: whoever reaches the gateway must be a machine the operator owns, and the control plane must be able to say which one — without a secret shared across a user's nodes, replayable, or stored anywhere it can be read.

The asset is **attribution**, not the node's secrets. Credentials never travel to the node and an operation runs with the node's own local material, so an impostor re-running the same operation gains nothing. What an impostor gains is influence over the control plane: it reads the operation stream routed to that alias, it lies about outcomes, and its results are consumed by the model as input. Node identity protects the attribution of the dialer.

Node identity is aimed at parties that are **not** the control plane. The control plane is the trusted authority by construction: it chooses the operation, holds the agent, and routes every call, so a node neither can nor tries to constrain it. What is fixed here is the other direction — nobody else had to prove anything either.

## Decision

### 1. Identity is one token per node, minted by the control plane

32 random bytes, URL-safe base64 without padding, minted by the node registry and shown exactly once, in the response that mints it. It rides the **dial** as a query parameter — `/effector/<alias>?token=…` — because a URL query needs no escaping, and a socket's arrival is the one moment a peer is answered before it is served.

Nothing about the token travels inside a frame. A proof carried in a frame is a credential inside every log that carries frames, and the connection already carries it.

### 2. The registry stores a digest, never the token

The row holds the SHA-256 hex of the token: compared, never parsed. A node token is already high-entropy randomness, so a slow KDF buys nothing here; what matters is that a dump of the node table is not a set of keys. The token itself lives where the node lives — its config file, or `--token` for the container form; effector ADR-0003 §11 keeps it off argv and out of the environment, where another process on the machine could read it.

### 3. The dial is the handshake; `Register` declares shape only

There is no challenge and no signature. The URL path is the claim (`/effector/<alias>`), the token on it is checked before the socket is served, and the node's first frame says what it IS — alias, carriers, class, machine (effector ADR-0006) — never what it knows.

What the gateway answers, and from which row:

| posture | the peer presents | answer |
|---|---|---|
| `required` | no token | refused, and the reason says how to mint one |
| `required` | a digest with no approved row | `pending` — the claim queues itself so the operator sees the attempt |
| `required` | the approved digest for that alias | `registered` |
| `required` | a digest approved for another record under that alias | `conflict` — refused and surfaced |
| `required` | the approved digest, from a machine the token is not bound to | `conflict` — refused, both machines named (prism ADR-0020) |
| `open` | anything, token or not | `registered`; replacing a live connection is surfaced |

### 4. Approval is by digest, at the control plane

The connection plane — Prism — keeps the registry: `{alias, credential, status, created_at, labels, owner, machine}` with `status` in `pending | approved | revoked`. Approving means moving a specific digest to `approved`; the alias stays a human-chosen name and stops being security-relevant, because two nodes sharing an alias are distinguishable by digest and a mismatch is `conflict`, never a takeover.

Two facts were added to this record later and belong to it: `labels` are the names the operator gives the machine at minting, and `owner` whose they are (prism ADR-0020); `machine` is written by the first approved registration and binds the token to that hardware (effector ADR-0006).

### 5. HTTP surface, curl-first (interim until accounts exist)

```
POST   /admin/nodes                    {alias, labels?, owner?} → mint; the token is in this response, once
GET    /admin/nodes                                             → alias, status, labels, owner, machine
POST   /admin/nodes/{alias}/approve    {credential?}            → approve (the sole pending record when absent)
DELETE /admin/nodes/{alias}            {credential?}            → revoke (rows survive as `revoked`)
```

Four endpoints over one record, hosted by the connection plane (ADR-0017 §7), so they ride Prism's routing surface. When the account system exists, the same records hang under the account: minting sets `owner`, and every endpoint requires the logged-in user, so "the node registry" becomes "your nodes" with no record migration.

### 6. Rotation and revocation

One token per node record. Rotation = mint a new token and approve it; the previously approved record falls to `revoked` — a tombstone, never a silent delete, because the history is the audit. Revocation is the same transition applied to every live record of an alias. Both are actions of the operator (or, later, the account owner), never of the node itself.

Because a token binds to the machine that first registers it (prism ADR-0020), rotation is also how a machine is RENAMED: the name lives with the token, the hardware identity does not.

### 7. Trust mode is a declared deployment-form switch

Authentication is worth exactly what the reachable set behind it is worth. In a private network, behind a VPN, in an in-cluster control channel — where the gateway can only be reached by machines the operator starts, and those machines register themselves at start under predictable names — a token guards an entrance the network already guards. The same holds for a rebound connection: a container inside an isolated network dialling out to a control plane that lives on the operator's own side.

So the mode is a deployment-form declaration, not an assumption — the `identity` field on Prism's gateway config:

```
{ "identity": "required" }   // unapproved or keyless peers are answered, never served
{ "identity": "open" }       // registration by alias alone: the network is the boundary
```

- **The field has no default.** Same structural reason as the declared KV prefix: a gateway that accepts whatever arrives is a state the operator must *choose* and must be able to read in the config, not a state inherited from an absent value.
- **A gateway in `open` mode states its posture at startup** (registrations are unauthenticated), so a deployment's trust mode is visible in its own logs rather than only in its config file.
- **The effector declares the same thing**: with a token it dials; without one it dials keyless. A keyless effector against a `required` gateway is answered `unauthenticated` — an error the operator sees, never a silent downgrade.

**Replacement discipline is the rule that survives in both modes.** In `open` mode a second connection may take over an alias — a restarted container has to be able to reclaim its own name from its stale registration — but the takeover is logged as an event naming the alias, the old peer's address and the new one's. In `required` mode it cannot happen at all: an approved digest is never silently displaced. Silent replacement is how an operator loses track of where their calls actually go, and that is true inside a trusted network too.

**What `open` mode gives up, stated plainly:** attribution, and any defence against another machine inside the same boundary. Whoever can reach the gateway can claim an alias, read the operation stream routed to it, and answer in its name — the payoff named in Context. That is a correct trade when the reachable set is the operator's own machines; it is a wrong trade the moment anything else can reach the gateway. For a rebound deployment the boundary that counts is the **gateway's** reachable set, not the node's: if the control plane itself is exposed to an open network, `open` mode means anyone may claim any alias, and the tunnel has to terminate somewhere that filters. The switch does not make that judgement — the operator does, in one line of config that is legible in review.

## Evolution

This record first accepted a **per-node ed25519 keypair** in WireGuard's format: the node generated the pair and printed the public key, the operator approved that key, and the handshake ran a server nonce which the node signed inside its `Register`. It was dropped when effector ADR-0003 settled the dial as the one place authorization happens (§11: *one token at the dial; the connection is the authority*). Three reasons killed the signature:

- **The proof was travelling inside a frame.** A signature in `Register` is a credential inside every log that carries frames — the exact thing the design was supposed to avoid.
- **The node had nowhere good to keep key material.** A container is handed its credential at launch; a private key file on a node machine is a file to lose, to back up, and to rotate on every redeploy.
- **Revocation on a fleet came down to re-minting anyway**, and a re-minted key is indistinguishable from a re-minted token except in the number of moving parts.

What survived unchanged, and is why the section numbers here did not move: approval is an operator action at the control plane (§4), the four-endpoint curl surface (§5), rotation and revocation as status transitions with tombstones (§6), the declared posture and replacement discipline (§7). What was added later on the same record: labels and owner at minting, and machine binding (prism ADR-0020).

The implementation order this record originally set (trust-mode switch and disclosure → keypair handshake and registry → fold into account auth) was followed for its first two steps and is superseded by the above for the second; the third still pends Prism's account session surface.

## Honest semantic cost

- **Approving a digest is a comparison.** While a record is `pending`, an impostor who can reach the gateway and knows the alias can also be pending. The operator approves the digest the node printed against the one the registry queued; approving a list entry without comparing is a click, not a decision — the same failure mode as an unverified SSH host key.
- **A token in a query string is a token in the logs.** URLs are logged by proxies, servers and shells far more readily than headers or bodies, so a deployment that reaches the gateway across anything it does not control owes TLS, and owes keeping its access logs as carefully as the token.
- **Over plain `ws://` the node cannot tell who it is talking to**, so a deployment outside a trusted network requires TLS with a verified server certificate; the effector should refuse plain `ws://` when configured to require TLS. Today's tests use `ws://`, which is why this is a precondition rather than a detail.
- **The token protects against remote claimants, not a compromised node.** Whoever holds the machine holds the token and can be the node. The mitigation is the deployment form (short-lived, replaceable nodes), not a stronger handshake.
- **What this does not claim.** The control plane is not treated as an adversary: it is the trusted authority and a node cannot constrain it. Keeping tokens out of the venue's own tables is about not creating leak surfaces — a token stored there can escape through a backup, a dump or a log regardless of anyone's intent.
- **Results stay untrusted input.** Authentication changes who may be a node, not what a result is worth: the effector executes AI-generated code, so its output is data for the control plane, never instructions. That ruling is separate from this one and is not satisfied by it.

## Consequences

- **effector-protocol**: `Register` carries `{alias, carriers, class, machine}`; there is no credential field and no challenge frame. The token rides the dial.
- **prism**: the registry rows, the four endpoints, the declared posture and the verdicts of §3 — plus the binding: the first approved registration writes `machine` and every later one must come from it (prism ADR-0020). Auth data is a connection-plane concern (ADR-0017 §7); building it inside aura as well would give the decision two homes.
- **aura**: nodes live in ONE pool every realm borrows, with the labels admission prefixed (aura ADR-0053); placement filters that pool. The gateway correctness that holds regardless of who configures it is the replacement discipline — an alias takeover stays permitted in the open posture, is never silent, and the presence guard is identity-checked so the displaced connection cannot unregister the new holder.
- **Superseded wording**: `engine/src/lib.rs`'s "namespace is derived from the user credential at registration". The namespace hangs off the account; the token only proves the machine — user identity and machine identity are separate axes.
- **Accounts**: until Prism's account session surface exists, a node token is minted by an operator with curl and its `owner` is whatever that operator says. That is an interim shape, stated so it is not read as a permanent one.