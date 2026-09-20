# 0015 — Node identity: per-node ed25519 keypairs, approval by public key, no shared secrets

> **Languages:** [English](0015-node-identity-ed25519.md) (primary) · [中文](0015-node-identity-ed25519.zh-CN.md)

**Status:** Accepted (2026-09-20) — design; implementation pending, see Consequences

## Context

A probe dials the gateway and claims a node alias. Today that claim carries one piece of
evidence: a user-level shared secret read from the environment variable named by
`ProbeConfig.credential_env`. Three facts about the current state:

- **The secret is not checked.** The gateway's register arm takes `node_alias` and `carriers`
  and discards the credential (aura `crates/engine/src/probes.rs`). The field is carried and
  unused; `engine/src/lib.rs`'s "namespace is derived from the user credential at
  registration" describes an intent, not a behaviour.
- **The alias is a claim, not an identity.** Registration inserts into `probes` by alias;
  a second connection claiming the same alias silently replaces the live one. The real node's
  in-flight calls die and subsequent calls — the operation's inline code and arguments — are
  routed to whoever holds the alias, whose answers then enter the control plane as tool
  results.
- **The payoff of that theft is not on the node.** Credentials never travel: the capability
  surface has no credential fields, and an operation runs with the node's own local material,
  so an impostor re-running the same operation gains nothing. What an impostor gains is
  influence over the *control plane*: it reads the user's operation stream, it lies about
  outcomes, and its `ToolResult` content is consumed by the LLM as an input. The asset being
  protected is the attribution of the dialer, not the node's secrets.

The threat model that matters is therefore: whoever reaches the gateway must be a machine the
user owns, and the control plane must be able to say which one — without a secret that is
shared across a user's nodes, replayable, or stored anywhere it can be read.

Node identity is aimed at parties that are **not** the control plane. The control plane is the
trusted authority by construction: it chooses the operation, holds the agent, and routes every
call, so a node neither can nor tries to constrain it. What is being fixed here is the other
direction — today nobody else has to prove anything either.

## Decision

### 1. Identity is a per-node ed25519 keypair, in WireGuard's format

One node, one keypair. The key is a raw 32-byte value carried as base64 text on a single
line; the public key is derived from the private key. No algorithm negotiation (ed25519 only,
which removes the downgrade surface), no PEM, no ASN.1, no certificate chain — the format
carries nothing that has to be parsed by a third party.

The private key is the node's own file, mode 0600, referenced by path from the config
(`identity_file`). Key material never appears in a config file, and `credential_env` — a
user-wide secret in the environment — is deleted.

Note on the format's provenance: the *encoding* is borrowed from WireGuard, not its algorithm
(X25519 ECDH). `wg pubkey` must not be used to derive this public key; derivation is ed25519
and ships as the probe's own `keygen`.

### 2. The node generates the keypair; the control plane only ever receives the public key

Generation is not an extra tool to install: `probe keygen [--out <path>]` mints the pair, writes
the private key 0600 and prints the public key, and a first start with no identity file does the
same thing automatically. The operator's workflow is unchanged — one `curl` submitting the printed
public key; the private key never has to be transported, because it is already where it belongs.
Deployment artifacts (containers, secret stores, images) carry the *public* key, which is not
sensitive.

*Alternative considered — the control plane mints the keypair and returns it over HTTP.* Rejected
for mechanical reasons, not for distrust: the private key would transit the wire, land in the
operator's terminal scrollback, shell history and CI logs, and remain in whatever the minting
service recorded, so the set of places that can impersonate the node grows for no benefit — local
generation gives the same one-command workflow (`probe keygen`, then `curl` the public key). Keeping
generation on the node also keeps one future direction open: a non-exportable key (TPM / Secure
Enclave) can only ever be generated there, and server-side minting forecloses it permanently.

The argument that is *not* being made: the control plane is the trusted authority by construction
(it chooses the operation, holds the agent, routes every call), so "it could impersonate a node"
is not a threat and its motives are not a design input. What is at stake is only a secret existing
in places that gain nothing from holding it.

### 3. Handshake: server nonce, signature, no pairing code

```
gateway → probe   {"type":"challenge","nonce":"<32 bytes hex>"}
probe   → gateway {"type":"register","node_alias":"home-pc","public_key":"<base64>",
                   "signature":"<ed25519 over the nonce, hex>"}
gateway → probe   {"type":"registered"} | {"type":"pending"} | {"type":"conflict"}
```

The gateway speaks first (as SSH's server sends its banner first), the node proves possession,
and no replay of a previous connection is possible. In `open` mode (§7) the probe registers
without key material and this exchange is skipped. Three answers:

- `registered` — the key is approved for that alias; serve normally.
- `pending` — no decision for this key yet (the alias is new, or its key is awaiting
  approval). The probe logs that it needs approval and retries with backoff.
- `conflict` — a *different* key is approved for this alias. Refused, and surfaced as an
  event: an alias's enrolled key is never silently replaced. This is the current behaviour
  being removed.

A pairing code was considered and dropped: its only job is to avoid trust-on-first-use, and
with approval-by-public-key plus (later) account binding, the code would be a second
authorization path to the same record.

### 4. Approval is by public key, at the control plane

The control plane keeps a node registry — `{ alias, public_key, status, created_at }`, and
later `{ user }` — where `status` is `pending | approved | revoked`. Approving a node means
approving a specific public key; the alias remains a human-chosen display label and stops
being security-relevant (two nodes sharing an alias are distinguishable by key, and a
mismatch is `conflict`, not a takeover).

### 5. HTTP surface, curl-first (interim until accounts exist)

```
POST   /nodes                 {alias, public_key}   → creates a pending record
GET    /nodes                                       → alias, public key, status
POST   /nodes/{alias}/approve                       → approve that public key
DELETE /nodes/{alias}                               → revoke
```

The surface is deliberately four endpoints over one record. When the account system exists
(Prism's auth, Phase 1), the same records hang under the account: creation sets `user`, and
every endpoint requires the logged-in user, so "the node registry" becomes "your nodes" with
no record migration.

### 6. Rotation and revocation

One key per node record. Rotation = approve a new key, which replaces the old one in the
approved set; revocation = delete the record. Both are actions of the authenticated operator
(or, later, the account owner) — never of the node itself.

### 7. Trust mode is a declared deployment-form switch

Authentication is worth exactly what the reachable set behind it is worth. In a private
network, behind a VPN, in an in-cluster control channel — where the gateway can only be reached
by machines the operator starts, and those machines register themselves at start under
predictable names — a keypair handshake guards an entrance the network already guards. The same
holds for a rebound connection: a container inside an isolated network dialling out to a control
plane that lives on the operator's own side.

So the mode is a deployment-form declaration, not an assumption:

```
{ "identity": "required" }   // ed25519 handshake: unapproved or keyless peers are answered, never served
{ "identity": "open" }       // registration by alias alone: the network is the boundary
```

- **The field has no default.** Same structural reason as the declared KV prefix: a gateway that
  accepts whatever arrives is a state the operator must *choose* and must be able to read in the
  config, not a state inherited from an absent value.
- **A gateway in `open` mode states its posture at startup** (registrations are unauthenticated),
  so a deployment's trust mode is visible in its own logs rather than only in its config file.
- **The probe declares the same thing**: with an identity file it signs, without one it registers
  keyless. A keyless probe against a `required` gateway is answered `unauthenticated` — an error
  the operator sees, never a silent downgrade.

The complete behaviour difference:

| gateway mode | the peer presents | answer |
|---|---|---|
| `required` | an approved key | `registered` |
| `required` | an unknown key | `pending` |
| `required` | no key | `unauthenticated` |
| `required` | a key enrolled for a different record under that alias | `conflict` — refused and surfaced |
| `open` | anything, key or not | `registered`; replacing a live connection is surfaced |

**Replacement discipline is the rule that survives in both modes.** In `open` mode a second
connection may take over an alias — a restarted container has to be able to reclaim its own name
from its stale registration — but the takeover is logged as an event naming the alias, the old
peer's address and the new one's. Silent replacement is how an operator loses track of where
their calls actually go, and that is true inside a trusted network too.

**What `open` mode gives up, stated plainly:** attribution, and any defence against another
machine inside the same boundary. Whoever can reach the gateway can claim an alias, read the
operation stream routed to it, and answer in its name — the payoff named in Context. That is a
correct trade when the reachable set is the operator's own machines; it is a wrong trade the
moment anything else can reach the gateway. For a rebound deployment the boundary that counts is
the **gateway's** reachable set, not the node's: if the control plane itself is exposed to an
open network, `open` mode means anyone may claim any alias, and the tunnel has to terminate
somewhere that filters. The switch does not make that judgement — the operator does, in one line
of config that is legible in review.

## Honest semantic cost

- **Approving a fingerprint is a comparison.** While a record is `pending`, an impostor who
  can reach the gateway and knows the alias can also be pending. The operator must compare the
  public key they are approving against the one the node printed. Approving a list entry
  without comparing is a click, not a decision — the same failure mode as an unverified SSH
  host key.
- **The challenge prevents replay, not an impostor server.** Over plain `ws://` the probe
  cannot tell who it is talking to, so a deployment outside a trusted network requires TLS
  with a verified server certificate; the probe should refuse plain `ws://` when configured to
  require TLS. Today's tests use `ws://`, which is why this is a precondition rather than a
  detail.
- **The key protects against remote claimants, not a compromised node.** Whoever holds the
  machine holds the private key and can be the node. The mitigation is the deployment form
  (short-lived, replaceable nodes), not a stronger handshake.
- **What this does not claim.** The control plane is not treated as an adversary: it is the
  trusted authority, and a node cannot constrain it (§2). Keeping private keys off the control
  plane is about not creating leak surfaces — a key stored there can escape through a backup, a
  dump or a log regardless of anyone's intent.
- **Results stay untrusted input.** Authentication changes who may be a node, not what a
  result is worth: the probe executes AI-generated code, so its output is data for the control
  plane, never instructions. That ruling is separate from this one and is not satisfied by it.

## Consequences

- **probe-protocol**: `challenge` becomes a frame; `register` gains `public_key` and
  `signature`; `pending` and `conflict` become answers; `credential` disappears. An
  unapproved node is answered, never dropped.
- **probe**: `keygen` (and first-start generation), `identity_file` in the config,
  `credential_env` deleted.
- **aura gateway**: a node registry of approved keys replaces alias-inserts; the interim HTTP
  surface; the silent replacement path is removed.
- **Superseded wording**: `engine/src/lib.rs`'s "namespace is derived from the user credential
  at registration". The namespace hangs off the account; the key only proves the machine —
  user identity and machine identity are separate axes.
- **Both ends move together** (probe-protocol is a path dependency), so this lands as one
  coordinated change, not a compatible extension.

**Implementation order** (stated so it is not done in the wrong order):

1. **Trust-mode switch and disclosure** — the `identity` field (no default) on the gateway, the
   `open` registration path, the `unauthenticated` answer, and replacement surfacing. No key
   material, no crypto dependency: this is what makes in-cluster and VPN deployments honest and
   usable today, and it removes the silent alias replacement that is wrong in every mode.
2. **Keypair handshake and node registry** — the `required` mode above: `challenge`, the signed
   `register`, `pending` / `conflict`, the node registry and its four endpoints.
3. **Fold into account auth** — when Prism's auth exists, the records gain their owner and the
   endpoints require a session.

Until step 2 is configured for a deployment, that deployment runs in `open` mode and its
protection is the network boundary; the config says so, and the startup log says so.
