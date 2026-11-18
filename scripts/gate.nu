#!/usr/bin/env nu
# aura's gate: the default workspace, then the surfaces a plain `cargo test`
# never compiles.
#
# Why this file exists: every engine test that touches the python face sits
# behind `#![cfg(feature = "python")]`, so the default gate skips those FILES
# entirely. When okm-python renamed its one write verb (`merge` -> `put`), the
# sweep covered the workspace and the python tests stayed green-by-not-running —
# they were invisible, not passing.
#
# Usage:  nu scripts/gate.nu        (stops at the first failing gate, exit 1)
#
# A new feature-gated surface belongs here, beside the python one: the rule is
# that a gate must COMPILE what it claims to cover.

$env.NO_PROXY = "*"

# pyo3 links libpython, and nix keeps it out of the linker's search path. Asked
# of the interpreter rather than written down, so a python bump does not rot it.
let libdir = (^python3 -c "import sysconfig; print(sysconfig.get_config_var('LIBDIR'))" | str trim)

print $"(ansi green_bold)-- gate 1: workspace, default features(ansi reset)"
^cargo test --workspace
if $env.LAST_EXIT_CODE != 0 {
    error make {msg: "the default workspace gate failed"}
}

$env.RUSTFLAGS = $"-L native=($libdir)"

print $"(ansi green_bold)-- gate 2: aura-engine with --features python, which gate 1 skips(ansi reset)"
^cargo test -p aura-engine --features python
if $env.LAST_EXIT_CODE != 0 {
    error make {msg: "the python gate failed"}
}

print $"(ansi green_bold)-- both gates passed(ansi reset)"