//! The venue as its own node: answering `ctx.resource` for code that runs IN-PROCESS (ADR-0003 §1 —
//! the node is what contains the code, and the venue contains its own).
//!
//! Over the wire this is two parties: the plane mints the grant (`prism`, from the registry row) and
//! the node moves the bytes (`effector`). In-process there is nobody to ask and no wire to wait on, so
//! the venue does both — one call, mint then move, and the script is told a path beside the venue.
//!
//! What this file deliberately does NOT do: invent a second transfer implementation. `effector`'s
//! `resource::ensure/sync` already move git and object data, and a venue that grew its own `git clone`
//! would be a second answer to "what does `remote` mean" — the two would drift, and the drift would be
//! invisible because each side's tests would still pass. So the venue calls the node's code with the
//! node's root convention, and the only thing local is WHERE that root sits.

use crate::resources::{Mode, Registry};
use aura_booth::ResourceOp;
use effector_runtime::resource::{self, Spec};
use std::path::{Path, PathBuf};

/// Where materialised data lands for the venue's own executions.
///
/// The same variable the node reads, because it is the same fact: "the directory this process puts
/// fetched data in". A venue therefore answers where it sits, not where some machine does — the data
/// is fetched to the venue's disk, which is the point of the in-process class (ADR-0003 §1).
fn root() -> PathBuf {
    std::env::var("EFFECTOR_RESOURCES_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")).join("resources"))
}

/// The venue's answer to "was this cancelled?": the activation's STOP BIT (ADR-0047).
///
/// A node has a per-call control because the plane talks to it about one call; a venue has no such
/// conversation — what an operator cancels here is the ACTIVATION, and from the transfer's point of
/// view that is the same question. Reading the bit rather than subscribing to an event is the same
/// decision the stop arm already made: a delivery to a busy instance queues behind the work it means
/// to end.
pub struct StopBit<'a> {
    stops: &'a crate::stop::Stops,
    id: &'a aura_booth::InstanceId,
}

impl<'a> StopBit<'a> {
    pub fn new(stops: &'a crate::stop::Stops, id: &'a aura_booth::InstanceId) -> Self {
        Self { stops, id }
    }
}

impl resource::Interrupt for StopBit<'_> {
    fn cancelled(&self) -> bool {
        self.stops.is_set(self.id)
    }
}

/// Answer one `ctx.resource` ask from the venue's own side.
///
/// `tenant` is the account the activation runs for — recorded once at the entrance from the turn's
/// envelope (`Instance.owner`), never re-derived from an id's shape and never named by the script.
/// A missing account is a refusal, not a default: materialising for "somebody" would be minting a
/// grant nobody asked for.
pub fn ask(
    registry: &Registry,
    tenant: Option<&str>,
    op: &ResourceOp,
    cancel: Option<&dyn resource::Interrupt>,
) -> Result<String, String> {
    let tenant = tenant.ok_or(
        "ctx.resource: this activation runs for no account, and a resource is minted for the turn's \
         own account — the venue refuses to guess whose slice this is",
    )?;
    let write = op.write().map_err(|e| e.to_string())?;
    let material = registry
        .material(tenant, &op.kind, write)
        .map_err(|e| e.to_string())?;
    let spec = Spec::from_plane(&op.kind, mode_text(material.mode), op.at.as_deref())
        .map_err(|e| e.to_string())?;
    let reach = resource::Material {
        base: material.base,
        credential: material.credential,
    };
    let mount = op.mount.as_deref().map(Path::new);
    let root = root();
    let path = resource::ensure(&spec, &reach, &op.remote, mount, &root, cancel)
        .map_err(|e| e.to_string())?;
    if !write {
        return Ok(path.to_string_lossy().into_owned());
    }
    // The reference the store answered is the caller's handle on what it just wrote — a git
    // `<branch>@<sha>`, an object's ETag. It goes back to the script; the venue keeps no copy.
    resource::sync(&spec, &reach, &op.remote, mount, &root, cancel).map_err(|e| e.to_string())
}

fn mode_text(mode: Mode) -> &'static str {
    mode.as_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::Grant;
    use crate::mq::MqStore;

    /// A bare repository with one commit on `main`, so `ensure` has something to clone.
    fn a_repo_with_one_commit(base: &Path, name: &str) -> String {
        let bare = base.join(name);
        let work = base.join(format!("{name}-work"));
        let git = |dir: &Path, args: &[&str]| {
            let out = std::process::Command::new("git")
                .args(["-c", "user.email=a@b", "-c", "user.name=t"])
                .args(args)
                .current_dir(dir)
                .env("NO_PROXY", "*")
                .output()
                .expect("git runs");
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        };
        std::fs::create_dir_all(&bare).expect("a dir");
        git(&bare, &["init", "--bare", "-q", "."]);
        std::fs::create_dir_all(&work).expect("a dir");
        git(&work, &["init", "-q", "-b", "main", "."]);
        std::fs::write(work.join("readme.txt"), "hello from the venue\n").expect("a file");
        git(&work, &["add", "."]);
        git(&work, &["commit", "-q", "-m", "one"]);
        git(&work, &["remote", "add", "origin", bare.to_str().unwrap()]);
        git(&work, &["push", "-q", "origin", "main"]);
        format!("{}/{name}", base.display())
    }

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("aura-resource-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("a scratch dir");
        dir
    }

    #[test]
    fn the_venue_mints_and_moves_so_an_in_process_script_gets_a_path() {
        let dir = scratch("get");
        // The row's base is composed with the tenant and kind (`<base>/<tenant>/<kind>/<remote>`), so
        // the repository lives one level deeper than the declaration — the prefix is this code's, not
        // the operator's to spell.
        let base = dir.join("base");
        // The name the script asks for IS the last segment: `remote` is what an S3 client would call
        // the key, and the base is the prefix this code composed.
        a_repo_with_one_commit(&base.join("alice").join("git"), "mine");
        std::env::set_var("EFFECTOR_RESOURCES_ROOT", dir.join("root").to_str().unwrap());

        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        registry
            .declare(
                "git",
                &Grant {
                    desc: String::new(),
                    base: base.display().to_string(),
                    mode: Mode::ReadOnly,
                    secret_name: String::new(),
                },
                1,
            )
            .expect("declared");
        let op = ResourceOp::parse(&serde_json::json!({
            "op": "get", "kind": "git", "remote": "mine"
        }))
        .expect("parses");

        let path = ask(&registry, Some("alice"), &op, None).expect("the venue answers with a path");
        assert!(
            path.starts_with(dir.join("root").to_str().unwrap()),
            "the path sits beside the venue, under its own root: {path}"
        );
        let file = std::path::Path::new(&path).join("readme.txt");
        assert_eq!(
            std::fs::read_to_string(&file).expect("the clone landed"),
            "hello from the venue\n"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The in-process side of the same rule the node learned: a cancelled activation does not keep
    /// moving bytes. Here the token is already up before the ask, which is the honest worst case —
    /// nothing may be created at all.
    #[test]
    fn a_cancelled_activation_never_starts_the_transfer() {
        struct Up;
        impl resource::Interrupt for Up {
            fn cancelled(&self) -> bool {
                true
            }
        }

        let dir = scratch("cancelled");
        std::env::set_var("EFFECTOR_RESOURCES_ROOT", dir.join("root").to_str().unwrap());
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        registry
            .declare(
                "git",
                &Grant {
                    desc: String::new(),
                    base: dir.display().to_string(),
                    mode: Mode::ReadOnly,
                    secret_name: String::new(),
                },
                1,
            )
            .expect("declared");
        let op = ResourceOp::parse(&serde_json::json!({
            "op": "get", "kind": "git", "remote": "mine"
        }))
        .expect("parses");

        let err = ask(&registry, Some("alice"), &op, Some(&Up)).unwrap_err();
        assert!(err.contains("cancelled"), "{err}");
        assert!(
            !dir.join("root").join("alice").join("git").join("mine").exists(),
            "a cancelled activation leaves nothing behind"
        );
    }

    #[test]
    fn an_activation_with_no_account_gets_a_refusal_not_somebody_elses_slice() {
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        let op = ResourceOp::parse(&serde_json::json!({
            "op": "get", "kind": "git", "remote": "mine"
        }))
        .expect("parses");
        let err = ask(&registry, None, &op, None).unwrap_err();
        assert!(err.contains("no account"), "{err}");
    }

    #[test]
    fn a_read_only_grant_refuses_the_publish_half_before_the_transfer_runs() {
        let registry = Registry::new(MqStore::mem()).expect("a mem store takes the schemas");
        registry
            .declare(
                "git",
                &Grant {
                    desc: String::new(),
                    base: "/nonexistent-on-purpose".into(),
                    mode: Mode::ReadOnly,
                    secret_name: String::new(),
                },
                1,
            )
            .expect("declared");
        let op = ResourceOp::parse(&serde_json::json!({
            "op": "publish", "kind": "git", "remote": "mine"
        }))
        .expect("parses");
        let err = ask(&registry, Some("alice"), &op, None).unwrap_err();
        assert!(err.contains("read-only"), "{err}");
    }
}