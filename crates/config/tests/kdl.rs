//! Root config acceptance: knus parsing of `aura.kdl`, two-plane storage.

use aura_config::kdl::RootConfig;

#[test]
fn parses_two_plane_storage() {
    let text = r#"
node {
    id "home-node"
    namespace "default"
}

data {
    engine "fjall"
    path "/var/lib/aura/data"
}

"#;
    let cfg = knus::parse::<RootConfig>("test.kdl", text).unwrap();
    assert_eq!(cfg.node.id, "home-node");
    assert_eq!(cfg.data.engine, "fjall");
    assert_eq!(cfg.data.path, "/var/lib/aura/data");
}

#[test]
fn converts_to_engine_config() {
    let text = r#"
node {
    id "n1"
    namespace "default"
}

data {
    engine "fjall"
    path "/tmp/aura-data"
}

"#;
    let cfg = knus::parse::<RootConfig>("test.kdl", text).unwrap();
    let ec = aura_config::EngineConfig::try_from(cfg).unwrap();
    assert_eq!(ec.node_id, "n1");
    assert!(matches!(ec.engine, aura_config::Engine::Fjall));
    assert_eq!(
        ec.data_dir.as_deref(),
        Some(std::path::Path::new("/tmp/aura-data"))
    );
}

#[test]
fn unknown_engine_is_rejected() {
    let text = r#"
node {
    id "n1"
    namespace "default"
}

data {
    engine "sqlite"
    path "/tmp/x"
}

"#;
    let cfg = knus::parse::<RootConfig>("test.kdl", text).unwrap();
    let err = aura_config::EngineConfig::try_from(cfg).unwrap_err();
    assert!(err.contains("sqlite"));
}

#[test]
fn slate_engine_shape_parses_with_s3_block() {
    // slate engine option: S3 endpoint block present (adapter lands with
    // the slate plane work; the config shape is settled now).
    let text = r#"
node {
    id "n1"
    namespace "default"
}

data {
    engine "slate"
    path "/bench/slatedb"
    s3 {
        endpoint "http://127.0.0.1:9000"
        bucket "aura"
        key-env "AURA_S3_KEY"
        secret-env "AURA_S3_SECRET"
    }
}

"#;
    let cfg = knus::parse::<RootConfig>("test.kdl", text).unwrap();
    assert_eq!(cfg.data.engine, "slate");
    let s3 = cfg.data.s3.expect("s3 block");
    assert_eq!(s3.bucket, "aura");
    assert_eq!(s3.key_env, "AURA_S3_KEY");
}
