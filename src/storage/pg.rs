//! PostgreSQL connection and versioned schema migrations.
use crate::{Error, Result};
use postgres::{Client, NoTls};

/// Applied in order; never edit a released entry, append a new version instead.
pub(crate) const MIGRATIONS: &[(i32, &str)] = &[
    (1, include_str!("migrations/0001_initial.sql")),
    (2, include_str!("migrations/0002_replay_jobs.sql")),
];

/// Connection errors are reduced to a class: driver messages can echo the URL or credentials.
/// Loopback and Unix sockets may be plaintext; every other host requires authenticated TLS.
pub(crate) fn connect(url: &str) -> Result<Client> {
    use postgres::config::SslMode;
    let mut config: postgres::Config = url.parse().map_err(|_| Error("storage_open_failed"))?;
    if loopback(&config) && config.get_ssl_mode() != SslMode::Require {
        return config
            .connect(NoTls)
            .map_err(|_| Error("storage_open_failed"));
    }
    if config.get_ssl_mode() == SslMode::Disable {
        return Err(Error("storage_tls_required"));
    }
    // `prefer` would fall back to plaintext if the server declined TLS.
    config.ssl_mode(SslMode::Require);
    // A pin authenticates only the certificate; binding SCRAM to this TLS channel also
    // proves the server holds the password verifier, defeating a relaying intermediary.
    if std::env::var_os("INDEXER_DB_TLS_SHA256").is_some() {
        config.channel_binding(postgres::config::ChannelBinding::Require);
    }
    let tls = tokio_postgres_rustls::MakeRustlsConnect::new(super::tls::from_env()?);
    config.connect(tls).map_err(|error| {
        Error(if tls_failure(&error) {
            "storage_tls_failed"
        } else {
            "storage_open_failed"
        })
    })
}

fn tls_failure(error: &postgres::Error) -> bool {
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = source {
        let rustls = current.downcast_ref::<rustls::Error>().is_some()
            || current
                .downcast_ref::<std::io::Error>()
                .and_then(|io| io.get_ref())
                .is_some_and(|inner| inner.downcast_ref::<rustls::Error>().is_some());
        if rustls {
            return true;
        }
        source = current.source();
    }
    false
}

fn loopback(config: &postgres::Config) -> bool {
    use postgres::config::Host;
    let hosts = config.get_hosts().iter().all(|host| match host {
        Host::Tcp(name) => matches!(name.as_str(), "localhost" | "127.0.0.1" | "::1"),
        #[cfg(unix)]
        Host::Unix(_) => true,
    });
    hosts && config.get_hostaddrs().iter().all(|ip| ip.is_loopback())
}

/// Brings the schema to the newest known version and returns it. Refuses a database
/// already migrated by a newer binary rather than running against an unknown schema.
pub(crate) fn migrate(db: &mut Client, migrations: &[(i32, &str)]) -> Result<i32> {
    let mut tx = db
        .transaction()
        .map_err(|_| Error("storage_migration_failed"))?;
    // The lock serializes migrators before the table exists; it is released at commit or rollback.
    tx.batch_execute(&format!(
        "SELECT pg_advisory_xact_lock({MIGRATION_LOCK});
        CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now());"
    ))
    .map_err(|_| Error("storage_migration_failed"))?;
    let current: i32 = tx
        .query_one(
            "SELECT COALESCE(MAX(version),0) FROM schema_migrations",
            &[],
        )
        .and_then(|r| r.try_get(0))
        .map_err(|_| Error("storage_migration_failed"))?;
    let latest = migrations.last().map_or(0, |m| m.0);
    if current > latest {
        return Err(Error("storage_schema_newer_than_binary"));
    }
    for (version, sql) in migrations.iter().filter(|m| m.0 > current) {
        tx.batch_execute(sql)
            .map_err(|_| Error("storage_migration_failed"))?;
        tx.execute(
            "INSERT INTO schema_migrations(version) VALUES($1)",
            &[version],
        )
        .map_err(|_| Error("storage_migration_failed"))?;
    }
    tx.commit().map_err(|_| Error("storage_migration_failed"))?;
    Ok(latest)
}

const MIGRATION_LOCK: i64 = 7311946001;

/// A PostgreSQL server plus one session schema. Sessions isolate independent indexer
/// runs (console, runtime, each test) on a shared server. The URL never leaves this type.
#[derive(Clone)]
pub struct Database {
    url: String,
    schema: String,
}

impl std::fmt::Debug for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Database {{ schema: {:?}, url: <redacted> }}",
            self.schema
        )
    }
}

impl Database {
    pub fn new(url: impl Into<String>, session: &str) -> Result<Self> {
        if session.contains(['/', '.', '\\']) {
            return Err(Error("sqlite_path_no_longer_supported"));
        }
        let valid = session.len() <= 63
            && session.starts_with(|c: char| c.is_ascii_lowercase())
            && session
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            && !session.starts_with("pg_")
            && !matches!(session, "public" | "information_schema");
        if !valid {
            return Err(Error("invalid_session_name"));
        }
        Ok(Self {
            url: url.into(),
            schema: session.into(),
        })
    }

    /// The server comes only from DATABASE_URL, never from arguments.
    pub fn from_env(session: &str) -> Result<Self> {
        let url = std::env::var("DATABASE_URL").map_err(|_| Error("missing_database_url"))?;
        Self::new(url, session)
    }

    /// Readiness must not create a schema, run migrations or repair missing tables.
    pub fn health(&self) -> Result<serde_json::Value> {
        let mut client = self.connect_unmigrated()?;
        let mut tx = client
            .build_transaction()
            .read_only(true)
            .start()
            .map_err(|_| Error("storage_not_ready"))?;
        let version: i32 = tx
            .query_one(
                "SELECT COALESCE(MAX(version),0) FROM schema_migrations",
                &[],
            )
            .map_err(|_| Error("storage_not_ready"))?
            .get(0);
        if version != MIGRATIONS.last().map_or(0, |m| m.0) {
            return Err(Error("storage_not_ready"));
        }
        tx.query("SELECT transfer_id FROM transfers LIMIT 1", &[])
            .map_err(|_| Error("storage_not_ready"))?;
        tx.commit().map_err(|_| Error("storage_not_ready"))?;
        Ok(
            serde_json::json!({"status":"ready","backend":"postgresql","session":self.schema,"schema_version":version}),
        )
    }

    pub fn session(&self) -> &str {
        &self.schema
    }

    /// Connects with the session schema as the only search path, creating and
    /// migrating it first. Concurrent first connections serialize on an advisory lock.
    pub(crate) fn connect(&self) -> Result<Client> {
        let mut client = self.connect_unmigrated()?;
        let mut tx = client
            .transaction()
            .map_err(|_| Error("storage_migration_failed"))?;
        tx.batch_execute(&format!(
            "SELECT pg_advisory_xact_lock({MIGRATION_LOCK}); CREATE SCHEMA IF NOT EXISTS \"{}\";",
            self.schema
        ))
        .map_err(|_| Error("storage_migration_failed"))?;
        tx.commit().map_err(|_| Error("storage_migration_failed"))?;
        migrate(&mut client, MIGRATIONS)?;
        Ok(client)
    }

    /// A migrated raw connection for tests that inspect rows or inject failures.
    #[doc(hidden)]
    pub fn test_client(&self) -> Client {
        self.connect().unwrap()
    }

    fn connect_unmigrated(&self) -> Result<Client> {
        let mut client = connect(&self.url)?;
        // Set per connection: unqualified names must never resolve in another schema.
        client
            .batch_execute(&format!("SET search_path TO \"{}\"", self.schema))
            .map_err(|_| Error("storage_open_failed"))?;
        Ok(client)
    }
}

/// A fresh session schema on the local test server, dropped afterwards, so tests run
/// in parallel. Start the server with `docker compose up -d postgres`.
#[doc(hidden)]
pub struct TestDatabase(pub Database);

#[doc(hidden)]
impl TestDatabase {
    pub fn url() -> String {
        std::env::var("INDEXER_TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://indexer:indexer@127.0.0.1:55432/indexer".into())
    }

    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos();
        let session = format!(
            "test_{}_{}_{nanos}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let db = Database::new(Self::url(), &session).unwrap();
        connect(&db.url)
            .and_then(|mut c| {
                c.batch_execute(&format!("DROP SCHEMA IF EXISTS \"{session}\" CASCADE"))
                    .map_err(|_| Error("storage_open_failed"))
            })
            .expect("local PostgreSQL unavailable: run `docker compose up -d postgres` or set INDEXER_TEST_DATABASE_URL");
        Self(db)
    }

    /// A connection on the session schema without migrations, for schema-level tests and fault injection.
    pub fn sql(&self) -> Client {
        let mut client = connect(&self.0.url).unwrap();
        client
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS \"{0}\"; SET search_path TO \"{0}\"",
                self.0.schema
            ))
            .unwrap();
        client
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        if let Ok(mut client) = connect(&self.0.url) {
            let _ = client.batch_execute(&format!(
                "DROP SCHEMA IF EXISTS \"{}\" CASCADE",
                self.0.schema
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const V1: (i32, &str) = (1, "CREATE TABLE a(id INTEGER PRIMARY KEY);");
    const V2: (i32, &str) = (2, "CREATE TABLE b(id INTEGER PRIMARY KEY);");

    fn versions(db: &mut Client) -> Vec<i32> {
        db.query(
            "SELECT version FROM schema_migrations ORDER BY version",
            &[],
        )
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
    }

    #[test]
    fn health_is_read_only_and_never_creates_or_repairs_a_session() {
        let test = TestDatabase::new();
        assert!(test.0.health().is_err());
        let mut control = connect(&TestDatabase::url()).unwrap();
        assert!(
            !control
                .query_one(
                    "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1)",
                    &[&test.0.schema]
                )
                .unwrap()
                .get::<_, bool>(0)
        );
        test.0.connect().unwrap();
        assert_eq!(test.0.health().unwrap()["backend"], "postgresql");
        test.0
            .test_client()
            .batch_execute("DROP TABLE transfers CASCADE")
            .unwrap();
        assert!(test.0.health().is_err());
        assert!(
            test.sql()
                .query_one("SELECT to_regclass('transfers') IS NULL", &[])
                .unwrap()
                .get::<_, bool>(0)
        );
    }

    #[test]
    fn migrations_apply_once_in_order_and_resume() {
        let test = TestDatabase::new();
        let mut db = test.sql();
        assert_eq!(migrate(&mut db, &[V1]).unwrap(), 1);
        assert_eq!(migrate(&mut db, &[V1]).unwrap(), 1);
        assert_eq!(migrate(&mut db, &[V1, V2]).unwrap(), 2);
        assert_eq!(versions(&mut db), [1, 2]);
        db.execute("INSERT INTO b VALUES(1)", &[]).unwrap();
    }

    #[test]
    fn failed_migration_applies_nothing() {
        let test = TestDatabase::new();
        let mut db = test.sql();
        let broken = (
            2,
            "CREATE TABLE c(id INTEGER); SELECT missing_column FROM c;",
        );
        assert_eq!(
            migrate(&mut db, &[V1, broken]).unwrap_err().0,
            "storage_migration_failed"
        );
        assert!(
            db.query_one("SELECT to_regclass('a') IS NULL", &[])
                .unwrap()
                .get::<_, bool>(0)
        );
    }

    #[test]
    fn newer_schema_is_refused() {
        let test = TestDatabase::new();
        let mut db = test.sql();
        migrate(&mut db, &[V1, V2]).unwrap();
        assert_eq!(
            migrate(&mut db, &[V1]).unwrap_err().0,
            "storage_schema_newer_than_binary"
        );
    }

    #[test]
    fn plaintext_connections_are_refused_off_loopback() {
        for url in [
            "postgres://u:p@db.example.test/railway?sslmode=disable",
            "postgres://u:p@10.0.0.5/x?sslmode=disable",
            "host=127.0.0.1,db.example.test user=u sslmode=disable",
            "host=localhost hostaddr=192.0.2.1 user=u sslmode=disable",
        ] {
            let Err(error) = connect(url) else {
                panic!("connected without TLS");
            };
            assert_eq!(error.0, "storage_tls_required", "{url}");
        }
    }

    #[test]
    fn untrusted_or_unpinned_servers_fail_tls_without_a_plaintext_fallback() {
        // A local plain server on a non-loopback address would accept plaintext; TLS is
        // demanded instead, and the server's refusal surfaces as a TLS or open failure.
        let url = TestDatabase::url().replace("127.0.0.1", "localhost") + "?sslmode=require";
        let Err(error) = connect(&url) else {
            panic!("connected without verified TLS");
        };
        assert!(
            matches!(error.0, "storage_tls_failed" | "storage_open_failed"),
            "{}",
            error.0
        );
    }

    #[test]
    fn concurrent_first_migrations_both_succeed() {
        let test = TestDatabase::new();
        drop(test.sql());
        let url = TestDatabase::url();
        let schema = test.0.schema.clone();
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let (url, schema) = (url.clone(), schema.clone());
                std::thread::spawn(move || {
                    let mut client = connect(&url).unwrap();
                    client
                        .batch_execute(&format!("SET search_path TO \"{schema}\""))
                        .unwrap();
                    migrate(&mut client, &[V1, V2])
                })
            })
            .collect();
        for worker in workers {
            assert_eq!(worker.join().unwrap().unwrap(), 2);
        }
    }

    #[test]
    fn session_names_are_schema_safe_and_urls_stay_hidden() {
        let url = "postgres://u:hunter2@127.0.0.1/x";
        for (session, error) in [
            ("index.sqlite", "sqlite_path_no_longer_supported"),
            (
                "/app/data/runtime/index.sqlite",
                "sqlite_path_no_longer_supported",
            ),
            ("public", "invalid_session_name"),
            ("information_schema", "invalid_session_name"),
            ("pg_catalog", "invalid_session_name"),
            ("Runtime", "invalid_session_name"),
            ("x\"; DROP SCHEMA public; --", "invalid_session_name"),
            ("", "invalid_session_name"),
        ] {
            assert_eq!(
                Database::new(url, session).unwrap_err().0,
                error,
                "{session}"
            );
        }
        let db = Database::new(url, "runtime").unwrap();
        assert!(!format!("{db:?}").contains("hunter2"));
        assert_eq!(db.session(), "runtime");
    }

    #[test]
    fn sessions_are_isolated_on_one_server() {
        let (a, b) = (TestDatabase::new(), TestDatabase::new());
        a.0.connect()
            .unwrap()
            .execute("INSERT INTO live_retry_cursor VALUES(1,7)", &[])
            .unwrap();
        let count: i64 =
            b.0.connect()
                .unwrap()
                .query_one("SELECT COUNT(*) FROM live_retry_cursor", &[])
                .unwrap()
                .get(0);
        assert_eq!(count, 0);
    }

    #[test]
    fn concurrent_first_connections_to_a_new_session_all_migrate() {
        let test = TestDatabase::new();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(6));
        let workers: Vec<_> = (0..6)
            .map(|_| {
                let (db, barrier) = (test.0.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    db.connect().map(drop)
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap().unwrap();
        }
        let versions: i64 = test
            .sql()
            .query_one("SELECT COUNT(*) FROM schema_migrations", &[])
            .unwrap()
            .get(0);
        assert_eq!(versions, MIGRATIONS.len() as i64);
    }

    #[test]
    fn connection_errors_never_carry_the_url() {
        let Err(error) = connect("postgres://user:secret-password@127.0.0.1:1/x") else {
            panic!("connected to a closed port");
        };
        assert_eq!(error.0, "storage_open_failed");
        assert!(!format!("{error:?}").contains("secret-password"));
    }
}
