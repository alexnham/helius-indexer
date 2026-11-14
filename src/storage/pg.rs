//! PostgreSQL connection and versioned schema migrations.
use crate::{Error, Result};
use postgres::{Client, NoTls};

/// Applied in order; never edit a released entry, append a new version instead.
pub(crate) const MIGRATIONS: &[(i32, &str)] = &[];

/// Connection errors are reduced to a class: driver messages can echo the URL or credentials.
pub(crate) fn connect(url: &str) -> Result<Client> {
    // shortcut: no TLS, so only local databases work; add TLS before the runtime uses a hosted database (#35).
    Client::connect(url, NoTls).map_err(|_| Error("storage_open_failed"))
}

/// Brings the schema to the newest known version and returns it. Refuses a database
/// already migrated by a newer binary rather than running against an unknown schema.
pub(crate) fn migrate(db: &mut Client, migrations: &[(i32, &str)]) -> Result<i32> {
    let mut tx = db
        .transaction()
        .map_err(|_| Error("storage_migration_failed"))?;
    tx.batch_execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations(version INTEGER PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now());
        LOCK TABLE schema_migrations IN EXCLUSIVE MODE;",
    )
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

/// A connection confined to a fresh schema that is dropped afterwards, so tests can run
/// in parallel against one local server. Start it with `docker compose --profile db up -d postgres`.
#[cfg(test)]
pub(crate) struct TestDb {
    pub client: Client,
    schema: String,
}

#[cfg(test)]
impl TestDb {
    pub(crate) fn new() -> Self {
        use std::sync::atomic::{AtomicU32, Ordering};
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let url = std::env::var("INDEXER_TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://indexer:indexer@127.0.0.1:55432/indexer".into());
        let mut client = connect(&url).expect(
            "local PostgreSQL unavailable: run `docker compose --profile db up -d postgres` or set INDEXER_TEST_DATABASE_URL",
        );
        let schema = format!(
            "test_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        client
            .batch_execute(&format!(
                "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}; SET search_path TO {schema};"
            ))
            .unwrap();
        Self { client, schema }
    }
}

#[cfg(test)]
impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = self
            .client
            .batch_execute(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema));
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
    fn migrations_apply_once_in_order_and_resume() {
        let mut db = TestDb::new();
        assert_eq!(migrate(&mut db.client, &[V1]).unwrap(), 1);
        assert_eq!(migrate(&mut db.client, &[V1]).unwrap(), 1);
        assert_eq!(migrate(&mut db.client, &[V1, V2]).unwrap(), 2);
        assert_eq!(versions(&mut db.client), [1, 2]);
        db.client.execute("INSERT INTO b VALUES(1)", &[]).unwrap();
    }

    #[test]
    fn failed_migration_applies_nothing() {
        let mut db = TestDb::new();
        let broken = (
            2,
            "CREATE TABLE c(id INTEGER); SELECT missing_column FROM c;",
        );
        assert_eq!(
            migrate(&mut db.client, &[V1, broken]).unwrap_err().0,
            "storage_migration_failed"
        );
        assert!(
            db.client
                .query_one("SELECT to_regclass('a') IS NULL", &[])
                .unwrap()
                .get::<_, bool>(0)
        );
    }

    #[test]
    fn newer_schema_is_refused() {
        let mut db = TestDb::new();
        migrate(&mut db.client, &[V1, V2]).unwrap();
        assert_eq!(
            migrate(&mut db.client, &[V1]).unwrap_err().0,
            "storage_schema_newer_than_binary"
        );
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
