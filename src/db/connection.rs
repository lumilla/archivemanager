// AI WARNING: The entire DAL system (RELIC) was effectively "vibe-coded".
// As such, it is intentionally abstracted away. It is a cursed mess:
// Thus the name RELIC (Regrettably Engineered Layer for Indexed Collections)
// It is cursed, touch at your own risk.

// db/connection.rs
// Connect to Neo4j by reading creds from the environment and hand back an ArchiveDb.
// The rest of the code never needs to know about neo4rs::Graph. We hide that here.

use crate::db::ArchiveDb;
use anyhow::Result;
use neo4rs::{ConfigBuilder, Graph};

/// Connect to Neo4j using the standard environment variables:
///   NEO4J_URI      – bolt/neo4j URI
///   NEO4J_USER     – database user
///   NEO4J_PASS     – password
///   NEO4J_DATABASE – (optional) target database name
///
/// Returns an `ArchiveDb` ready for use.  Connection errors bubble up.
pub async fn connect() -> Result<ArchiveDb> {
    let uri = std::env::var("NEO4J_URI").expect("NEO4J_URI must be set");
    let user = std::env::var("NEO4J_USER").expect("NEO4J_USER must be set");
    let pass = std::env::var("NEO4J_PASS").expect("NEO4J_PASS must be set");
    let db_name = std::env::var("NEO4J_DATABASE").ok();

    let mut config = ConfigBuilder::new().uri(&uri).user(&user).password(&pass);
    if let Some(db) = db_name {
        config = config.db(db);
    }

    let graph = Graph::connect(config.build()?).await?;
    Ok(ArchiveDb::new(graph))
}
