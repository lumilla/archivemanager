// AI WARNING: The entire DAL system (RELIC) was effectively "vibe-coded".
// As such, it is intentionally abstracted away. It is a cursed mess:
// Thus the name RELIC (Regrettably Engineered Layer for Indexed Collections)
// It is cursed, touch at your own risk.

// db/mod.rs
//
// This is the ONLY place the rest of the app imports DB stuff from.
// No Cypher. No neo4rs types. No graph topology leaks past this boundary.
//
// To add a new DB operation:
//   1. Implement the async fn in queries.rs
//   2. Add a one-liner delegate here
//   3. Done -- main.rs never needs to change for DB concerns.

mod connection;
mod queries;

use crate::entities::artifacts::{DynamicModel, Schema};
use anyhow::Result;
use neo4rs::Graph;
use std::collections::HashMap;

pub use connection::connect;

// ── Handle ────────────────────────────────────────────────────────────────────

/// Live handle to an open Neo4j connection.
/// Clone it freely – the underlying pool is reference-counted.
#[derive(Clone)]
pub struct ArchiveDb {
    graph: Graph,
}

// ── Supporting types ──────────────────────────────────────────────────────────

/// What we know about a named archive before deciding how to open it.
pub struct ArchiveStatus {
    /// `false` means the archive is empty / does not yet exist.
    pub exists: bool,
    /// Property keys found on existing nodes (internal fields excluded).
    pub property_fields: Vec<String>,
}

/// Summary returned after a bulk operation.
pub struct BulkResult {
    pub records_affected: usize,
}

// ── API ───────────────────────────────────────────────────────────────────────

impl ArchiveDb {
    pub(crate) fn new(graph: Graph) -> Self {
        Self { graph }
    }

    // ── Archive lifecycle ───────────────────────────────────────────────────

    /// Probe the DB to find out whether `archive_name` already has data and,
    /// if so, which property keys are stored on its nodes.
    pub async fn get_archive_status(&self, archive_name: &str) -> Result<ArchiveStatus> {
        queries::get_archive_status(&self.graph, archive_name).await
    }

    /// Remove `fields_to_remove` properties from every node in the archive and
    /// initialise `fields_to_add` to an empty string on every node.
    pub async fn migrate(
        &self,
        archive_name: &str,
        fields_to_add: Vec<String>,
        fields_to_remove: Vec<String>,
    ) -> Result<()> {
        queries::run_migration(&self.graph, archive_name, fields_to_add, fields_to_remove).await
    }

    // ── Reading ─────────────────────────────────────────────────────────────

    /// Fetch one page of artifacts starting at `skip`, optionally filtered by
    /// the schema's title field.  Returns at most `page_size` items.
    pub async fn list_artifacts(
        &self,
        archive_name: &str,
        schema: &Schema,
        skip: usize,
        page_size: usize,
        search: &str,
    ) -> Result<Vec<DynamicModel>> {
        queries::list_artifacts(&self.graph, archive_name, schema, skip, page_size, search).await
    }

    // ── Writing ─────────────────────────────────────────────────────────────

    /// Create a brand-new artifact with the given field values.
    /// Returns the fully populated model (including the new DB id).
    #[allow(dead_code)]
    pub async fn create_artifact(
        &self,
        archive_name: &str,
        fields: HashMap<String, String>,
        schema: &Schema,
    ) -> Result<DynamicModel> {
        queries::create_artifact(&self.graph, archive_name, fields, schema).await
    }

    /// Persist all field edits on a single artifact back to the DB.
    pub async fn update_artifact(
        &self,
        archive_name: &str,
        model: &DynamicModel,
        schema: &Schema,
    ) -> Result<()> {
        queries::update_artifact(&self.graph, archive_name, model, schema).await
    }

    /// Permanently delete an artifact (and all its graph edges).
    #[allow(dead_code)]
    pub async fn delete_artifact(&self, archive_name: &str, id: &str) -> Result<()> {
        queries::delete_artifact(&self.graph, archive_name, id).await
    }

    // ── Bulk import / export ────────────────────────────────────────────────

    /// Import every `[[records]]` entry from a TOML data file into the archive.
    ///
    /// - Fields not present in the schema are silently ignored.
    /// - `Unit` fields may supply a companion `<field_id>_unit` key; the two
    ///   are concatenated before storage (`dimensions = "30"` +
    ///   `dimensions_unit = "cm"` → `"30cm"`).
    /// - Missing schema fields default to an empty string.
    ///
    /// Returns a [`BulkResult`] with the count of imported records.
    pub async fn import_from_file(
        &self,
        archive_name: &str,
        path: &str,
        schema: &Schema,
    ) -> Result<BulkResult> {
        let n = queries::import_from_file(&self.graph, archive_name, path, schema).await?;
        Ok(BulkResult {
            records_affected: n,
        })
    }

    /// Export every artifact in the archive to a TOML file that
    /// `import_from_file` can read back verbatim.
    ///
    /// `Unit` fields are split back into value + `_unit` pairs so they
    /// round-trip correctly through import → export → import.
    ///
    /// Returns a [`BulkResult`] with the count of exported records.
    #[allow(dead_code)]
    pub async fn export_to_file(
        &self,
        archive_name: &str,
        path: &str,
        schema: &Schema,
    ) -> Result<BulkResult> {
        let n = queries::export_to_file(&self.graph, archive_name, path, schema).await?;
        Ok(BulkResult {
            records_affected: n,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entities::artifacts::{FeatureMetadata, RelationConfig, UiTypeDef};
    use std::collections::HashMap;

    async fn get_test_db() -> Option<ArchiveDb> {
        let _ = dotenvy::dotenv();
        if std::env::var("NEO4J_URI").is_err() {
            println!("NEO4J_URI is not set; skipping integration test.");
            return None;
        }
        match connect().await {
            Ok(db) => Some(db),
            Err(e) => {
                println!(
                    "Failed to connect to Neo4j: {}. Skipping integration test.",
                    e
                );
                None
            }
        }
    }

    #[tokio::test]
    async fn test_db_operations_flow() {
        let db = match get_test_db().await {
            Some(db) => db,
            None => return,
        };

        let archive_name = "TestArchiveDbIntegration";

        // Clean up before starting
        let clean_query = neo4rs::query(&format!("MATCH (n:{}) DETACH DELETE n", archive_name));
        let _ = db.graph.run(clean_query.clone()).await;

        let schema = Schema {
            features: vec![
                FeatureMetadata {
                    id: "title".to_string(),
                    label: "Title".to_string(),
                    ui_type: UiTypeDef::Text,
                    required: true,
                    relation: None,
                    system_title: Some(true),
                },
                FeatureMetadata {
                    id: "dimensions".to_string(),
                    label: "Dimensions".to_string(),
                    ui_type: UiTypeDef::Unit {
                        name: "Length".to_string(),
                        options: vec!["cm".to_string(), "m".to_string()],
                    },
                    required: false,
                    relation: None,
                    system_title: None,
                },
                FeatureMetadata {
                    id: "material".to_string(),
                    label: "Material".to_string(),
                    ui_type: UiTypeDef::Dropdown {
                        options: vec!["Stone".to_string(), "Metal".to_string()],
                    },
                    required: false,
                    relation: Some(RelationConfig {
                        rel_type: "HAS_MATERIAL".to_string(),
                        target_label: "Material".to_string(),
                    }),
                    system_title: None,
                },
            ],
        };

        // 1. Get status of non-existent/empty archive
        let status = db.get_archive_status(archive_name).await.unwrap();
        assert!(!status.exists);

        // 2. Create artifact
        let mut fields = HashMap::new();
        fields.insert("title".to_string(), "Roman Shield".to_string());
        fields.insert("dimensions".to_string(), "100cm".to_string());
        fields.insert("material".to_string(), "Stone".to_string());

        let created = db
            .create_artifact(archive_name, fields, &schema)
            .await
            .unwrap();
        assert!(!created.id.is_empty());
        assert_eq!(created.get_field("title"), "Roman Shield");
        assert_eq!(created.get_field("dimensions"), "100cm");
        assert_eq!(created.get_field("material"), "Stone");

        // 3. Status should now reflect exists = true
        let status = db.get_archive_status(archive_name).await.unwrap();
        assert!(status.exists);
        assert!(status.property_fields.contains(&"title".to_string()));

        // 4. List artifacts
        let list = db
            .list_artifacts(archive_name, &schema, 0, 10, "")
            .await
            .unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, created.id);
        assert_eq!(list[0].get_field("title"), "Roman Shield");

        // 5. List with search query
        let search_list = db
            .list_artifacts(archive_name, &schema, 0, 10, "Shield")
            .await
            .unwrap();
        assert_eq!(search_list.len(), 1);
        let search_empty = db
            .list_artifacts(archive_name, &schema, 0, 10, "Sword")
            .await
            .unwrap();
        assert_eq!(search_empty.len(), 0);

        // 6. Update artifact (change title and change relationship)
        let mut updated_fields = created.fields.clone();
        updated_fields.insert("title".to_string(), "Viking Shield".to_string());
        updated_fields.insert("material".to_string(), "Metal".to_string());
        let updated_model = DynamicModel {
            id: created.id.clone(),
            is_archived: false,
            fields: updated_fields,
        };

        db.update_artifact(archive_name, &updated_model, &schema)
            .await
            .unwrap();

        // Retrieve and check updates
        let list_after_update = db
            .list_artifacts(archive_name, &schema, 0, 10, "")
            .await
            .unwrap();
        assert_eq!(list_after_update.len(), 1);
        assert_eq!(list_after_update[0].get_field("title"), "Viking Shield");
        assert_eq!(list_after_update[0].get_field("material"), "Metal");

        // 7. Migration test (add a new field, remove an old one)
        db.migrate(
            archive_name,
            vec!["new_field".to_string()],
            vec!["dimensions".to_string()],
        )
        .await
        .unwrap();
        let status_migrated = db.get_archive_status(archive_name).await.unwrap();
        assert!(
            status_migrated
                .property_fields
                .contains(&"new_field".to_string())
        );
        assert!(
            !status_migrated
                .property_fields
                .contains(&"dimensions".to_string())
        );

        // 8. Import / Export
        let temp_file = "temp_integration_test_export.toml";

        let schema_export = Schema {
            features: vec![
                FeatureMetadata {
                    id: "title".to_string(),
                    label: "Title".to_string(),
                    ui_type: UiTypeDef::Text,
                    required: true,
                    relation: None,
                    system_title: Some(true),
                },
                FeatureMetadata {
                    id: "new_field".to_string(),
                    label: "New Field".to_string(),
                    ui_type: UiTypeDef::Text,
                    required: false,
                    relation: None,
                    system_title: None,
                },
                FeatureMetadata {
                    id: "material".to_string(),
                    label: "Material".to_string(),
                    ui_type: UiTypeDef::Dropdown {
                        options: vec!["Stone".to_string(), "Metal".to_string()],
                    },
                    required: false,
                    relation: Some(RelationConfig {
                        rel_type: "HAS_MATERIAL".to_string(),
                        target_label: "Material".to_string(),
                    }),
                    system_title: None,
                },
            ],
        };

        let export_res = db
            .export_to_file(archive_name, temp_file, &schema_export)
            .await
            .unwrap();
        assert_eq!(export_res.records_affected, 1);

        // Delete the node first, then import
        db.delete_artifact(archive_name, &created.id).await.unwrap();
        let list_empty = db
            .list_artifacts(archive_name, &schema_export, 0, 10, "")
            .await
            .unwrap();
        assert_eq!(list_empty.len(), 0);

        let import_res = db
            .import_from_file(archive_name, temp_file, &schema_export)
            .await
            .unwrap();
        assert_eq!(import_res.records_affected, 1);

        let list_imported = db
            .list_artifacts(archive_name, &schema_export, 0, 10, "")
            .await
            .unwrap();
        assert_eq!(list_imported.len(), 1);
        assert_eq!(list_imported[0].get_field("title"), "Viking Shield");
        assert_eq!(list_imported[0].get_field("material"), "Metal");

        // Clean up temp file
        let _ = std::fs::remove_file(temp_file);

        // Clean up database nodes
        let _ = db.graph.run(clean_query).await;
    }
}
