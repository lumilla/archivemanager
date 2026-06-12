// AI WARNING: The entire DAL system (RELIC) was effectively "vibe-coded".
// As such, it is intentionally abstracted away. It is a cursed mess:
// Thus the name RELIC (Regrettably Engineered Layer for Indexed Collections)
// It is cursed, touch at your own risk.

// This is where all the Cypher query logic lives. Private to the db module.
// Every function gets a plain &Graph and domain types, returns domain types only.
// Zero neo4rs types escape this file.

use crate::db::ArchiveStatus;
use crate::entities::artifacts::{DynamicModel, Schema, UiTypeDef};
use anyhow::{Context, Result};
use neo4rs::{Graph, Node, query};
use serde::Deserialize;
use std::collections::HashMap;

// ── Import/export data file format ────────────────────────────────────────────

/// Deserialization target for `.toml` data files (demo_data.toml or any user
/// export).  Each `[[records]]` entry is a free-form field-id → value map.
#[derive(Deserialize)]
struct DataFile {
    #[serde(default)]
    records: Vec<HashMap<String, String>>,
}

// ── Archive status ────────────────────────────────────────────────────────────

pub async fn get_archive_status(graph: &Graph, archive_name: &str) -> Result<ArchiveStatus> {
    let cypher_count = format!(
        "MATCH (n:Artifact:{archive}) RETURN count(n) AS count LIMIT 1",
        archive = archive_name
    );
    let mut res = graph.execute(query(&cypher_count)).await?;
    let count: i64 = match res.next().await? {
        Some(r) => r.get("count").unwrap_or(0),
        None => 0,
    };

    if count == 0 {
        return Ok(ArchiveStatus {
            exists: false,
            property_fields: Vec::new(),
        });
    }

    let cypher_keys = format!(
        "MATCH (n:Artifact:{archive}) UNWIND keys(n) AS key RETURN DISTINCT key",
        archive = archive_name
    );
    let mut keys_res = graph.execute(query(&cypher_keys)).await?;
    let mut property_fields = Vec::new();
    while let Some(row) = keys_res.next().await? {
        if let Ok(key) = row.get::<String>("key") {
            if key != "is_archived" {
                property_fields.push(key);
            }
        }
    }

    Ok(ArchiveStatus {
        exists: true,
        property_fields,
    })
}

// ── Schema migration ──────────────────────────────────────────────────────────

pub async fn run_migration(
    graph: &Graph,
    archive_name: &str,
    fields_to_add: Vec<String>,
    fields_to_remove: Vec<String>,
) -> Result<()> {
    if !fields_to_remove.is_empty() {
        let remove_clauses: Vec<String> =
            fields_to_remove.iter().map(|f| format!("n.{f}")).collect();
        let cypher = format!(
            "MATCH (n:Artifact:{archive}) REMOVE {clauses}",
            archive = archive_name,
            clauses = remove_clauses.join(", ")
        );
        graph.run(query(&cypher)).await?;
    }

    if !fields_to_add.is_empty() {
        let set_clauses: Vec<String> = fields_to_add
            .iter()
            .map(|f| format!("n.{f} = \"\""))
            .collect();
        let cypher = format!(
            "MATCH (n:Artifact:{archive}) SET {clauses}",
            archive = archive_name,
            clauses = set_clauses.join(", ")
        );
        graph.run(query(&cypher)).await?;
    }

    Ok(())
}

// ── List / paginate ───────────────────────────────────────────────────────────

pub async fn list_artifacts(
    graph: &Graph,
    archive_name: &str,
    schema: &Schema,
    skip: usize,
    page_size: usize,
    search: &str,
) -> Result<Vec<DynamicModel>> {
    let title_field = schema.get_title_field_id();
    let mut cypher = format!("MATCH (n:Artifact:{archive_name})");
    if !search.is_empty() {
        cypher.push_str(&format!(" WHERE n.{title_field} CONTAINS $search"));
    }

    let mut return_clauses = vec!["n".to_string()];
    for (i, feat) in schema.features.iter().enumerate() {
        if let Some(rel) = &feat.relation {
            cypher.push_str(&format!(
                "\nOPTIONAL MATCH (n)-[:{}]->(r{}:{})",
                rel.rel_type, i, rel.target_label
            ));
            return_clauses.push(format!("r{i}.name AS {id}", id = feat.id));
        }
    }

    cypher.push_str(&format!(
        "\nRETURN {cols} ORDER BY id(n) SKIP $skip LIMIT $limit",
        cols = return_clauses.join(", ")
    ));

    let q = query(&cypher)
        .param("search", search)
        .param("skip", skip as i64)
        .param("limit", page_size as i64);

    let mut result = graph.execute(q).await?;
    let mut models = Vec::new();
    while let Ok(Some(row)) = result.next().await {
        let node: Node = row.get("n")?;
        models.push(DynamicModel::from_row(&row, node, schema));
    }

    Ok(models)
}

// ── Create single artifact ────────────────────────────────────────────────────

pub async fn create_artifact(
    graph: &Graph,
    archive_name: &str,
    fields: HashMap<String, String>,
    schema: &Schema,
) -> Result<DynamicModel> {
    // Build SET for non-relation fields.
    let mut set_parts = vec!["n.is_archived = false".to_string()];
    for feat in &schema.features {
        if feat.relation.is_none() {
            set_parts.push(format!("n.{id} = ${id}", id = feat.id));
        }
    }

    // RETURN n so we can hand back the newly minted model (with its DB id).
    let cypher = format!(
        "CREATE (n:Artifact:{archive_name}) SET {sets} RETURN n",
        sets = set_parts.join(", ")
    );

    let mut q = query(&cypher);
    for feat in &schema.features {
        if feat.relation.is_none() {
            q = q.param(
                feat.id.as_str(),
                fields.get(&feat.id).cloned().unwrap_or_default(),
            );
        }
    }

    let mut result = graph.execute(q).await?;
    let row = result.next().await?.context("CREATE returned no rows")?;
    let node: Node = row.get("n")?;

    // Build the returned model with known fields; relations start empty.
    let mut model_fields = HashMap::new();
    for feat in &schema.features {
        model_fields.insert(
            feat.id.clone(),
            fields.get(&feat.id).cloned().unwrap_or_default(),
        );
    }

    // Now resolve any relation fields via MERGE.
    let model = DynamicModel {
        id: node.id().to_string(),
        is_archived: false,
        fields: model_fields,
    };
    update_artifact(graph, archive_name, &model, schema).await?;

    Ok(model)
}

// ── Update single artifact ────────────────────────────────────────────────────

pub async fn update_artifact(
    graph: &Graph,
    archive_name: &str,
    model: &DynamicModel,
    schema: &Schema,
) -> Result<()> {
    let q = model.build_update_query(archive_name, schema);
    graph.run(q).await?;
    Ok(())
}

// ── Delete single artifact ────────────────────────────────────────────────────

pub async fn delete_artifact(graph: &Graph, archive_name: &str, id: &str) -> Result<()> {
    let id_int: i64 = id
        .parse()
        .with_context(|| format!("Invalid artifact id: {id}"))?;
    // DETACH DELETE removes the node and all its edges.
    let cypher = format!("MATCH (n:Artifact:{archive_name}) WHERE id(n) = $id DETACH DELETE n");
    graph.run(query(&cypher).param("id", id_int)).await?;
    Ok(())
}

// ── Import from TOML file ─────────────────────────────────────────────────────

/// Read a TOML data file and insert each `[[records]]` entry as a new artifact.
/// Fields not present in the schema are ignored.  Missing schema fields default
/// to an empty string.
///
/// Unit fields: if a schema field has `UiTypeDef::Unit`, the data file may
/// supply a companion `<field_id>_unit` key; the two are concatenated before
/// storage (e.g. `dimensions = "30"`, `dimensions_unit = "cm"` → `"30cm"`).
///
/// Returns the number of records successfully imported.
pub async fn import_from_file(
    graph: &Graph,
    archive_name: &str,
    path: &str,
    schema: &Schema,
) -> Result<usize> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("Cannot read data file '{path}'"))?;
    let file: DataFile =
        toml::from_str(&raw).with_context(|| format!("Invalid TOML in '{path}'"))?;

    let count = file.records.len();
    for record in file.records {
        // Map record keys → schema fields, assembling the final field values.
        let mut resolved: HashMap<String, String> = HashMap::new();
        for feat in &schema.features {
            let raw_val = record.get(&feat.id).cloned().unwrap_or_default();
            let val = match &feat.ui_type {
                UiTypeDef::Unit { .. } => {
                    // Combine numeric value + unit suffix if supplied separately.
                    let unit_key = format!("{}_unit", feat.id);
                    let unit = record.get(&unit_key).cloned().unwrap_or_default();
                    format!("{raw_val}{unit}")
                }
                _ => raw_val,
            };
            resolved.insert(feat.id.clone(), val);
        }
        create_artifact(graph, archive_name, resolved, schema).await?;
    }

    log::info!("Imported {count} records into '{archive_name}' from '{path}'.");
    Ok(count)
}

// ── Export to TOML file ───────────────────────────────────────────────────────

/// Dump every artifact in the archive to a TOML file that `import_from_file`
/// can read back verbatim.  Unit fields are split back into value + `_unit`
/// pairs so they round-trip correctly.
///
/// Returns the number of records written.
pub async fn export_to_file(
    graph: &Graph,
    archive_name: &str,
    path: &str,
    schema: &Schema,
) -> Result<usize> {
    // Fetch everything (no pagination – this is a full dump).
    let mut all: Vec<DynamicModel> = Vec::new();
    let mut skip = 0usize;
    let page = 500usize;
    loop {
        let batch = list_artifacts(graph, archive_name, schema, skip, page, "").await?;
        let done = batch.len() < page;
        all.extend(batch);
        if done {
            break;
        }
        skip += page;
    }

    let count = all.len();
    let mut out =
        String::from("# Exported by Archive Manager – re-importable with File > Import\n\n");

    for model in &all {
        out.push_str("[[records]]\n");
        for feat in &schema.features {
            let stored = model.get_field(&feat.id);
            match &feat.ui_type {
                UiTypeDef::Unit { options, .. } => {
                    // Split stored "30cm" → value = "30", unit = "cm".
                    let (val, unit) = crate::entities::artifacts::split_unit(&stored, options);
                    out.push_str(&toml_kv(&feat.id, &val));
                    if !unit.is_empty() {
                        out.push_str(&toml_kv(&format!("{}_unit", feat.id), &unit));
                    }
                }
                _ => {
                    out.push_str(&toml_kv(&feat.id, &stored));
                }
            }
        }
        out.push('\n');
    }

    std::fs::write(path, &out).with_context(|| format!("Cannot write export file '{path}'"))?;

    log::info!("Exported {count} records from '{archive_name}' to '{path}'.");
    Ok(count)
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Format a single TOML key = "value" line (escaping embedded quotes).
fn toml_kv(key: &str, value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("{key} = \"{escaped}\"\n")
}
