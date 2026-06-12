use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Clone, Debug, PartialEq)]
#[serde(tag = "type")]
pub enum UiTypeDef {
    Text,
    Dropdown { options: Vec<String> },
    Unit { name: String, options: Vec<String> },
}

impl Default for UiTypeDef {
    fn default() -> Self {
        UiTypeDef::Text
    }
}

#[derive(Deserialize, Clone, Debug, PartialEq)]
pub struct RelationConfig {
    pub rel_type: String,
    pub target_label: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct FeatureMetadata {
    pub id: String,
    pub label: String,
    #[serde(default)]
    pub ui_type: UiTypeDef,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub relation: Option<RelationConfig>,
    #[serde(default)]
    pub system_title: Option<bool>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Schema {
    pub features: Vec<FeatureMetadata>,
}

impl Schema {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("Failed to read schema.toml: {}", e))?;
        let schema: Schema = toml::from_str(&s)?;
        schema.validate()?;
        Ok(schema)
    }

    pub fn get_title_field_id(&self) -> String {
        self.features
            .iter()
            .find(|f| f.system_title.unwrap_or(false))
            .map(|f| f.id.clone())
            .unwrap_or_else(|| "title".to_string())
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let mut ids = std::collections::HashSet::new();
        let mut title_flags_count = 0;
        for feat in &self.features {
            if feat.id.is_empty() {
                return Err(anyhow::anyhow!("Schema contains a feature with an empty ID!"));
            }
            if !ids.insert(feat.id.clone()) {
                return Err(anyhow::anyhow!("Schema contains duplicate feature ID: '{}'!", feat.id));
            }
            if feat.system_title.unwrap_or(false) {
                title_flags_count += 1;
            }
        }

        if title_flags_count == 0 {
            return Err(anyhow::anyhow!("Schema is missing a required feature marked with system_title = true!"));
        }

        if title_flags_count > 1 {
            return Err(anyhow::anyhow!("Schema is invalid: multiple features are marked with system_title = true! Only one is allowed."));
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DynamicModel {
    pub id: String,
    pub is_archived: bool,
    pub fields: HashMap<String, String>,
}

impl DynamicModel {

    pub fn from_row(row: &neo4rs::Row, node: neo4rs::Node, schema: &Schema) -> Self {
        let mut fields = HashMap::new();
        for feat in &schema.features {
            if feat.relation.is_some() {
                let val: Option<String> = row.get(&feat.id).unwrap_or(None);
                fields.insert(feat.id.clone(), val.unwrap_or_default());
            } else {
                let val: Option<String> = node.get(&feat.id).unwrap_or(None);
                fields.insert(feat.id.clone(), val.unwrap_or_default());
            }
        }

        Self {
            id: node.id().to_string(),
            is_archived: node.get("is_archived").unwrap_or(false),
            fields,
        }
    }

    pub fn get_field(&self, id: &str) -> String {
        self.fields.get(id).cloned().unwrap_or_default()
    }

    /// Builds the full Cypher update query for this model.
    /// Called exclusively by `db::queries` – not meant for direct use elsewhere.
    pub fn build_update_query(&self, archive_name: &str, schema: &Schema) -> neo4rs::Query {
        let mut set_clauses = vec!["n.is_archived = $is_archived".to_string()];
        for feat in &schema.features {
            if feat.relation.is_none() {
                set_clauses.push(format!("n.{} = ${}", feat.id, feat.id));
            }
        }

        let mut cypher_parts = vec![format!(
            "MATCH (n:{}) WHERE id(n) = $id SET {}",
            archive_name,
            set_clauses.join(", ")
        )];

        for (i, feat) in schema.features.iter().enumerate() {
            if let Some(rel) = &feat.relation {
                cypher_parts.push("WITH n".to_string());
                cypher_parts.push(format!(
                    "OPTIONAL MATCH (n)-[r{} : {}]->() DELETE r{}",
                    i, rel.rel_type, i
                ));
                cypher_parts.push("WITH n".to_string());
                cypher_parts.push(format!(
                    "FOREACH (_ IN CASE WHEN ${} <> \"\" THEN [1] ELSE [] END | MERGE (m{}:{} {{name: ${}}}) MERGE (n)-[:{}]->(m{}))",
                    feat.id, i, rel.target_label, feat.id, rel.rel_type, i
                ));
            }
        }

        let cypher = cypher_parts.join("\n");

        let mut q = neo4rs::query(&cypher)
            .param("id", self.id.parse::<i64>().unwrap_or_default())
            .param("is_archived", self.is_archived);

        for (k, v) in &self.fields {
            q = q.param(k.as_str(), v.clone());
        }

        q
    }
}

/// Split a stored value into (value, unit) using the known unit options list.
pub fn split_unit(val: &str, options: &[String]) -> (String, String) {
    let mut sorted = options.to_vec();
    sorted.sort_by_key(|o| std::cmp::Reverse(o.len()));
    for opt in sorted {
        if val.ends_with(&opt) {
            let prefix = &val[..val.len() - opt.len()];
            return (prefix.to_string(), opt);
        }
    }
    (val.to_string(), String::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_unit() {
        let options = vec!["cm".to_string(), "m".to_string(), "mm".to_string(), "inch".to_string()];
        
        assert_eq!(split_unit("30cm", &options), ("30".to_string(), "cm".to_string()));
        assert_eq!(split_unit("1.5m", &options), ("1.5".to_string(), "m".to_string()));
        assert_eq!(split_unit("45", &options), ("45".to_string(), "".to_string()));
        assert_eq!(split_unit("10inch", &options), ("10".to_string(), "inch".to_string()));
        assert_eq!(split_unit("cm", &options), ("".to_string(), "cm".to_string()));
    }

    #[test]
    fn test_schema_validation_success() {
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
                    id: "description".to_string(),
                    label: "Description".to_string(),
                    ui_type: UiTypeDef::Text,
                    required: false,
                    relation: None,
                    system_title: None,
                },
            ],
        };
        assert!(schema.validate().is_ok());
        assert_eq!(schema.get_title_field_id(), "title");
    }

    #[test]
    fn test_schema_validation_missing_system_title() {
        let schema = Schema {
            features: vec![
                FeatureMetadata {
                    id: "title".to_string(),
                    label: "Title".to_string(),
                    ui_type: UiTypeDef::Text,
                    required: true,
                    relation: None,
                    system_title: Some(false),
                },
            ],
        };
        let err = schema.validate().unwrap_err();
        assert!(err.to_string().contains("missing a required feature marked with system_title = true"));
    }

    #[test]
    fn test_schema_validation_multiple_system_titles() {
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
                    id: "name".to_string(),
                    label: "Name".to_string(),
                    ui_type: UiTypeDef::Text,
                    required: true,
                    relation: None,
                    system_title: Some(true),
                },
            ],
        };
        let err = schema.validate().unwrap_err();
        assert!(err.to_string().contains("multiple features are marked with system_title = true"));
    }

    #[test]
    fn test_schema_validation_duplicate_ids() {
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
                    id: "title".to_string(),
                    label: "Alternate Title".to_string(),
                    ui_type: UiTypeDef::Text,
                    required: false,
                    relation: None,
                    system_title: None,
                },
            ],
        };
        let err = schema.validate().unwrap_err();
        assert!(err.to_string().contains("duplicate feature ID: 'title'"));
    }

    #[test]
    fn test_schema_validation_empty_id() {
        let schema = Schema {
            features: vec![
                FeatureMetadata {
                    id: "".to_string(),
                    label: "Title".to_string(),
                    ui_type: UiTypeDef::Text,
                    required: true,
                    relation: None,
                    system_title: Some(true),
                },
            ],
        };
        let err = schema.validate().unwrap_err();
        assert!(err.to_string().contains("feature with an empty ID"));
    }

    #[test]
    fn test_build_update_query() {
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

        let mut fields = HashMap::new();
        fields.insert("title".to_string(), "Sword".to_string());
        fields.insert("dimensions".to_string(), "90cm".to_string());
        fields.insert("material".to_string(), "Metal".to_string());

        let model = DynamicModel {
            id: "12345".to_string(),
            is_archived: false,
            fields,
        };

        let query = model.build_update_query("TestArchive", &schema);
        
        // Assert parameters are set correctly
        assert!(query.has_param_key("id"));
        assert!(query.has_param_key("is_archived"));
        assert!(query.has_param_key("title"));
        assert!(query.has_param_key("dimensions"));
        assert!(query.has_param_key("material"));
    }
}

