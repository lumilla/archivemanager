// this is based off of what sea-orm-cli gave
use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Artifacts::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Artifacts::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Artifacts::Title).string().not_null())
                    .col(ColumnDef::new(Artifacts::Dimensions).string())
                    .col(ColumnDef::new(Artifacts::IsArchived).integer().default(0))
                    .to_owned(),
            )
            .await
    }

    /// Drop the `artifacts` table (used for rolling back migrations).
    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Artifacts::Table).to_owned())
            .await
    }
}

// Enforcing database column names as type-safe Rust Enums
#[derive(DeriveIden)]
enum Artifacts {
    Table,
    Id,
    Title,
    Dimensions,
    IsArchived,
}
