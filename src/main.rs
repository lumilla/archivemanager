// At this point, initialize the application and set up the Slint UI.
use sea_orm::{Database, DbConn};
use sea_orm_migration::prelude::*;
use slint::ComponentHandle;

slint::include_modules!();

/// Sanitizes the archive name and initializes the SQLite database.

// Will throw an error if the database doesn't exist and create is false.
async fn init_db(archive_name: &str, create: bool) -> anyhow::Result<DbConn> {
    // Sanitize the archive name to be a safe filename.
    // even though uppercase letters and special characters are technically allowed in filenames AFAIK
    // they are a pain in the you-know-what to work with.
    let safe_name: String = archive_name
        .to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();

    let db_file = format!("{}.db", safe_name);

    // If we're just opening, check if the file exists first to avoid eager creation.
    if !create && !std::path::Path::new(&db_file).exists() {
        return Err(anyhow::anyhow!(
            "Archive '{}' does not exist.",
            archive_name
        ));
    }

    // SQLite connection string. RWC = Read-Write-Create, RW = Read-Write.
    let mode = if create { "rwc" } else { "rw" };
    let db_url = format!("sqlite://{}?mode={}", db_file, mode);
    let db: DbConn = Database::connect(&db_url).await?;

    // Automatically apply database migrations.
    migration::Migrator::up(&db, None).await?;
    Ok(db)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging. At this time, the default level is info.
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).try_init(); // "try_init" seemed cooler.

    // Load the UI (from the generated code)
    let app = AppWindow::new().map_err(|e| anyhow::anyhow!("Failed to load UI: {}", e))?;

    // Create a weak handle to the app to share with asynchronous tasks.
    let app_weak = app.as_weak();
    let app_clone = app_weak.clone();

    app.on_create_archive(move |archive_name| {
        let app_weak = app_weak.clone();
        if archive_name.is_empty() {
            log::warn!("Attempted to create an archive with an empty name. Aborted.");
            if let Some(app) = app_weak.upgrade() {
                app.set_status_text("Error: Archive name cannot be empty".into());
                app.set_is_error(true);
            }
            return;
        }

        // Spawn a background task to handle database initialization.
        tokio::spawn(async move {
            match init_db(&archive_name, true).await {
                Ok(_db) => {
                    log::info!(
                        "Database initialized successfully for archive '{}'",
                        archive_name
                    );
                    let archive_name_clone = archive_name.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(app) = app_weak.upgrade() {
                            app.set_status_text(
                                format!("Success: Created archive '{}'", archive_name_clone).into(),
                            );
                            app.set_is_error(false);
                        }
                    });
                }
                Err(e) => {
                    log::error!(
                        "Failed to initialize database for archive '{}': {}",
                        archive_name,
                        e
                    );
                    let error_msg = e.to_string();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(app) = app_weak.upgrade() {
                            app.set_status_text(format!("Error: {}", error_msg).into());
                            app.set_is_error(true);
                        }
                    });
                }
            }
        });
    });

    app.on_open_archive(move |archive_name| {
        let app_weak = app_clone.clone();
        if archive_name.is_empty() {
            log::warn!("Attempted to open an archive with an empty name. Aborted.");
            if let Some(app) = app_weak.upgrade() {
                app.set_status_text("Error: Archive name cannot be empty".into());
                app.set_is_error(true);
            }
            return;
        }

        tokio::spawn(async move {
            match init_db(&archive_name, false).await {
                Ok(_db) => {
                    log::info!(
                        "Database opened successfully for archive '{}'",
                        archive_name
                    );
                    let archive_name_clone = archive_name.clone();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(app) = app_weak.upgrade() {
                            app.set_status_text(
                                format!("Success: Opened archive '{}'", archive_name_clone).into(),
                            );
                            app.set_is_error(false);
                        }
                    });
                }
                Err(e) => {
                    log::error!(
                        "Failed to open database for archive '{}': {}",
                        archive_name,
                        e
                    );
                    let error_msg = e.to_string();
                    let _ = slint::invoke_from_event_loop(move || {
                        if let Some(app) = app_weak.upgrade() {
                            app.set_status_text(format!("Error: {}", error_msg).into());
                            app.set_is_error(true);
                        }
                    });
                }
            }
        });
    });

    // And run the actual app
    app.run()
        .map_err(|e| anyhow::anyhow!("Failed to run the application: {}", e))?;

    Ok(()) // Lesson learned: whatever a function returns it does not have a semicolon for some reason.
    // I do not like it.
}
