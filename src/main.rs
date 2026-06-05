use eframe::egui;
use sea_orm::{Database, DbConn};
use sea_orm_migration::prelude::*;
use std::sync::mpsc;

// Message passed from an async database task back to the UI.
enum DbMessage {
    Opened,
    Error(String),
}

// Sanitize the archive name to be a safe filename.
// even though uppercase letters and special characters are technically allowed in filenames AFAIK
// they are a pain in the you-know-what to work with.
fn sanitize_name(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect()
}

// Will throw an error if the database does not exist and create is false.
async fn init_db(archive_name: &str, create: bool) -> anyhow::Result<DbConn> {
    let db_file = format!("{}.db", sanitize_name(archive_name));

    // If we are just opening, check if the file exists first to avoid eager creation.
    if !create && !std::path::Path::new(&db_file).exists() {
        return Err(anyhow::anyhow!(
            "Archive '{}' does not exist.",
            archive_name
        ));
    }

    // SQLite connection string. RWC = Read-Write-Create, RW = Read-Write.
    let mode = if create { "rwc" } else { "rw" };
    let db = Database::connect(format!("sqlite://{}?mode={}", db_file, mode)).await?;

    // Automatically apply database migrations.
    migration::Migrator::up(&db, None).await?;
    Ok(db)
}

struct ArchiveManagerApp {
    archive_name: String,
    archive_exists: bool,

    // Dialog visibility
    show_confirm: bool,
    show_error: bool,
    error_message: String,

    // Channel for passing results from async tasks back to the UI.
    // The sender is cloned into each spawned task; the receiver is polled every frame.
    tx: mpsc::Sender<DbMessage>,
    rx: mpsc::Receiver<DbMessage>,

    // Tokio runtime for database operations.
    // Stored here so it stays alive for the whole app session.
    rt: tokio::runtime::Runtime,
}

impl ArchiveManagerApp {
    fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create tokio runtime");

        Self {
            archive_name: String::new(),
            archive_exists: false,
            show_confirm: false,
            show_error: false,
            error_message: String::new(),
            tx,
            rx,
            rt,
        }
    }

    // Spawn a background task to open or create the archive database.
    // Sends a DbMessage back through the channel and requests a repaint when done.
    fn run_db_op(&self, create: bool, ctx: egui::Context) {
        let archive_name = self.archive_name.clone();
        let tx = self.tx.clone();
        self.rt.spawn(async move {
            match init_db(&archive_name, create).await {
                Ok(_db) => {
                    log::info!(
                        "{} archive '{}'",
                        if create { "Created" } else { "Opened" },
                        archive_name
                    );
                    tx.send(DbMessage::Opened).ok();
                }
                Err(e) => {
                    log::error!("Database operation failed: {}", e);
                    tx.send(DbMessage::Error(e.to_string())).ok();
                }
            }
            // Wake up the event loop so the message gets processed this frame.
            ctx.request_repaint();
        });
    }
}

impl eframe::App for ArchiveManagerApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // Need the context for egui::Window and for passing to async tasks.
        let ctx = ui.ctx().clone();

        // Process any results that came in from async tasks since the last frame.
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                DbMessage::Opened => {
                    self.archive_exists = true;
                    // TODO: navigate to the archive content view
                }
                DbMessage::Error(e) => {
                    self.error_message = e;
                    self.show_error = true;
                }
            }
        }

        ui.add_space(8.0);

        let response = ui.add(
            egui::TextEdit::singleline(&mut self.archive_name)
                .hint_text("Archive name...")
                .desired_width(f32::INFINITY),
        );
        if response.changed() {
            self.archive_exists =
                std::path::Path::new(&format!("{}.db", sanitize_name(&self.archive_name))).exists();
        }

        ui.add_space(4.0);

        // Copy before the closure to avoid borrow issues with self inside it.
        let archive_exists = self.archive_exists;
        ui.add_enabled_ui(!self.archive_name.is_empty(), |ui| {
            let label = if archive_exists {
                "Open Archive"
            } else {
                "Create Archive"
            };
            if ui.button(label).clicked() {
                if archive_exists {
                    self.run_db_op(false, ctx.clone());
                } else {
                    self.show_confirm = true;
                }
            }
        });

        // Confirm dialog. egui::Window is a floating panel with a title bar, draggable.
        if self.show_confirm {
            let archive_name = self.archive_name.clone();
            let mut open = true;
            egui::Window::new("Create Archive?")
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .default_pos(ctx.content_rect().center() - egui::vec2(120.0, 50.0))
                .show(&ctx, |ui| {
                    ui.label(format!("Create a new archive named '{}'?", archive_name));
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("Yes, Create It").clicked() {
                            self.show_confirm = false;
                            self.run_db_op(true, ctx.clone());
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_confirm = false;
                        }
                    });
                });
            if !open {
                self.show_confirm = false;
            }
        }

        // Error dialog.
        if self.show_error {
            let error_message = self.error_message.clone();
            let mut open = true;
            egui::Window::new("Error")
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .default_pos(ctx.content_rect().center() - egui::vec2(120.0, 40.0))
                .show(&ctx, |ui| {
                    ui.label(&error_message);
                    ui.add_space(8.0);
                    if ui.button("Close").clicked() {
                        self.show_error = false;
                    }
                });
            if !open {
                self.show_error = false;
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    // Initialize logging. At this time, the default level is info.
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init(); // "try_init" seemed cooler.

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([400.0, 120.0])
            .with_min_inner_size([300.0, 90.0])
            .with_title("Archive Manager"),
        ..Default::default()
    };

    // And run the actual app
    eframe::run_native(
        "Archive Manager",
        native_options,
        Box::new(|_cc| Ok(Box::new(ArchiveManagerApp::new()))),
    )?;

    Ok(()) // Lesson learned: whatever a function returns it does not have a semicolon for some reason.
    // I do not like it.
}
