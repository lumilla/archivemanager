mod entities;
mod error_propagator;

use eframe::egui;
use egui_infinite_scroll::InfiniteScroll;
use entities::artifacts;
use error_propagator::ErrorPropagator;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Database, DbConn, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect,
};
use sea_orm_migration::prelude::*;
use std::sync::mpsc;

const PAGE_SIZE: usize = 25;

// One row as fetched from the DB, kept small since it gets cloned a lot by the scroll list.
#[derive(Debug, Clone)]
struct ArtifactRow {
    id: i64,
    title: String,
    dimensions: Option<String>,
}

// Message passed from an async database task back to the UI.
enum DbMessage {
    Opened(DbConn),
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

// Seed some demo items so a freshly created archive has something to show.
// Does nothing if the archive already has data.
async fn seed_demo_data(db: &DbConn) -> anyhow::Result<()> {
    if artifacts::Entity::find().count(db).await? > 0 {
        return Ok(());
    }

    let demo: &[(&str, Option<&str>)] = &[
        // demo data courtesy of Google Gemini.
        ("Roman Amphora", Some("30x15cm")),
        ("Bronze Age Sword", Some("75x8cm")),
        ("Medieval Coin", None),
        ("Viking Brooch", Some("5x5cm")),
        ("Egyptian Amulet", Some("4x2cm")),
        ("Greek Pottery Fragment", Some("12x8cm")),
        ("Iron Age Axe Head", Some("18x10cm")),
        ("Roman Legionary Helmet", Some("25x22cm")),
        ("Byzantine Mosaic Tile", Some("10x10cm")),
        ("Celtic Torc Fragment", Some("6x3cm")),
        ("Neolithic Arrowhead", Some("4x2cm")),
        ("Persian Silver Bowl", Some("20x8cm")),
        ("Mayan Jade Pendant", Some("7x4cm")),
        ("Chinese Tang Figurine", Some("15x8cm")),
        ("Sumerian Clay Tablet", Some("12x9cm")),
        ("Norse Rune Stone Fragment", Some("30x20cm")),
        ("Etruscan Gold Earring", Some("3x2cm")),
        ("Minoan Seal Stone", Some("2x2cm")),
        ("Roman Glass Bottle", Some("12x5cm")),
        ("Aztec Obsidian Blade", Some("8x3cm")),
        ("Phoenician Glass Bead", Some("1x1cm")),
        ("Mesopotamian Cylinder Seal", Some("4x2cm")),
        ("Incan Quipu Fragment", None),
        ("Ottoman Calligraphy Scroll", Some("45x15cm")),
        ("Ming Dynasty Porcelain Shard", Some("5x4cm")),
    ];

    for (title, dims) in demo {
        artifacts::ActiveModel {
            title: sea_orm::Set(String::from(*title)),
            dimensions: sea_orm::Set(dims.map(|s| (*s).to_owned())),
            is_archived: sea_orm::Set(Some(0)), // I think some() is a cute keyword
            ..Default::default()
        }
        .insert(db)
        .await?;
    }

    log::info!("Seeded {} demo artifacts.", demo.len());
    Ok(())
}

// Fetch PAGE_SIZE artifacts starting at offset, optionally filtered by title.
async fn fetch_artifacts(
    db: &DbConn,
    offset: usize,
    query: &str,
) -> Result<Vec<ArtifactRow>, sea_orm::DbErr> {
    let mut select = artifacts::Entity::find().order_by_asc(artifacts::Column::Id);

    if !query.is_empty() {
        select = select.filter(artifacts::Column::Title.contains(query));
    }

    let rows = select
        .offset(offset as u64)
        .limit(PAGE_SIZE as u64)
        .all(db)
        .await?;

    Ok(rows
        .into_iter()
        .map(|m| ArtifactRow {
            id: m.id,
            title: m.title,
            dimensions: m.dimensions,
        })
        .collect())
}

// Build a fresh scroll list for the given db connection and search query.
// Called once on archive open and again whenever the search query changes.
fn make_scroll(
    db: DbConn,
    rt_handle: tokio::runtime::Handle,
    query: String,
) -> InfiniteScroll<ArtifactRow, usize> {
    InfiniteScroll::new().end_loader(move |cursor, callback| {
        let db = db.clone();
        let query = query.clone();
        let offset = cursor.unwrap_or(0);
        rt_handle.spawn(async move {
            match fetch_artifacts(&db, offset, &query).await {
                Ok(items) => {
                    let next = if items.len() < PAGE_SIZE {
                        None
                    } else {
                        Some(offset + PAGE_SIZE)
                    };
                    callback(Ok((items, next)));
                }
                Err(e) => callback(Err(e.to_string())),
            }
        });
    })
}

struct ArchiveManagerApp {
    // Launcher state
    archive_name: String,
    archive_exists: bool,
    show_confirm: bool,

    // Archive view state. None until an archive is opened.
    db: Option<DbConn>,
    scroll: Option<InfiniteScroll<ArtifactRow, usize>>,
    search_open: bool,
    search_query: String,
    archive_just_opened: bool, // true for one frame on entry, used to resize and retitle

    // Error propagator for displaying errors in the UI
    error_propagator: ErrorPropagator,

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
            db: None,
            scroll: None,
            search_open: false,
            search_query: String::new(),
            archive_just_opened: false,
            error_propagator: ErrorPropagator::new(),
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
                Ok(db) => {
                    if create {
                        // Populate fresh archives with some demo data so there is something to look at
                        // TODO: Remove this (but not before real data input is possible, obviously)
                        // Might want to keep some kind of demo mode, though (to demo for Hack Club).
                        if let Err(e) = seed_demo_data(&db).await {
                            log::warn!("Demo seeding failed: {}", e);
                        }
                    }
                    log::info!(
                        "{} archive '{}'",
                        if create { "Created" } else { "Opened" },
                        archive_name
                    );
                    // FIXME: I should really propagate this to the UI better.
                    if let Err(e) = tx.send(DbMessage::Opened(db)) {
                        log::error!("Failed to send DB message: {}", e);
                    }
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

    fn show_launcher(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        ui.add_space(8.0); // Completely arbitrary.

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

        // commented out but kept for easy testing of the error thing
        /*
        // test: create error test button
        if ui.button("Test Error").clicked() {
            self.error_propagator.push(
                "Test Error",
                Some("This is a test error message.".to_string()),
            );
        }
        */

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
                .show(ctx, |ui| {
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
    }

    fn show_archive(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        // Expand and retitle the window on the first frame after opening.
        if self.archive_just_opened {
            self.archive_just_opened = false;
            ctx.send_viewport_cmd(egui::ViewportCommand::InnerSize(egui::vec2(900.0, 600.0)));
            ctx.send_viewport_cmd(egui::ViewportCommand::Title(format!(
                "Archive Manager - {}",
                self.archive_name
            )));
        }

        // Ctrl+F toggles search.
        if ctx.input(|i| i.key_pressed(egui::Key::F) && i.modifiers.ctrl) {
            self.search_open = !self.search_open;
        }

        if let Some(scroll) = &mut self.scroll {
            egui::ScrollArea::vertical().show(ui, |ui| {
                scroll.ui(ui, 5, |ui, _idx, item| {
                    ui.horizontal(|ui| {
                        // Placeholder image - will be replaced once we have actual image data.
                        let thumb_size = egui::vec2(56.0, 56.0);
                        let (rect, _) = ui.allocate_exact_size(thumb_size, egui::Sense::hover());
                        ui.painter()
                            .rect_filled(rect, 4.0, egui::Color32::from_gray(55));
                        ui.painter().text(
                            rect.center(),
                            egui::Align2::CENTER_CENTER,
                            "img",
                            egui::FontId::proportional(10.0),
                            egui::Color32::from_gray(100),
                        );

                        ui.add_space(8.0);

                        ui.vertical(|ui| {
                            ui.add_space(6.0);
                            ui.strong(format!("#{}", item.id));
                            ui.label(&item.title);
                            if let Some(dims) = &item.dimensions {
                                ui.small(dims);
                            }
                        });
                    });

                    ui.separator();
                });
            });
        }

        // Search window, opened with Ctrl+F.
        // FIXME: Known issue: WILDLY inefficient, rebuilds the entire freaking scroll list every time
        // (So, I should really optimize this, but it works for now!)
        if self.search_open {
            if ctx.input(|i| i.key_pressed(egui::Key::Escape)) {
                self.search_open = false;
            }

            let mut open = true;
            let mut search_changed = false;
            egui::Window::new("Search")
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .default_pos([20.0, 40.0])
                .show(ctx, |ui| {
                    let response = ui.add(
                        egui::TextEdit::singleline(&mut self.search_query)
                            .hint_text("Search by title...")
                            .desired_width(220.0),
                    );
                    response.request_focus(); // this saves a click to focus the search box
                    if response.changed() {
                        search_changed = true;
                    }
                });
            if !open {
                self.search_open = false;
            }
            // Rebuild the scroll outside the closure once we know the query changed.
            if search_changed {
                if let Some(db) = &self.db {
                    self.scroll = Some(make_scroll(
                        db.clone(),
                        self.rt.handle().clone(),
                        self.search_query.clone(), // cloning this is fine?
                    ));
                }
            }
        }
    }
}

impl eframe::App for ArchiveManagerApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();

        // Process any results that came in from async tasks since the last frame.
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                DbMessage::Opened(db) => {
                    self.scroll = Some(make_scroll(
                        db.clone(), // Note to self: this does NOT clone the entire DB, just the connection.
                        // However, I should still try to avoid cloning things so much, because it's
                        // a memory hog. But then again to me it seems like the easiest thing for 99%
                        // of use cases to just clone stuff. Thanks for coming to my TED talk!
                        self.rt.handle().clone(),
                        String::new(),
                    ));
                    self.db = Some(db);
                    self.archive_just_opened = true;
                }
                DbMessage::Error(e) => {
                    self.error_propagator.push(e, None);
                }
            }
        }

        if self.db.is_none() {
            self.show_launcher(ui, &ctx);
        } else {
            self.show_archive(ui, &ctx);
        }

        // Display any errors that occurred.
        self.error_propagator.show(&ctx)
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
        Box::new(|cc| {
            let mut fonts = egui::FontDefinitions::default();
            egui_phosphor::add_to_fonts(&mut fonts, egui_phosphor::Variant::Regular);
            // Should patch egui phoshpor to have other variants I guess, but haven't gotten around to it.
            cc.egui_ctx.set_fonts(fonts);

            Ok(Box::new(ArchiveManagerApp::new()))
        }),
    )?;

    Ok(()) // Lesson learned: whatever a function returns it does not have a semicolon for some reason.
    // I do not like it.
}
