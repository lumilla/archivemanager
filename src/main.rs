mod entities;
mod error_propagator;

use eframe::egui::{self};
use egui_infinite_scroll::InfiniteScroll;
use entities::artifacts;
use error_propagator::ErrorPropagator;
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Database, DbConn, EntityTrait, PaginatorTrait, QueryFilter,
    QueryOrder, QuerySelect,
};
use sea_orm_migration::prelude::*;
use std::sync::{Arc, Mutex, mpsc};

const PAGE_SIZE: usize = 25;

// One row as fetched from the DB, kept small since it gets cloned a lot by the scroll list.
#[derive(Debug, Clone)]
struct ArtifactRow {
    id: i64,
    title: String,
    dimensions: Option<String>,
    selected: bool,
}

#[derive(Debug, Clone, PartialEq)]
enum DetailEditField {
    Title,
    Dimensions,
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
            is_archived: sea_orm::Set(Some(0)), // I think some() is a cute thing
            ..Default::default()
        }
        .insert(db)
        .await?;
    }

    log::info!("Seeded {} demo artifacts.", demo.len());
    Ok(())
}

// Fetch PAGE_SIZE artifacts starting at cursor, optionally filtered by title.
async fn fetch_artifacts(
    db: &DbConn,
    last_id: Option<i64>,
    query: &str,
) -> Result<Vec<ArtifactRow>, sea_orm::DbErr> {
    let mut select = artifacts::Entity::find().order_by_asc(artifacts::Column::Id);

    if !query.is_empty() {
        select = select.filter(artifacts::Column::Title.contains(query));
    }

    if let Some(id) = last_id {
        select = select.filter(artifacts::Column::Id.gt(id));
    }

    let rows = select.limit(PAGE_SIZE as u64).all(db).await?;

    Ok(rows
        .into_iter()
        .map(|m| ArtifactRow {
            id: m.id,
            title: m.title,
            dimensions: m.dimensions,
            selected: false,
        })
        .collect())
}

async fn update_artifact(
    db: &DbConn,
    id: i64,
    title: String,
    dimensions: Option<String>,
) -> Result<(), sea_orm::DbErr> {
    let artifact = artifacts::Entity::find_by_id(id).one(db).await?;
    if let Some(artifact) = artifact {
        let mut artifact: artifacts::ActiveModel = artifact.into();
        artifact.title = sea_orm::Set(title);
        artifact.dimensions = sea_orm::Set(dimensions);
        artifact.update(db).await?;
    }
    Ok(())
}

// Build a fresh scroll list for the given db connection and search query.
// Called once on archive open and again whenever the search query changes.
fn make_scroll(
    db: DbConn,
    rt_handle: tokio::runtime::Handle,
    query: String,
    active_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
) -> InfiniteScroll<ArtifactRow, i64> {
    InfiniteScroll::new().end_loader(move |cursor, callback| {
        let db = db.clone();
        let query = query.clone();
        let active_task = active_task.clone();

        let handle = rt_handle.spawn(async move {
            match fetch_artifacts(&db, cursor, &query).await {
                Ok(items) => {
                    let next = if items.len() < PAGE_SIZE {
                        None
                    } else {
                        items.last().map(|item| item.id)
                    };
                    callback(Ok((items, next)));
                }
                Err(e) => callback(Err(e.to_string())),
            }
        });

        // Instantly abort the old background query task if it's still dragging along
        let mut guard = active_task.lock().unwrap();
        if let Some(old_task) = guard.replace(handle) {
            old_task.abort();
        }
    })
}

struct ArchiveManagerApp {
    // Launcher state
    archive_name: String,
    archive_exists: bool,
    show_confirm: bool,

    // Archive view state. None until an archive is opened.
    db: Option<DbConn>,
    scroll: Option<InfiniteScroll<ArtifactRow, i64>>,
    search_open: bool,
    search_query: String,
    archive_just_opened: bool,

    // The item currently open in the detail view. None = detail window closed.
    detail_item: Option<ArtifactRow>,
    detail_edit_field: Option<DetailEditField>,
    detail_edit_buf: String,

    active_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,

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
            detail_item: None,
            detail_edit_field: None,
            detail_edit_buf: String::new(),
            active_task: Arc::new(Mutex::new(None)),
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

    fn save_artifact_edit(&self, di: &ArtifactRow) {
        if let Some(db) = &self.db {
            let db = db.clone();
            let id = di.id;
            let title = di.title.clone();
            let dimensions = di.dimensions.clone();
            let tx = self.tx.clone();
            self.rt.spawn(async move {
                if let Err(e) = update_artifact(&db, id, title, dimensions).await {
                    let _ = tx.send(DbMessage::Error(e.to_string()));
                }
            });
        }
    }

    fn sync_artifact_to_list(&mut self, id: i64, title: String, dimensions: Option<String>) {
        if let Some(scroll) = &mut self.scroll {
            for item in scroll.items.iter_mut() {
                if item.id == id {
                    item.title = title.clone();
                    item.dimensions = dimensions.clone();
                }
            }
        }
    }

    fn show_launcher(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
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

        // Collect any open-detail requests from the list so we can act on them
        // without holding a mutable borrow on self inside the scroll closure.
        let mut open_detail: Option<ArtifactRow> = None;

        if let Some(scroll) = &mut self.scroll {
            egui::ScrollArea::vertical().show(ui, |ui| {
                scroll.ui(ui, 5, |ui, _idx, item| {
                    ui.horizontal(|ui| {
                        // Checkbox for selection
                        let desired = egui::vec2(18.0, 56.0);
                        let (rect, response) =
                            ui.allocate_exact_size(desired, egui::Sense::click());
                        if response.clicked() {
                            item.selected = !item.selected;
                        }
                        let center = rect.center();
                        let radius = 8.0_f32;
                        let painter = ui.painter();
                        let (bg, border) = if item.selected {
                            (
                                egui::Color32::from_rgb(100, 160, 255),
                                egui::Color32::from_rgb(70, 130, 220),
                            )
                        } else {
                            (egui::Color32::from_gray(55), egui::Color32::from_gray(120))
                        };
                        painter.circle_filled(center, radius, bg);
                        painter.circle_stroke(center, radius, egui::Stroke::new(1.5, border));
                        if item.selected {
                            let o = egui::vec2(-3.5, 0.5);
                            let m = egui::vec2(-0.5, 3.0);
                            let e = egui::vec2(4.0, -3.5);
                            painter.line_segment(
                                [center + o, center + m],
                                egui::Stroke::new(2.0, egui::Color32::WHITE),
                            );
                            painter.line_segment(
                                [center + m, center + e],
                                egui::Stroke::new(2.0, egui::Color32::WHITE),
                            );
                        }

                        // Placeholder thumbnail.
                        let thumb_size = egui::vec2(56.0, 56.0);
                        let (rect, _) = ui.allocate_exact_size(thumb_size, egui::Sense::hover()); // I don't know what the hover does here?
                        let thumb_color = if item.selected {
                            egui::Color32::from_gray(35)
                        } else {
                            egui::Color32::from_gray(55)
                        };
                        ui.painter().rect_filled(rect, 4.0, thumb_color);
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

                            let resp = ui.label(&item.title);
                            if resp.double_clicked() {
                                open_detail = Some(item.clone());
                            }

                            if let Some(dims) = &item.dimensions {
                                ui.small(dims);
                            }
                        });
                    });

                    ui.separator();
                });
            });
        }

        // Apply any pending open-detail action now that the borrow is released... or something like that
        if let Some(item) = open_detail {
            self.detail_item = Some(item);
        }

        // Using a fixed ID so the title can change without egui complaining?
        if let Some(item) = &self.detail_item.clone() {
            let mut open = true;
            egui::Window::new(format!("Item #{} — {}", item.id, item.title))
                .id(egui::Id::new("detail_window"))
                .open(&mut open)
                .resizable(true)
                .min_size([320.0, 240.0])
                .default_size([420.0, 480.0])
                .show(ctx, |ui| {
                    let img_height = 160.0_f32;
                    let img_width = ui.available_width();
                    let (img_rect, _) = ui.allocate_exact_size(
                        egui::vec2(img_width, img_height),
                        egui::Sense::hover(),
                    );
                    ui.painter()
                        .rect_filled(img_rect, 6.0, egui::Color32::from_gray(40));
                    ui.painter().text(
                        img_rect.center(),
                        egui::Align2::CENTER_CENTER,
                        egui_phosphor::regular::IMAGE,
                        egui::FontId::proportional(36.0),
                        egui::Color32::from_gray(90),
                    );

                    ui.add_space(8.0); // Completely arbitrary.

                    egui::Grid::new("detail_grid")
                        .num_columns(3)
                        .spacing([8.0, 6.0])
                        .striped(true)
                        .show(ui, |ui| {
                            ui.strong("ID");
                            ui.label(item.id.to_string());
                            ui.label("");
                            ui.end_row();

                            ui.strong("Title");
                            let editing_title =
                                self.detail_edit_field == Some(DetailEditField::Title);
                            if editing_title {
                                let mut commit_item = None;
                                if let Some(ref mut di) = self.detail_item {
                                    let resp = ui.add(
                                        egui::TextEdit::singleline(&mut self.detail_edit_buf)
                                            .desired_width(f32::INFINITY),
                                    );
                                    resp.request_focus();
                                    let commit = resp.lost_focus()
                                        || ui.input(|i| i.key_pressed(egui::Key::Enter));
                                    if commit {
                                        di.title = self.detail_edit_buf.clone();
                                        commit_item = Some(di.clone());
                                        self.detail_edit_field = None;
                                    }
                                }
                                if let Some(item) = commit_item {
                                    self.save_artifact_edit(&item);
                                    self.sync_artifact_to_list(
                                        item.id,
                                        item.title,
                                        item.dimensions,
                                    );
                                }
                            } else {
                                ui.label(&item.title);
                                if ui
                                    .add(
                                        egui::Button::new(
                                            egui::RichText::new(egui_phosphor::regular::PEN)
                                                .size(14.0),
                                        )
                                        .frame(false),
                                    )
                                    .clicked()
                                {
                                    self.detail_edit_buf = item.title.clone();
                                    self.detail_edit_field = Some(DetailEditField::Title);
                                }
                            }
                            ui.end_row();

                            ui.strong("Dimensions");
                            let editing_dims =
                                self.detail_edit_field == Some(DetailEditField::Dimensions);
                            if editing_dims {
                                let mut commit_item = None;
                                if let Some(ref mut di) = self.detail_item {
                                    let resp = ui.add(
                                        egui::TextEdit::singleline(&mut self.detail_edit_buf)
                                            .desired_width(f32::INFINITY),
                                    );
                                    resp.request_focus();
                                    let commit = resp.lost_focus()
                                        || ui.input(|i| i.key_pressed(egui::Key::Enter));
                                    if commit {
                                        di.dimensions = if self.detail_edit_buf.is_empty() {
                                            None
                                        } else {
                                            Some(self.detail_edit_buf.clone())
                                        };
                                        commit_item = Some(di.clone());
                                        self.detail_edit_field = None;
                                    }
                                }
                                if let Some(item) = commit_item {
                                    self.save_artifact_edit(&item);
                                    self.sync_artifact_to_list(
                                        item.id,
                                        item.title,
                                        item.dimensions,
                                    );
                                }
                            } else {
                                ui.label(item.dimensions.as_deref().unwrap_or("—"));
                                if ui
                                    .add(
                                        egui::Button::new(
                                            egui::RichText::new(egui_phosphor::regular::PEN)
                                                .size(14.0),
                                        )
                                        .frame(false),
                                    )
                                    .clicked()
                                {
                                    self.detail_edit_buf =
                                        item.dimensions.clone().unwrap_or_default();
                                    self.detail_edit_field = Some(DetailEditField::Dimensions);
                                }
                            }
                            ui.end_row();
                        });
                });

            if !open {
                self.detail_item = None;
                self.detail_edit_field = None;
            }
        }

        // Search window, opened with Ctrl+F.
        // Now more efficient than it was, still not ideal
        // FIXME: Known issue: The UI will blink when the search query changes,
        // I found fixing this exceedingly difficult, and gave up.
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

            if search_changed {
                if let Some(db) = &self.db {
                    if let Some(old_task) = self.active_task.lock().unwrap().take() {
                        old_task.abort();
                    }
                    self.scroll = Some(make_scroll(
                        db.clone(),
                        self.rt.handle().clone(),
                        self.search_query.clone(),
                        self.active_task.clone(),
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
                        self.active_task.clone(),
                    ));
                    self.db = Some(db);
                    self.archive_just_opened = true; // true for one frame on entry, used to resize and retitle
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

/* You may ask: "Why build a custom schema? Why not use CIDOC CRM?"

The short answer is that in my opinion, CIDOC is a monolithic, over-engineered nightmare
that serves ISO committees better than it serves actual local museum curators.

I spent TOO much time trying to implement the CIDOC ontology. It is
essentially an academic fever dream that effectively demands a semantic graph database architecture.
Right, I attempted to use SurrealDB to handle the graph relationships,
this not only made the compile times gigantic, also was a massive pain in the ass in all the ways
barely worked, easily tripled the lines of code here.
Doing even simple things with CIDOC required dozens of lines of boilerplate.
Though yes, CIDOC is theoretically "correct" for interoperability in massive, multi-national
institutional databases. For this app, it is a parasitic, soul-crushing weight and I have a
love-hate relationship with it. I theoretically would want this app to use CIDOC for
interoperability (read: international standards are cool)
HOWEVER, I do NOT want to maintain that, and I could not even write it. Hell, I tried AI tools and
even those completely and utterly failed at even really initializing a CIDOC schema.
Thus I have opted for a (completely arbitrary), flat, sane, relational schema that I can actually maintain.
If the museum ever needs to export to CIDOC standards in the future, we can
write an adapter layer then. Or something like that, anyway. But for now? Screw that. I am prioritizing
functionality and a codebase that doesn't make me want to walk into the sea.
(Also wikidata model would be cool, also kind of a graph DB though. And I am NOT touching that)
*/
