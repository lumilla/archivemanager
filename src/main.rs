mod db;
mod entities;
mod error_propagator;

use db::{ArchiveDb, ArchiveStatus};

use eframe::egui::{self};
use egui_infinite_scroll::InfiniteScroll;
use entities::artifacts::{self, DynamicModel, Schema};
use error_propagator::ErrorPropagator;
use std::sync::{Arc, Mutex, mpsc};

const PAGE_SIZE: usize = 25;

// Domain row type

// One artifact row as shown in the scroll list.  Kept small since it is
// cloned frequently by the infinite-scroll widget.
#[derive(Debug, Clone)]
struct ArtifactRow {
    model: DynamicModel,
    selected: bool,
}

#[derive(Debug, Clone, PartialEq)]
struct DetailEditField {
    id: String,
}

// Async to UI message bus

enum DbMessage {
    // Initial connection succeeded.
    Connected(ArchiveDb),
    // Archive probe result.
    ArchiveStatus(ArchiveStatus),
    // Archive is fully ready (opened or migrated).
    Opened,
    // Any recoverable error to show to the user.
    Error(String),
}

// Infinite-scroll factory

fn make_scroll(
    db: ArchiveDb,
    rt_handle: tokio::runtime::Handle,
    archive_name: String,
    search_query: String,
    schema: Arc<Schema>,
    active_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
) -> InfiniteScroll<ArtifactRow, usize> {
    InfiniteScroll::new().end_loader(move |cursor, callback| {
        let db = db.clone();
        let archive_name = archive_name.clone();
        let search_query = search_query.clone();
        let schema = schema.clone();
        let active_task = active_task.clone();
        let skip = cursor.unwrap_or(0);

        let handle = rt_handle.spawn(async move {
            match db
                .list_artifacts(&archive_name, &schema, skip, PAGE_SIZE, &search_query)
                .await
            {
                Ok(models) => {
                    let next = if models.len() < PAGE_SIZE {
                        None
                    } else {
                        Some(skip + models.len())
                    };
                    let rows = models
                        .into_iter()
                        .map(|m| ArtifactRow {
                            model: m,
                            selected: false,
                        })
                        .collect::<Vec<_>>();
                    callback(Ok((rows, next)));
                }
                Err(e) => callback(Err(e.to_string())),
            }
        });

        // Abort any still-running page load from a previous scroll position.
        let mut guard = active_task.lock().unwrap();
        if let Some(old) = guard.replace(handle) {
            old.abort();
        }
    })
}

// Application state

struct ArchiveManagerApp {
    schema: Arc<Schema>,
    db: Option<ArchiveDb>,

    // Launcher state
    archive_name: String,
    show_confirm: bool,
    show_migration_confirm: bool,
    migration_fields_to_remove: Vec<String>,
    migration_fields_to_add: Vec<String>,
    archive_open: bool,

    // Archive view state
    scroll: Option<InfiniteScroll<ArtifactRow, usize>>,
    search_open: bool,
    search_query: String,
    archive_just_opened: bool,

    // Detail panel state
    detail_item: Option<ArtifactRow>,
    detail_edit_field: Option<DetailEditField>,
    detail_edit_buf: String,
    detail_edit_unit: String,

    // Async infrastructure
    active_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    error_propagator: ErrorPropagator,
    tx: mpsc::Sender<DbMessage>,
    rx: mpsc::Receiver<DbMessage>,
    rt: tokio::runtime::Runtime,
}

impl ArchiveManagerApp {
    fn new(schema: Schema) -> Self {
        let (tx, rx) = mpsc::channel();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to create Tokio runtime");

        // Kick off the initial DB connection in the background.
        let tx_clone = tx.clone();
        rt.spawn(async move {
            match db::connect().await {
                Ok(db) => {
                    let _ = tx_clone.send(DbMessage::Connected(db));
                }
                Err(e) => {
                    let _ = tx_clone.send(DbMessage::Error(e.to_string()));
                }
            }
        });

        Self {
            schema: Arc::new(schema),
            db: None,
            archive_name: String::new(),
            show_confirm: false,
            show_migration_confirm: false,
            migration_fields_to_remove: Vec::new(),
            migration_fields_to_add: Vec::new(),
            archive_open: false,
            scroll: None,
            search_open: false,
            search_query: String::new(),
            archive_just_opened: false,
            detail_item: None,
            detail_edit_field: None,
            detail_edit_buf: String::new(),
            detail_edit_unit: String::new(),
            active_task: Arc::new(Mutex::new(None)),
            error_propagator: ErrorPropagator::new(),
            tx,
            rx,
            rt,
        }
    }

    // DB operation helpers

    // Probe the archive and send an ArchiveStatus message back.
    fn check_archive_op(&self, ctx: &egui::Context) {
        if let Some(db) = &self.db {
            let db = db.clone();
            let archive_name = self.archive_name.clone();
            let tx = self.tx.clone();
            self.rt.spawn(async move {
                match db.get_archive_status(&archive_name).await {
                    Ok(status) => {
                        tx.send(DbMessage::ArchiveStatus(status)).ok();
                    }
                    Err(e) => {
                        tx.send(DbMessage::Error(e.to_string())).ok();
                    }
                }
            });
            ctx.request_repaint();
        }
    }

    // Run a schema migration (add/remove fields), then signal Opened.
    fn run_migration_op(
        &self,
        fields_to_add: Vec<String>,
        fields_to_remove: Vec<String>,
        ctx: &egui::Context,
    ) {
        if let Some(db) = &self.db {
            let db = db.clone();
            let archive_name = self.archive_name.clone();
            let tx = self.tx.clone();
            self.rt.spawn(async move {
                match db
                    .migrate(&archive_name, fields_to_add, fields_to_remove)
                    .await
                {
                    Ok(()) => tx.send(DbMessage::Opened).ok(),
                    Err(e) => tx.send(DbMessage::Error(e.to_string())).ok(),
                };
            });
            ctx.request_repaint();
        }
    }

    // Open (and optionally seed) the archive, then signal Opened.
    fn open_archive_op(&self, seed: bool, ctx: &egui::Context) {
        if let Some(db) = &self.db {
            let db = db.clone();
            let archive_name = self.archive_name.clone();
            let schema = self.schema.clone();
            let tx = self.tx.clone();
            self.rt.spawn(async move {
                if seed {
                    // Load demo data from demo_data.toml on first open.
                    // Swap this file for any other TOML import to pre-populate a new archive.
                    match db
                        .import_from_file(&archive_name, "demo_data.toml", &schema)
                        .await
                    {
                        Ok(r) => log::info!("Loaded {} demo records.", r.records_affected),
                        Err(e) => log::warn!("Demo import failed: {e}"),
                    }
                }
                tx.send(DbMessage::Opened).ok();
            });
            ctx.request_repaint();
        }
    }

    // Persist edits on a single artifact row back to the DB.
    fn save_artifact_edit(&self, row: &ArtifactRow) {
        if let Some(db) = &self.db {
            let db = db.clone();
            let model = row.model.clone();
            let schema = self.schema.clone();
            let archive_name = self.archive_name.clone();
            let tx = self.tx.clone();
            self.rt.spawn(async move {
                if let Err(e) = db.update_artifact(&archive_name, &model, &schema).await {
                    let _ = tx.send(DbMessage::Error(e.to_string()));
                }
            });
        }
    }

    // Mirror an in-memory model update back into the scroll list so the list
    // stays in sync without a full reload.
    fn sync_artifact_to_list(&mut self, updated: DynamicModel) {
        if let Some(scroll) = &mut self.scroll {
            for row in scroll.items.iter_mut() {
                if row.model.id == updated.id {
                    row.model = updated.clone();
                }
            }
        }
    }

    // UI panels

    fn show_launcher(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        ui.add_space(8.0);

        if self.db.is_none() {
            ui.label("Connecting to Neo4j database...");
            ui.spinner();
            return;
        }

        ui.label("Enter Archive Label (Neo4j):");
        ui.add(
            egui::TextEdit::singleline(&mut self.archive_name)
                .hint_text("e.g. RomanCollection")
                .desired_width(f32::INFINITY),
        );
        ui.add_space(4.0);

        ui.add_enabled_ui(!self.archive_name.is_empty(), |ui| {
            if ui.button("Open / Create Archive").clicked() {
                self.check_archive_op(ctx);
            }
        });

        // Create archive confirmation dialog
        if self.show_confirm {
            let archive_name = self.archive_name.clone();
            let mut open = true;
            egui::Window::new("Create Archive?")
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .default_pos(ctx.content_rect().center() - egui::vec2(120.0, 50.0))
                .show(ctx, |ui| {
                    ui.label(format!(
                        "Archive label '{}' not found. Create it?",
                        archive_name
                    ));
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("Yes, Create It").clicked() {
                            self.show_confirm = false;
                            self.open_archive_op(true, ctx);
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

        // Destructive migration confirmation dialog
        if self.show_migration_confirm {
            let mut open = true;
            let remove_list = self.migration_fields_to_remove.join(", ");
            let add_list = self.migration_fields_to_add.join(", ");
            let archive_name = self.archive_name.clone();

            egui::Window::new("Destructive Schema Migration?")
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .default_pos(ctx.content_rect().center() - egui::vec2(150.0, 75.0))
                .show(ctx, |ui| {
                    ui.label(format!(
                        "Archive '{}' has a schema mismatch with schema.toml.",
                        archive_name
                    ));
                    ui.add_space(4.0);
                    if !self.migration_fields_to_remove.is_empty() {
                        ui.colored_label(
                            egui::Color32::LIGHT_RED,
                            format!(
                                "DESTRUCTIVE: The following fields will be permanently REMOVED:\n{}",
                                remove_list
                            ),
                        );
                    }
                    if !self.migration_fields_to_add.is_empty() {
                        ui.label(format!("The following new fields will be added:\n{}", add_list));
                    }
                    ui.add_space(8.0);
                    ui.label("Are you sure you want to run the migration and open the database?");
                    ui.add_space(8.0);
                    ui.horizontal(|ui| {
                        if ui.button("Yes, Migrate & Open").clicked() {
                            self.show_migration_confirm = false;
                            self.run_migration_op(
                                self.migration_fields_to_add.clone(),
                                self.migration_fields_to_remove.clone(),
                                ctx,
                            );
                        }
                        if ui.button("Cancel").clicked() {
                            self.show_migration_confirm = false;
                            self.migration_fields_to_remove.clear();
                            self.migration_fields_to_add.clear();
                        }
                    });
                });
            if !open {
                self.show_migration_confirm = false;
                self.migration_fields_to_remove.clear();
                self.migration_fields_to_add.clear();
            }
        }
    }

    fn show_archive(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        // Expand and retitle the window on the first frame after opening.
        if self.archive_just_opened {
            self.archive_just_opened = false;
            ctx.send_viewport_cmd(egui::ViewportCommand::InnerSize(egui::vec2(900.0, 600.0)));
            ctx.send_viewport_cmd(egui::ViewportCommand::Title(format!(
                "Graph Archive - {}",
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
                        // Selection circle
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

                        // Placeholder thumbnail
                        let thumb_size = egui::vec2(56.0, 56.0);
                        let (rect, _) = ui.allocate_exact_size(thumb_size, egui::Sense::hover());
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

                        // Row text
                        ui.vertical(|ui| {
                            ui.add_space(6.0);
                            ui.strong(format!("#{}", item.model.id));

                            let title_field = self.schema.get_title_field_id();
                            let title = item.model.get_field(&title_field);
                            let resp = ui.label(if title.is_empty() { "Untitled" } else { &title });
                            if resp.double_clicked() {
                                open_detail = Some(item.clone());
                            }

                            for feature in self
                                .schema
                                .features
                                .iter()
                                .filter(|f| !f.system_title.unwrap_or(false))
                                .take(2)
                            {
                                let val = item.model.get_field(&feature.id);
                                if !val.is_empty() && val != "—" {
                                    ui.small(format!("{}: {}", feature.label, val));
                                }
                            }
                        });
                    });
                    ui.separator();
                });
            });
        }

        // Apply any pending open-detail action now that the borrow on scroll
        // has been released.
        if let Some(item) = open_detail {
            self.detail_item = Some(item);
        }

        // Detail window
        let detail_item = self.detail_item.clone();
        if let Some(item) = detail_item {
            let mut open = true;
            egui::Window::new(format!(
                "Graph Item #{} — {}",
                item.model.id,
                item.model.get_field(&self.schema.get_title_field_id())
            ))
            .id(egui::Id::new("detail_window"))
            .open(&mut open)
            .resizable(true)
            .min_size([320.0, 240.0])
            .default_size([420.0, 480.0])
            .show(ctx, |ui| {
                let img_height = 160.0_f32;
                let img_width = ui.available_width();
                let (img_rect, _) =
                    ui.allocate_exact_size(egui::vec2(img_width, img_height), egui::Sense::hover());
                ui.painter()
                    .rect_filled(img_rect, 6.0, egui::Color32::from_gray(40));
                ui.painter().text(
                    img_rect.center(),
                    egui::Align2::CENTER_CENTER,
                    egui_phosphor::regular::IMAGE,
                    egui::FontId::proportional(36.0),
                    egui::Color32::from_gray(90),
                );

                ui.add_space(8.0);

                egui::Grid::new("detail_grid")
                    .num_columns(3)
                    .spacing([8.0, 6.0])
                    .striped(true)
                    .show(ui, |ui| {
                        ui.strong("Neo4j ID");
                        ui.label(item.model.id.to_string());
                        ui.label("");
                        ui.end_row();

                        let mut new_edit_field = None;
                        let mut next_edit_buf = String::new();
                        let mut next_edit_unit = String::new();
                        let features = self.schema.features.clone();
                        for feature in &features {
                            ui.strong(&feature.label);
                            let is_editing = self
                                .detail_edit_field
                                .as_ref()
                                .is_some_and(|f| f.id == feature.id);

                            if is_editing {
                                let mut commit_item = None;
                                let mut di = item.clone();
                                let mut commit = false;

                                match &feature.ui_type {
                                    artifacts::UiTypeDef::Dropdown { options } => {
                                        let selected_text = if self.detail_edit_buf.is_empty() {
                                            "—"
                                        } else {
                                            &self.detail_edit_buf
                                        };
                                        egui::ComboBox::from_id_salt(&feature.id)
                                            .selected_text(selected_text)
                                            .show_ui(ui, |ui| {
                                                if !feature.required
                                                    && ui
                                                        .selectable_value(
                                                            &mut self.detail_edit_buf,
                                                            String::new(),
                                                            "—",
                                                        )
                                                        .clicked()
                                                {
                                                    commit = true;
                                                }
                                                for opt in options {
                                                    if ui
                                                        .selectable_value(
                                                            &mut self.detail_edit_buf,
                                                            opt.to_owned(),
                                                            opt,
                                                        )
                                                        .clicked()
                                                    {
                                                        commit = true;
                                                    }
                                                }
                                            });
                                    }
                                    artifacts::UiTypeDef::Unit { options, .. } => {
                                        ui.horizontal(|ui| {
                                            let r = ui.add(
                                                egui::TextEdit::singleline(
                                                    &mut self.detail_edit_buf,
                                                )
                                                .desired_width(100.0),
                                            );
                                            r.request_focus();
                                            if r.lost_focus()
                                                || ui.input(|i| i.key_pressed(egui::Key::Enter))
                                            {
                                                commit = true;
                                            }
                                            let selected_unit = if self.detail_edit_unit.is_empty()
                                            {
                                                "—"
                                            } else {
                                                &self.detail_edit_unit
                                            };
                                            egui::ComboBox::from_id_salt(format!(
                                                "{}_unit",
                                                feature.id
                                            ))
                                            .selected_text(selected_unit)
                                            .show_ui(
                                                ui,
                                                |ui| {
                                                    if ui
                                                        .selectable_value(
                                                            &mut self.detail_edit_unit,
                                                            String::new(),
                                                            "—",
                                                        )
                                                        .clicked()
                                                    {
                                                        commit = true;
                                                    }
                                                    for opt in options {
                                                        if ui
                                                            .selectable_value(
                                                                &mut self.detail_edit_unit,
                                                                opt.to_owned(),
                                                                opt,
                                                            )
                                                            .clicked()
                                                        {
                                                            commit = true;
                                                        }
                                                    }
                                                },
                                            );
                                        });
                                    }
                                    artifacts::UiTypeDef::Text => {
                                        let r = ui.add(
                                            egui::TextEdit::singleline(&mut self.detail_edit_buf)
                                                .desired_width(f32::INFINITY),
                                        );
                                        r.request_focus();
                                        if r.lost_focus()
                                            || ui.input(|i| i.key_pressed(egui::Key::Enter))
                                        {
                                            commit = true;
                                        }
                                    }
                                }

                                if commit {
                                    let val_to_insert = match &feature.ui_type {
                                        artifacts::UiTypeDef::Unit { .. } => format!(
                                            "{}{}",
                                            self.detail_edit_buf, self.detail_edit_unit
                                        ),
                                        _ => self.detail_edit_buf.clone(),
                                    };
                                    di.model.fields.insert(feature.id.clone(), val_to_insert);
                                    commit_item = Some(di.clone());
                                    self.detail_edit_field = None;
                                }

                                if let Some(committed) = commit_item {
                                    self.save_artifact_edit(&committed);
                                    self.sync_artifact_to_list(committed.model.clone());
                                    self.detail_item = Some(committed);
                                }

                                ui.label("");
                            } else {
                                let val = item.model.get_field(&feature.id);
                                ui.label(if val.is_empty() { "—" } else { &val });

                                if ui.button(egui_phosphor::regular::PEN).clicked() {
                                    new_edit_field = Some(DetailEditField {
                                        id: feature.id.to_string(),
                                    });
                                    match &feature.ui_type {
                                        artifacts::UiTypeDef::Unit { options, .. } => {
                                            let (v, u) = artifacts::split_unit(&val, options);
                                            next_edit_buf = v;
                                            next_edit_unit = u;
                                        }
                                        _ => {
                                            next_edit_buf = val;
                                            next_edit_unit = String::new();
                                        }
                                    }
                                }
                            }
                            ui.end_row();
                        }

                        if let Some(next) = new_edit_field {
                            self.detail_edit_field = Some(next);
                            self.detail_edit_buf = next_edit_buf;
                            self.detail_edit_unit = next_edit_unit;
                        }
                    });
            });
            if !open {
                self.detail_item = None;
                self.detail_edit_field = None;
            }
        }

        // Search window (Ctrl+F)
        // FIXME: Known issue - the list blinks when the search query changes.
        if self.search_open {
            let mut search_changed = false;
            egui::Window::new("Search").show(ctx, |ui| {
                let resp = ui
                    .add(egui::TextEdit::singleline(&mut self.search_query).hint_text("Search..."));
                resp.request_focus();
                if resp.changed() {
                    search_changed = true;
                }
            });
            if search_changed {
                if let Some(db) = &self.db {
                    self.scroll = Some(make_scroll(
                        db.clone(),
                        self.rt.handle().clone(),
                        self.archive_name.clone(),
                        self.search_query.clone(),
                        self.schema.clone(),
                        self.active_task.clone(),
                    ));
                }
            }
        }
    }
}

// eframe App impl

impl eframe::App for ArchiveManagerApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();

        // Process any results that came in from async tasks since the last frame.
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                DbMessage::Connected(db) => {
                    self.db = Some(db);
                }

                DbMessage::ArchiveStatus(status) => {
                    if status.exists {
                        let schema_fields: std::collections::HashSet<String> =
                            self.schema.features.iter().map(|f| f.id.clone()).collect();
                        let db_fields: std::collections::HashSet<String> =
                            status.property_fields.into_iter().collect();

                        let missing_in_db: Vec<String> =
                            schema_fields.difference(&db_fields).cloned().collect();
                        let extra_in_db: Vec<String> =
                            db_fields.difference(&schema_fields).cloned().collect();

                        if !extra_in_db.is_empty() {
                            self.migration_fields_to_remove = extra_in_db;
                            self.migration_fields_to_add = missing_in_db;
                            self.show_migration_confirm = true;
                        } else if !missing_in_db.is_empty() {
                            self.run_migration_op(missing_in_db, Vec::new(), &ctx);
                        } else {
                            self.open_archive_op(false, &ctx);
                        }
                    } else {
                        self.show_confirm = true;
                    }
                }

                DbMessage::Opened => {
                    self.archive_open = true;
                    self.scroll = Some(make_scroll(
                        self.db.clone().unwrap(),
                        self.rt.handle().clone(),
                        self.archive_name.clone(),
                        String::new(),
                        self.schema.clone(),
                        self.active_task.clone(),
                    ));
                    self.archive_just_opened = true;
                }

                DbMessage::Error(e) => {
                    self.error_propagator.push(e, None);
                }
            }
        }

        if !self.archive_open {
            self.show_launcher(ui, &ctx);
        } else {
            self.show_archive(ui, &ctx);
        }

        self.error_propagator.show(&ctx);
    }
}

// Entry point

fn main() -> anyhow::Result<()> {
    let _ = dotenvy::dotenv();

    // Initialize logging. At this time, the default level is info.
    let _ = env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .try_init();

    // Hard fail if the schema is invalid - there is nothing useful to do without it.
    let schema = Schema::load("schema.toml")
        .expect("Failed to load schema.toml! Refusing to start without a valid schema.");

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([400.0, 120.0])
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
            cc.egui_ctx.set_fonts(fonts);
            Ok(Box::new(ArchiveManagerApp::new(schema)))
        }),
    )?;

    Ok(())
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

// Alright, update on the above:
// I did... in fact, go for a graph DB. It was the only way to handle the graph relationships efficiently.
// ...which in plain terms is "I thought it's too cool" and ignored the fact its hell actually.
// But at least the Cypher queries are all hidden behind a DAL now. So it's cool hell.
