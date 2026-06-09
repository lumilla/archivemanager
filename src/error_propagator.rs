// reusable error window, probably unnecessary to have as its own file, but oh well.
use eframe::egui;
use std::cell::Cell;

#[derive(Clone)]
struct Error {
    id: u64,
    message: String,
    details: Option<String>,
    should_close: Cell<bool>,
}

#[derive(Clone, Default)]
pub struct ErrorPropagator {
    errors: Vec<Error>,
    next_id: Cell<u64>,
}

impl ErrorPropagator {
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            next_id: Cell::new(0),
        }
    }

    pub fn push(&mut self, message: impl Into<String>, details: Option<String>) {
        let id = self.next_id.get();
        self.next_id.set(id + 1);

        self.errors.push(Error {
            id,
            message: message.into(),
            details,
            should_close: Cell::new(false),
        });
    }

    pub fn show(&mut self, ctx: &egui::Context) {
        for (index, error) in self.errors.iter().enumerate() {
            let mut open = true;

            egui::Window::new(format!("Error #{}", error.id))
                // Error number fixes the issue where two errors at the same time would not be displayed.
                .open(&mut open)
                .collapsible(false)
                .resizable(false)
                .constrain(true)
                .default_pos(
                    ctx.content_rect().center()
                        - egui::vec2(150.0 + (index as f32 * 15.0), 60.0 + (index as f32 * 15.0)),
                )
                .show(ctx, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(
                            egui::RichText::new(egui_phosphor::regular::WARNING_CIRCLE)
                                .size(24.0)
                                .color(egui::Color32::from_rgb(255, 149, 0)),
                        );
                        ui.add_space(4.0);
                        ui.label(egui::RichText::new("Error").strong());
                    });

                    ui.separator();

                    ui.label(&error.message);

                    if let Some(details) = &error.details
                        && !details.is_empty() {
                            egui::CollapsingHeader::new("Details")
                                .default_open(false)
                                .show(ui, |ui| {
                                    egui::Frame::new()
                                        .fill(egui::Color32::from_rgb(30, 30, 30))
                                        .inner_margin(egui::Margin::symmetric(4, 4))
                                        .show(ui, |ui| {
                                            ui.code(details);
                                        });
                                });
                        }

                    ui.add_space(8.0);
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Min), |ui| {
                        if ui.button("Close").clicked() {
                            error.should_close.set(true);
                        }
                    });
                });

            if !open {
                error.should_close.set(true);
            }
        }

        self.errors.retain(|e| !e.should_close.get());
    }
}
