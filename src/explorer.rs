use std::collections::BTreeSet;

use egui::{Color32, RichText, ScrollArea, Ui};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

use crate::endpoint::{
    DirEntryInfo, EndpointPath, find_companion_mmproj, format_bytes, is_temp_artifact_name,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PaneRole {
    Source,
    Destination,
}

pub enum ExplorerAsyncMsg {
    DirectoryLoaded {
        request_id: u64,
        dir: EndpointPath,
        result: Result<Vec<DirEntryInfo>, String>,
    },
    FolderCreated {
        parent: EndpointPath,
        created: EndpointPath,
        result: Result<(), String>,
    },
    NativePicked {
        picked: Vec<EndpointPath>,
        navigate_into_first_dir: bool,
    },
}

pub struct ExplorerPane {
    pub role: PaneRole,
    pub current_dir: EndpointPath,
    pub address_input: String,
    pub filter_text: String,
    pub new_folder_name: String,
    pub show_new_folder_bar: bool,
    pub entries: Vec<DirEntryInfo>,
    pub selected: BTreeSet<usize>,
    pub loading: bool,
    pub error_banner: Option<String>,
    pub status_note: Option<String>,
    request_counter: u64,
    tx: mpsc::UnboundedSender<ExplorerAsyncMsg>,
    rx: mpsc::UnboundedReceiver<ExplorerAsyncMsg>,
    rt: Handle,
}

pub enum PaneAction {
    None,
    PinBookmark(EndpointPath),
    AddDestination(EndpointPath),
    QuickMorphTo(EndpointPath),
}

impl ExplorerPane {
    pub fn new(role: PaneRole, initial_dir: EndpointPath, rt: Handle) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut pane = Self {
            role,
            address_input: initial_dir.to_string(),
            current_dir: initial_dir.clone(),
            filter_text: String::new(),
            new_folder_name: String::new(),
            show_new_folder_bar: false,
            entries: Vec::new(),
            selected: BTreeSet::new(),
            loading: false,
            error_banner: None,
            status_note: None,
            request_counter: 0,
            tx,
            rx,
            rt,
        };
        pane.navigate_to(initial_dir);
        pane
    }

    pub fn navigate_to(&mut self, dir: EndpointPath) {
        self.request_counter = self.request_counter.wrapping_add(1);
        let req_id = self.request_counter;
        self.current_dir = dir.clone();
        self.address_input = dir.to_string();
        self.selected.clear();
        self.loading = true;
        self.error_banner = None;
        self.status_note = None;

        let tx = self.tx.clone();
        self.rt.spawn(async move {
            let res = dir.list_dir().await.map_err(|e| format!("{e:#}"));
            let _ = tx.send(ExplorerAsyncMsg::DirectoryLoaded {
                request_id: req_id,
                dir,
                result: res,
            });
        });
    }

    pub fn refresh(&mut self) {
        let dir = self.current_dir.clone();
        self.navigate_to(dir);
    }

    pub fn poll_messages(&mut self) {
        while let Ok(msg) = self.rx.try_recv() {
            match msg {
                ExplorerAsyncMsg::DirectoryLoaded {
                    request_id,
                    dir,
                    result,
                } => {
                    if request_id != self.request_counter {
                        continue;
                    }
                    self.loading = false;
                    self.current_dir = dir.clone();
                    self.address_input = dir.to_string();
                    match result {
                        Ok(list) => {
                            self.entries = list;
                            self.error_banner = None;
                        }
                        Err(err) => {
                            self.entries.clear();
                            self.error_banner = Some(err);
                        }
                    }
                }
                ExplorerAsyncMsg::FolderCreated {
                    parent: _,
                    created,
                    result,
                } => match result {
                    Ok(()) => {
                        self.new_folder_name.clear();
                        self.show_new_folder_bar = false;
                        self.status_note = Some(format!("Created folder {created}"));
                        self.navigate_to(created);
                    }
                    Err(err) => {
                        self.error_banner = Some(err);
                    }
                },
                ExplorerAsyncMsg::NativePicked {
                    picked,
                    navigate_into_first_dir,
                } => {
                    if picked.is_empty() {
                        continue;
                    }
                    if navigate_into_first_dir {
                        self.navigate_to(picked[0].clone());
                    } else if let Some(parent) = picked[0].parent() {
                        // Navigate to the parent directory so picked files are visible.
                        let picked_names: BTreeSet<String> =
                            picked.iter().filter_map(EndpointPath::file_name).collect();
                        self.navigate_to(parent);
                        self.status_note = Some(format!(
                            "Picked: {}",
                            picked_names.into_iter().collect::<Vec<_>>().join(", ")
                        ));
                    }
                }
            }
        }
    }

    /// Returns currently selected endpoints in this pane.
    /// For Destination panes, if no subfolder is selected, returns the open directory itself.
    pub fn selected_endpoints(&self) -> Vec<EndpointPath> {
        self.selected
            .iter()
            .filter_map(|&idx| self.entries.get(idx))
            .map(|e| e.path.clone())
            .collect()
    }

    /// For Destination panes: returns the highlighted subfolder if a single directory is selected,
    /// otherwise returns `self.current_dir`.
    pub fn active_destination_folder(&self) -> EndpointPath {
        if self.selected.len() == 1
            && let Some(&idx) = self.selected.iter().next()
            && let Some(entry) = self.entries.get(idx)
            && entry.is_dir
        {
            return entry.path.clone();
        }
        self.current_dir.clone()
    }

    pub fn selected_known_bytes(&self) -> u64 {
        self.selected
            .iter()
            .filter_map(|&idx| self.entries.get(idx))
            .map(|e| e.size)
            .sum()
    }

    pub fn ui(&mut self, ui: &mut Ui, hide_temp_artifacts: bool) -> PaneAction {
        self.poll_messages();
        let mut action = PaneAction::None;

        let title = match self.role {
            PaneRole::Source => "Source Explorer (X)",
            PaneRole::Destination => "Destination Explorer (Xⁿ)",
        };

        ui.horizontal(|ui| {
            ui.heading(RichText::new(title).strong());
            if self.current_dir.is_ssh() {
                ui.label(
                    RichText::new(" SSH / TAILNET ")
                        .small()
                        .strong()
                        .color(Color32::from_rgb(20, 25, 30))
                        .background_color(Color32::from_rgb(100, 220, 180)),
                );
            }
            if self.loading {
                ui.spinner();
            }
        });

        // Address bar + navigation controls
        ui.horizontal(|ui| {
            let up_enabled = self.current_dir.parent().is_some();
            if ui
                .add_enabled(up_enabled, egui::Button::new("⬆ Up"))
                .on_hover_text("Go to parent folder")
                .clicked()
                && let Some(parent) = self.current_dir.parent()
            {
                self.navigate_to(parent);
            }

            if ui.button("⟳").on_hover_text("Refresh directory").clicked() {
                self.refresh();
            }

            let addr_resp = ui.add(
                egui::TextEdit::singleline(&mut self.address_input)
                    .desired_width(ui.available_width() - 175.0)
                    .hint_text(r"D:\Models, \\ZIMABOARD2\Ark\Models, or ssh://root@host/path"),
            );
            let enter_pressed =
                addr_resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));

            if ui.button("Go").clicked() || enter_pressed {
                match EndpointPath::parse(&self.address_input) {
                    Ok(ep) => self.navigate_to(ep),
                    Err(e) => self.error_banner = Some(e.to_string()),
                }
            }

            if ui
                .button("★ Pin")
                .on_hover_text("Bookmark current folder in sidebar")
                .clicked()
            {
                action = PaneAction::PinBookmark(self.current_dir.clone());
            }

            if ui
                .button("📂 OS...")
                .on_hover_text("Pick folder via native OS file dialog")
                .clicked()
            {
                self.spawn_native_folder_picker();
            }
        });

        // Filter & "+ New Folder" toolbar
        ui.horizontal(|ui| {
            ui.label("🔍");
            ui.add(
                egui::TextEdit::singleline(&mut self.filter_text)
                    .desired_width(170.0)
                    .hint_text("Filter models / files..."),
            );
            if !self.filter_text.is_empty() && ui.small_button("×").clicked() {
                self.filter_text.clear();
            }

            ui.separator();

            if ui
                .button("+ New Folder")
                .on_hover_text("Create a new folder inside the current directory")
                .clicked()
            {
                self.show_new_folder_bar = !self.show_new_folder_bar;
            }

            if self.role == PaneRole::Source
                && !self.selected.is_empty()
                && ui.small_button("Clear Selection").clicked()
            {
                self.selected.clear();
            }
        });

        if self.show_new_folder_bar {
            ui.horizontal(|ui| {
                ui.label("Folder name:");
                let resp = ui.add(
                    egui::TextEdit::singleline(&mut self.new_folder_name)
                        .desired_width(180.0)
                        .hint_text("e.g. Qwen3.8-Cyber"),
                );
                let enter = resp.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
                if (ui.button("Create & Open").clicked() || enter)
                    && !self.new_folder_name.trim().is_empty()
                {
                    self.create_subfolder(self.new_folder_name.trim().to_string());
                }
                if ui.button("Cancel").clicked() {
                    self.show_new_folder_bar = false;
                }
            });
        }

        if let Some(err) = &self.error_banner {
            ui.colored_label(Color32::from_rgb(255, 110, 110), format!("⚠ {err}"));
        }
        if let Some(note) = &self.status_note {
            ui.colored_label(Color32::from_rgb(130, 210, 255), note);
        }

        ui.separator();

        // Directory table
        let filter_lower = self.filter_text.trim().to_ascii_lowercase();
        let list_height = (ui.available_height() - 86.0).max(150.0);

        let mut navigate_target: Option<EndpointPath> = None;
        let ctrl_held = ui.input(|i| i.modifiers.ctrl || i.modifiers.command);

        ScrollArea::vertical()
            .id_salt(match self.role {
                PaneRole::Source => "source_explorer_scroll",
                PaneRole::Destination => "dest_explorer_scroll",
            })
            .max_height(list_height)
            .auto_shrink([false, false])
            .show(ui, |ui| {
                for (idx, entry) in self.entries.iter().enumerate() {
                    if hide_temp_artifacts && is_temp_artifact_name(&entry.name) {
                        continue;
                    }
                    if !filter_lower.is_empty()
                        && !entry.name.to_ascii_lowercase().contains(&filter_lower)
                    {
                        continue;
                    }

                    let is_selected = self.selected.contains(&idx);
                    ui.horizontal(|ui| {
                        let mut checked = is_selected;
                        if ui.checkbox(&mut checked, "").changed() {
                            if checked {
                                self.selected.insert(idx);
                            } else {
                                self.selected.remove(&idx);
                            }
                        }

                        let (icon, color) = if entry.is_dir {
                            ("📁", Color32::from_rgb(245, 205, 115))
                        } else if entry.is_model_weight {
                            ("🧠", Color32::from_rgb(115, 225, 255))
                        } else {
                            ("📄", Color32::from_rgb(210, 215, 222))
                        };

                        let label_text = if entry.is_dir {
                            format!("{icon} {}/", entry.name)
                        } else {
                            format!("{icon} {}   ({})", entry.name, format_bytes(entry.size))
                        };

                        let resp = ui
                            .selectable_label(is_selected, RichText::new(label_text).color(color));

                        if resp.double_clicked() && entry.is_dir {
                            navigate_target = Some(entry.path.clone());
                        } else if resp.clicked() {
                            if ctrl_held {
                                if is_selected {
                                    self.selected.remove(&idx);
                                } else {
                                    self.selected.insert(idx);
                                }
                            } else {
                                self.selected.clear();
                                self.selected.insert(idx);
                            }
                        }

                        if entry.is_dir
                            && ui
                                .small_button("Open →")
                                .on_hover_text("Enter directory (or double-click)")
                                .clicked()
                        {
                            navigate_target = Some(entry.path.clone());
                        }
                    });
                }

                if self.entries.is_empty() && !self.loading && self.error_banner.is_none() {
                    ui.weak("(Empty folder)");
                }
            });

        if let Some(target) = navigate_target {
            self.navigate_to(target);
        }

        ui.separator();

        // Pane footer: Source companion detector / summary OR Destination queue controls
        match self.role {
            PaneRole::Source => {
                self.render_source_footer(ui);
            }
            PaneRole::Destination => {
                let target_folder = self.active_destination_folder();
                ui.horizontal_wrapped(|ui| {
                    ui.label(
                        RichText::new(format!("Target folder: {target_folder}"))
                            .small()
                            .color(Color32::from_rgb(175, 225, 175)),
                    );
                });
                ui.horizontal(|ui| {
                    if ui
                        .button(
                            RichText::new("+ Add to Destination Targets")
                                .strong()
                                .color(Color32::from_rgb(140, 230, 170)),
                        )
                        .on_hover_text("Queue this folder so you can morph into 1 or multiple places simultaneously")
                        .clicked()
                    {
                        action = PaneAction::AddDestination(target_folder.clone());
                    }

                    if ui
                        .button(
                            RichText::new("⚡ Morph Here Now")
                                .strong()
                                .color(Color32::from_rgb(255, 215, 120)),
                        )
                        .on_hover_text("Immediately copy selected source(s) into this folder")
                        .clicked()
                    {
                        action = PaneAction::QuickMorphTo(target_folder);
                    }
                });
            }
        }

        action
    }

    fn render_source_footer(&mut self, ui: &mut Ui) {
        if self.selected.is_empty() {
            ui.weak("Select one or more files or model folders above (Ctrl+Click or checkboxes for multi-select).");
            return;
        }

        let count = self.selected.len();
        let file_bytes = self.selected_known_bytes();
        let dir_count = self
            .selected
            .iter()
            .filter_map(|&i| self.entries.get(i))
            .filter(|e| e.is_dir)
            .count();

        let summary = if dir_count > 0 {
            format!(
                "Selected: {count} item(s) ({dir_count} folder(s); full recursive size computed on start)"
            )
        } else {
            format!("Selected: {count} file(s) ({})", format_bytes(file_bytes))
        };
        ui.label(
            RichText::new(summary)
                .small()
                .strong()
                .color(Color32::from_rgb(130, 220, 255)),
        );

        // Offer 1-click companion `mmproj` inclusion when a single VLM GGUF is selected
        if self.selected.len() == 1
            && let Some(&sel_idx) = self.selected.iter().next()
            && let Some(sel_entry) = self.entries.get(sel_idx)
            && let Some(companion) = find_companion_mmproj(&sel_entry.name, &self.entries)
            && let Some(comp_idx) = self.entries.iter().position(|e| e.name == companion.name)
            && !self.selected.contains(&comp_idx)
        {
            ui.horizontal(|ui| {
                ui.label(
                    RichText::new(format!(
                        "💡 Vision projector found: {} ({})",
                        companion.name,
                        format_bytes(companion.size)
                    ))
                    .small()
                    .color(Color32::from_rgb(250, 210, 125)),
                );
                if ui.small_button("+ Include mmproj").clicked() {
                    self.selected.insert(comp_idx);
                }
            });
        }
    }

    fn create_subfolder(&mut self, name: String) {
        let parent = self.current_dir.clone();
        let created = parent.join(&name);
        let tx = self.tx.clone();
        self.rt.spawn(async move {
            let res = created.create_dir_all().await.map_err(|e| format!("{e:#}"));
            let _ = tx.send(ExplorerAsyncMsg::FolderCreated {
                parent,
                created,
                result: res,
            });
        });
    }

    fn spawn_native_folder_picker(&self) {
        let tx = self.tx.clone();
        let start_local = match &self.current_dir {
            EndpointPath::Local(p) => Some(p.clone()),
            EndpointPath::Ssh { .. } => None,
        };

        std::thread::spawn(move || {
            let mut dlg = rfd::FileDialog::new();
            if let Some(dir) = start_local {
                dlg = dlg.set_directory(dir);
            }
            if let Some(folder) = dlg.pick_folder() {
                let _ = tx.send(ExplorerAsyncMsg::NativePicked {
                    picked: vec![EndpointPath::Local(folder)],
                    navigate_into_first_dir: true,
                });
            }
        });
    }
}
