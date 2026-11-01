use std::path::PathBuf;
use std::sync::Arc;

use egui::{Color32, ProgressBar, RichText, ScrollArea};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::discovery::{LocalRoot, TailnetPeer, discover_local_roots, discover_tailnet_peers};
use crate::endpoint::{EndpointPath, format_bytes};
use crate::explorer::{ExplorerPane, PaneAction, PaneRole};
use crate::transfer::{
    MorphJob, MorphOptions, RepaintCallback, TransferPhase, TransferProgress, execute_morph_job,
};

const PERSIST_KEY: &str = "identimorph_persisted_state_v1";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Bookmark {
    pub label: String,
    pub endpoint: EndpointPath,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PersistedConfig {
    bookmarks: Vec<Bookmark>,
    tailnet_ssh_user: String,
    tailnet_ssh_path: String,
    tailnet_smb_share: String,
    verify_integrity: bool,
    skip_temp_artifacts: bool,
    resume_partials: bool,
    overwrite_existing: bool,
}

impl Default for PersistedConfig {
    fn default() -> Self {
        Self {
            bookmarks: Vec::new(),
            tailnet_ssh_user: "root".to_string(),
            tailnet_ssh_path: "/DATA/Ark/Models".to_string(),
            tailnet_smb_share: r"Ark\Models".to_string(),
            verify_integrity: true,
            skip_temp_artifacts: true,
            resume_partials: true,
            overwrite_existing: false,
        }
    }
}

enum SystemDiscoveryMsg {
    LocalRoots(Vec<LocalRoot>),
    TailnetResult(Result<Vec<TailnetPeer>, String>),
}

pub struct IdentiMorphApp {
    rt: Arc<Runtime>,
    source_pane: ExplorerPane,
    dest_pane: ExplorerPane,
    destination_queue: Vec<EndpointPath>,
    local_roots: Vec<LocalRoot>,
    tailnet_peers: Vec<TailnetPeer>,
    tailnet_error: Option<String>,
    tailnet_scanning: bool,
    cfg: PersistedConfig,
    options: MorphOptions,
    show_logs: bool,
    ui_toast: Option<String>,

    sys_tx: mpsc::UnboundedSender<SystemDiscoveryMsg>,
    sys_rx: mpsc::UnboundedReceiver<SystemDiscoveryMsg>,

    progress_rx: watch::Receiver<TransferProgress>,
    active_cancel: Option<CancellationToken>,
}

impl IdentiMorphApp {
    pub fn new(cc: &eframe::CreationContext<'_>, rt: Arc<Runtime>) -> Self {
        configure_visuals(&cc.egui_ctx);

        let cfg: PersistedConfig = cc
            .storage
            .and_then(|s| eframe::get_value(s, PERSIST_KEY))
            .unwrap_or_default();

        let options = MorphOptions {
            verify_integrity: cfg.verify_integrity,
            skip_temp_artifacts: cfg.skip_temp_artifacts,
            resume_partials: cfg.resume_partials,
            overwrite_existing: cfg.overwrite_existing,
        };

        let default_local = dirs::home_dir()
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."));
        let default_dest = dirs::download_dir().unwrap_or_else(|| default_local.clone());

        let handle = rt.handle().clone();
        let source_pane = ExplorerPane::new(
            PaneRole::Source,
            EndpointPath::Local(default_local),
            handle.clone(),
        );
        let dest_pane = ExplorerPane::new(
            PaneRole::Destination,
            EndpointPath::Local(default_dest),
            handle,
        );

        let (sys_tx, sys_rx) = mpsc::unbounded_channel();
        let (_ptx, progress_rx) = watch::channel(TransferProgress::default());

        let mut app = Self {
            rt,
            source_pane,
            dest_pane,
            destination_queue: Vec::new(),
            local_roots: Vec::new(),
            tailnet_peers: Vec::new(),
            tailnet_error: None,
            tailnet_scanning: false,
            cfg,
            options,
            show_logs: false,
            ui_toast: None,
            sys_tx,
            sys_rx,
            progress_rx,
            active_cancel: None,
        };

        app.trigger_local_root_scan();
        app.trigger_tailnet_scan();
        app
    }

    fn trigger_local_root_scan(&self) {
        let tx = self.sys_tx.clone();
        self.rt.spawn(async move {
            let roots = discover_local_roots().await;
            let _ = tx.send(SystemDiscoveryMsg::LocalRoots(roots));
        });
    }

    fn trigger_tailnet_scan(&mut self) {
        self.tailnet_scanning = true;
        self.tailnet_error = None;
        let tx = self.sys_tx.clone();
        self.rt.spawn(async move {
            let res = discover_tailnet_peers().await.map_err(|e| format!("{e:#}"));
            let _ = tx.send(SystemDiscoveryMsg::TailnetResult(res));
        });
    }

    fn poll_discovery(&mut self) {
        while let Ok(msg) = self.sys_rx.try_recv() {
            match msg {
                SystemDiscoveryMsg::LocalRoots(roots) => {
                    self.local_roots = roots;
                }
                SystemDiscoveryMsg::TailnetResult(res) => {
                    self.tailnet_scanning = false;
                    match res {
                        Ok(peers) => {
                            self.tailnet_peers = peers;
                            self.tailnet_error = None;
                        }
                        Err(e) => {
                            self.tailnet_error = Some(e);
                        }
                    }
                }
            }
        }
    }

    fn add_bookmark(&mut self, endpoint: EndpointPath) {
        if self.cfg.bookmarks.iter().any(|b| b.endpoint == endpoint) {
            self.ui_toast = Some(format!("Already pinned: {endpoint}"));
            return;
        }
        let label = endpoint.file_name().unwrap_or_else(|| endpoint.to_string());
        self.cfg.bookmarks.push(Bookmark { label, endpoint });
    }

    fn add_destination_to_queue(&mut self, endpoint: EndpointPath) {
        if !self.destination_queue.contains(&endpoint) {
            self.destination_queue.push(endpoint);
        }
    }

    fn start_transfer(
        &mut self,
        ctx: &egui::Context,
        explicit_destinations: Option<Vec<EndpointPath>>,
    ) {
        let sources = self.source_pane.selected_endpoints();
        if sources.is_empty() {
            self.ui_toast = Some(
                "Select at least one file or model folder in Source Explorer first.".to_string(),
            );
            return;
        }

        let destinations = match explicit_destinations {
            Some(d) => d,
            None if !self.destination_queue.is_empty() => self.destination_queue.clone(),
            None => vec![self.dest_pane.active_destination_folder()],
        };

        let job = MorphJob {
            sources,
            destinations,
            options: self.options.clone(),
        };

        let (tx, rx) = watch::channel(TransferProgress::default());
        self.progress_rx = rx;

        let cancel = CancellationToken::new();
        self.active_cancel = Some(cancel.clone());
        self.ui_toast = None;

        let egui_ctx = ctx.clone();
        let repaint: RepaintCallback = Arc::new(move || {
            egui_ctx.request_repaint();
        });

        let dest_pane_refresh_ctx = ctx.clone();
        self.rt.spawn(async move {
            let _ = execute_morph_job(job, tx, cancel, Some(repaint)).await;
            dest_pane_refresh_ctx.request_repaint();
        });
    }

    fn handle_pane_action(&mut self, ctx: &egui::Context, action: PaneAction) {
        match action {
            PaneAction::None => {}
            PaneAction::PinBookmark(ep) => self.add_bookmark(ep),
            PaneAction::AddDestination(ep) => self.add_destination_to_queue(ep),
            PaneAction::QuickMorphTo(ep) => {
                self.start_transfer(ctx, Some(vec![ep]));
            }
        }
    }
}

impl eframe::App for IdentiMorphApp {
    fn save(&mut self, storage: &mut dyn eframe::Storage) {
        self.cfg.verify_integrity = self.options.verify_integrity;
        self.cfg.skip_temp_artifacts = self.options.skip_temp_artifacts;
        self.cfg.resume_partials = self.options.resume_partials;
        self.cfg.overwrite_existing = self.options.overwrite_existing;
        eframe::set_value(storage, PERSIST_KEY, &self.cfg);
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        self.poll_discovery();
        let progress = self.progress_rx.borrow().clone();
        let is_running = progress.phase.is_active();

        // Top Header Bar
        egui::Panel::top("identimorph_header").show(ui, |ui| {
            ui.add_space(4.0);
            ui.horizontal(|ui| {
                ui.heading(
                    RichText::new("IdentiMorph")
                        .strong()
                        .color(Color32::from_rgb(120, 225, 255)),
                );
                ui.label(
                    RichText::new("id : X → Xⁿ  •  Async Tokio File & Model Transporter")
                        .color(Color32::from_rgb(175, 185, 198)),
                );

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.checkbox(&mut self.options.overwrite_existing, "Overwrite existing");
                    ui.checkbox(&mut self.options.skip_temp_artifacts, "Skip .cache / .incomplete")
                        .on_hover_text("Automatically ignore .cache, *.incomplete, *.lock, and *.identimorph.part");
                    ui.checkbox(&mut self.options.resume_partials, "Resume partials")
                        .on_hover_text("Stage writes in .identimorph.part and resume from exact byte offset on retry");
                    ui.checkbox(&mut self.options.verify_integrity, "Verify BLAKE3 / SHA-256")
                        .on_hover_text("Cryptographically verify id(x) == x after transfer");
                });
            });
            ui.add_space(4.0);
        });

        // Left Sidebar: Local Drives, Bookmarks, and Tailscale Tailnet Peers
        egui::Panel::left("identimorph_sidebar")
            .default_size(275.0)
            .min_size(235.0)
            .resizable(true)
            .show(ui, |ui| {
                ScrollArea::vertical()
                    .id_salt("sidebar_scroll")
                    .show(ui, |ui| {
                        ui.add_space(4.0);
                        ui.label(
                            RichText::new("LOCAL DRIVES & FOLDERS")
                                .small()
                                .strong()
                                .weak(),
                        );
                        for root in self.local_roots.clone() {
                            ui.horizontal(|ui| {
                                ui.label(&root.label);
                                ui.with_layout(
                                    egui::Layout::right_to_left(egui::Align::Center),
                                    |ui| {
                                        if ui.small_button("→Dst").clicked() {
                                            self.dest_pane.navigate_to(root.path.clone());
                                        }
                                        if ui.small_button("→Src").clicked() {
                                            self.source_pane.navigate_to(root.path.clone());
                                        }
                                    },
                                );
                            });
                        }

                        ui.add_space(8.0);
                        ui.separator();
                        ui.label(RichText::new("PINNED BOOKMARKS").small().strong().weak());
                        if self.cfg.bookmarks.is_empty() {
                            ui.weak("Click '★ Pin' in either explorer to save NAS/model paths.");
                        } else {
                            let mut remove_idx = None;
                            for (i, bm) in self.cfg.bookmarks.clone().iter().enumerate() {
                                ui.horizontal(|ui| {
                                    ui.label(&bm.label).on_hover_text(bm.endpoint.to_string());
                                    ui.with_layout(
                                        egui::Layout::right_to_left(egui::Align::Center),
                                        |ui| {
                                            if ui.small_button("×").clicked() {
                                                remove_idx = Some(i);
                                            }
                                            if ui.small_button("→Dst").clicked() {
                                                self.dest_pane.navigate_to(bm.endpoint.clone());
                                            }
                                            if ui.small_button("→Src").clicked() {
                                                self.source_pane.navigate_to(bm.endpoint.clone());
                                            }
                                        },
                                    );
                                });
                            }
                            if let Some(i) = remove_idx {
                                self.cfg.bookmarks.remove(i);
                            }
                        }

                        ui.add_space(8.0);
                        ui.separator();
                        ui.horizontal(|ui| {
                            ui.label(
                                RichText::new("TAILSCALE TAILNET")
                                    .small()
                                    .strong()
                                    .color(Color32::from_rgb(130, 225, 195)),
                            );
                            if self.tailnet_scanning {
                                ui.spinner();
                            } else if ui.small_button("⟳ Scan").clicked() {
                                self.trigger_tailnet_scan();
                            }
                        });

                        egui::Grid::new("tailnet_defaults_grid")
                            .num_columns(2)
                            .spacing([6.0, 4.0])
                            .show(ui, |ui| {
                                ui.small("SSH User:");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.cfg.tailnet_ssh_user)
                                        .desired_width(155.0),
                                );
                                ui.end_row();

                                ui.small("SSH Path:");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.cfg.tailnet_ssh_path)
                                        .desired_width(155.0),
                                );
                                ui.end_row();

                                ui.small("SMB Share:");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.cfg.tailnet_smb_share)
                                        .desired_width(155.0),
                                );
                                ui.end_row();
                            });

                        ui.add_space(4.0);

                        if let Some(err) = &self.tailnet_error {
                            ui.colored_label(
                                Color32::from_rgb(235, 150, 120),
                                format!("Tailscale: {err}"),
                            );
                        } else if self.tailnet_peers.is_empty() && !self.tailnet_scanning {
                            ui.weak("No Tailnet peers detected.");
                        }

                        for peer in self.tailnet_peers.clone() {
                            egui::Frame::group(ui.style())
                                .inner_margin(6.0)
                                .show(ui, |ui| {
                                    ui.horizontal(|ui| {
                                        let dot_color = if peer.online {
                                            Color32::from_rgb(90, 230, 140)
                                        } else {
                                            Color32::from_rgb(130, 130, 140)
                                        };
                                        ui.colored_label(dot_color, "●");
                                        ui.strong(peer.display_label());
                                        if !peer.os.is_empty() {
                                            ui.weak(format!("({})", peer.os));
                                        }
                                    });

                                    if let Some(ip) = &peer.ipv4 {
                                        ui.small(format!(
                                            "IP: {ip}{}",
                                            if peer.tailscale_ssh {
                                                " • Tailscale SSH"
                                            } else {
                                                ""
                                            }
                                        ));
                                    }

                                    ui.horizontal_wrapped(|ui| {
                                        let ssh_ep = peer.ssh_endpoint(
                                            &self.cfg.tailnet_ssh_user,
                                            &self.cfg.tailnet_ssh_path,
                                        );
                                        if ui
                                            .small_button("SSH→Src")
                                            .on_hover_text(ssh_ep.to_string())
                                            .clicked()
                                        {
                                            self.source_pane.navigate_to(ssh_ep.clone());
                                        }
                                        if ui
                                            .small_button("SSH→Dst")
                                            .on_hover_text(ssh_ep.to_string())
                                            .clicked()
                                        {
                                            self.dest_pane.navigate_to(ssh_ep);
                                        }

                                        let smb_ep = peer.smb_endpoint(&self.cfg.tailnet_smb_share);
                                        if ui
                                            .small_button("SMB→Src")
                                            .on_hover_text(smb_ep.to_string())
                                            .clicked()
                                        {
                                            self.source_pane.navigate_to(smb_ep.clone());
                                        }
                                        if ui
                                            .small_button("SMB→Dst")
                                            .on_hover_text(smb_ep.to_string())
                                            .clicked()
                                        {
                                            self.dest_pane.navigate_to(smb_ep);
                                        }
                                    });
                                });
                        }
                    });
            });

        // Bottom Dock: Multi-Destination Queue + Tokio Execution & Telemetry
        egui::Panel::bottom("identimorph_transfer_dock")
            .resizable(true)
            .min_size(165.0)
            .default_size(215.0)
            .show(ui, |ui| {
                ui.add_space(4.0);

                // Row 1: Destination fan-out queue
                ui.horizontal_wrapped(|ui| {
                    ui.strong("Target Destinations (Xⁿ):");
                    if self.destination_queue.is_empty() {
                        let fallback = self.dest_pane.active_destination_folder();
                        ui.label(
                            RichText::new(format!(
                                "Direct mode → {fallback}  (use '+ Add to Destination Targets' to fan out to multiple folders/hosts)"
                            ))
                            .color(Color32::from_rgb(170, 200, 180)),
                        );
                    } else {
                        let mut remove_idx = None;
                        for (idx, dst) in self.destination_queue.iter().enumerate() {
                            ui.group(|ui| {
                                ui.small(format!("#{} {}", idx + 1, dst));
                                if !is_running && ui.small_button("×").clicked() {
                                    remove_idx = Some(idx);
                                }
                            });
                        }
                        if let Some(i) = remove_idx {
                            self.destination_queue.remove(i);
                        }
                        if !is_running && ui.small_button("Clear Queue").clicked() {
                            self.destination_queue.clear();
                        }
                    }
                });

                ui.add_space(4.0);

                // Row 2: Execute / Cancel controls + overall progress bar
                ui.horizontal(|ui| {
                    let can_start = !is_running && !self.source_pane.selected.is_empty();
                    let target_count = self.destination_queue.len().max(1);
                    let btn_label = format!(
                        "▶ Execute Identity Morphism ({} source → {} target{})",
                        self.source_pane.selected.len(),
                        target_count,
                        if target_count == 1 { "" } else { "s" }
                    );

                    if ui
                        .add_enabled(
                            can_start,
                            egui::Button::new(
                                RichText::new(btn_label)
                                    .strong()
                                    .color(Color32::from_rgb(20, 30, 25)),
                            )
                            .fill(Color32::from_rgb(95, 225, 160)),
                        )
                        .clicked()
                    {
                        self.start_transfer(&ctx, None);
                    }

                    if is_running
                        && ui
                            .add(
                                egui::Button::new(
                                    RichText::new("⏹ Cancel")
                                        .strong()
                                        .color(Color32::WHITE),
                                )
                                .fill(Color32::from_rgb(205, 70, 70)),
                            )
                            .clicked()
                        && let Some(token) = &self.active_cancel
                    {
                        token.cancel();
                    }

                    if matches!(progress.phase, TransferPhase::Completed)
                        && ui.button("⟳ Refresh Destination Explorer").clicked()
                    {
                        self.dest_pane.refresh();
                    }

                    ui.checkbox(&mut self.show_logs, "Activity Log");

                    if let Some(toast) = &self.ui_toast {
                        ui.colored_label(Color32::from_rgb(255, 195, 110), toast);
                    }
                });

                // Overall progress bar & speed/ETA
                let fraction = if progress.total_bytes > 0 {
                    (progress.bytes_done as f32 / progress.total_bytes as f32).clamp(0.0, 1.0)
                } else if matches!(progress.phase, TransferPhase::Completed) {
                    1.0
                } else {
                    0.0
                };

                let phase_str = match &progress.phase {
                    TransferPhase::Idle => "Ready".to_string(),
                    TransferPhase::Scanning => "Scanning source tree...".to_string(),
                    TransferPhase::Streaming => format!(
                        "Streaming {} ({}/{}) • {}/s{}",
                        progress.current_file,
                        progress.files_completed,
                        progress.total_files,
                        format_bytes(progress.throughput_bps as u64),
                        progress
                            .eta_secs
                            .map(|s| format!(" • ETA {}", format_eta(s)))
                            .unwrap_or_default()
                    ),
                    TransferPhase::Verifying => {
                        format!("Verifying cryptographic identity on {}...", progress.current_file)
                    }
                    TransferPhase::Completed => format!(
                        "Completed ✓ — {} ({} files) @ {}/s",
                        format_bytes(progress.total_bytes),
                        progress.total_files,
                        format_bytes(progress.throughput_bps as u64)
                    ),
                    TransferPhase::Cancelled => "Cancelled (partials saved)".to_string(),
                    TransferPhase::Failed(err) => format!("Failed: {err}"),
                };

                ui.add(
                    ProgressBar::new(fraction)
                        .show_percentage()
                        .text(format!(
                            "{} / {}  —  {}",
                            format_bytes(progress.bytes_done),
                            format_bytes(progress.total_bytes),
                            phase_str
                        )),
                );

                // Per-destination live status table
                ScrollArea::vertical()
                    .id_salt("dest_telemetry_scroll")
                    .max_height(if self.show_logs { 55.0 } else { 95.0 })
                    .show(ui, |ui| {
                        for (i, d) in progress.destinations.iter().enumerate() {
                            let d_frac = if progress.total_bytes > 0 {
                                (d.bytes_written as f32 / progress.total_bytes as f32)
                                    .clamp(0.0, 1.0)
                            } else {
                                0.0
                            };
                            ui.horizontal(|ui| {
                                ui.small(format!("Target #{}: {}", i + 1, d.destination));
                                ui.add_sized(
                                    [140.0, 14.0],
                                    ProgressBar::new(d_frac).show_percentage(),
                                );
                                ui.small(&d.status);
                            });
                        }
                    });

                if self.show_logs && !progress.log_lines.is_empty() {
                    ui.separator();
                    ScrollArea::vertical()
                        .id_salt("activity_log_scroll")
                        .stick_to_bottom(true)
                        .max_height(70.0)
                        .show(ui, |ui| {
                            for line in &progress.log_lines {
                                ui.monospace(RichText::new(line).small());
                            }
                        });
                }
            });

        // Main Split Dual-Pane Explorer
        egui::CentralPanel::default().show(ui, |ui| {
            let skip_temp = self.options.skip_temp_artifacts;
            let mut src_action = PaneAction::None;
            let mut dst_action = PaneAction::None;

            ui.columns(2, |cols| {
                src_action = self.source_pane.ui(&mut cols[0], skip_temp);
                dst_action = self.dest_pane.ui(&mut cols[1], skip_temp);
            });

            self.handle_pane_action(&ctx, src_action);
            self.handle_pane_action(&ctx, dst_action);
        });
    }
}

fn configure_visuals(ctx: &egui::Context) {
    let mut visuals = egui::Visuals::dark();
    visuals.widgets.noninteractive.bg_fill = Color32::from_rgb(22, 25, 31);
    visuals.panel_fill = Color32::from_rgb(24, 27, 34);
    visuals.extreme_bg_color = Color32::from_rgb(16, 18, 23);
    ctx.set_visuals(visuals);
}

fn format_eta(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        format!("{secs}s")
    }
}
