#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::sync::Arc;

use identimorph::app::IdentiMorphApp;
use tokio::runtime::Builder;

fn main() -> eframe::Result {
    let rt = Arc::new(
        Builder::new_multi_thread()
            .enable_all()
            .thread_name("identimorph-tokio")
            .build()
            .expect("Failed to initialize Tokio multi-threaded runtime"),
    );

    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("IdentiMorph — id: X → Xⁿ (Async File & Model Transporter)")
            .with_inner_size([1340.0, 820.0])
            .with_min_inner_size([920.0, 600.0]),
        ..Default::default()
    };

    eframe::run_native(
        "IdentiMorph",
        native_options,
        Box::new(move |cc| Ok(Box::new(IdentiMorphApp::new(cc, rt.clone())))),
    )
}
