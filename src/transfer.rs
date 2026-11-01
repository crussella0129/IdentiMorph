use std::io::SeekFrom;
use std::path::Path;
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use futures_util::future::try_join_all;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::endpoint::{
    EndpointPath, RelativeFileSpec, configure_background_command, format_bytes, shell_quote,
};

/// 4 MiB aligned buffer size tuned for multi-gigabyte `.safetensors` and `.gguf` transfers.
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct MorphOptions {
    pub verify_integrity: bool,
    pub skip_temp_artifacts: bool,
    pub resume_partials: bool,
    pub overwrite_existing: bool,
}

impl Default for MorphOptions {
    fn default() -> Self {
        Self {
            verify_integrity: true,
            skip_temp_artifacts: true,
            resume_partials: true,
            overwrite_existing: false,
        }
    }
}

#[derive(Clone, Debug)]
pub struct MorphJob {
    pub sources: Vec<EndpointPath>,
    pub destinations: Vec<EndpointPath>,
    pub options: MorphOptions,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferPhase {
    Idle,
    Scanning,
    Streaming,
    Verifying,
    Completed,
    Cancelled,
    Failed(String),
}

impl TransferPhase {
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Scanning | Self::Streaming | Self::Verifying)
    }
}

#[derive(Clone, Debug)]
pub struct DestinationProgress {
    pub destination: EndpointPath,
    pub bytes_written: u64,
    pub resumed_bytes: u64,
    pub status: String,
    pub last_digest: Option<String>,
}

#[derive(Clone, Debug)]
pub struct TransferProgress {
    pub phase: TransferPhase,
    pub current_file: String,
    pub files_completed: usize,
    pub total_files: usize,
    pub bytes_done: u64,
    pub total_bytes: u64,
    pub throughput_bps: f64,
    pub eta_secs: Option<u64>,
    pub destinations: Vec<DestinationProgress>,
    pub log_lines: Vec<String>,
}

impl Default for TransferProgress {
    fn default() -> Self {
        Self {
            phase: TransferPhase::Idle,
            current_file: String::new(),
            files_completed: 0,
            total_files: 0,
            bytes_done: 0,
            total_bytes: 0,
            throughput_bps: 0.0,
            eta_secs: None,
            destinations: Vec::new(),
            log_lines: Vec::new(),
        }
    }
}

pub type RepaintCallback = Arc<dyn Fn() + Send + Sync>;

/// Executes the identity morphism fan-out $\Delta_n : X \to X^n$ across all destinations.
pub async fn execute_morph_job(
    job: MorphJob,
    progress_tx: watch::Sender<TransferProgress>,
    cancel: CancellationToken,
    repaint: Option<RepaintCallback>,
) -> Result<()> {
    if job.sources.is_empty() {
        bail!("Select at least one source file or folder");
    }
    if job.destinations.is_empty() {
        bail!("Add at least one destination folder");
    }

    let mut state = TransferProgress {
        phase: TransferPhase::Scanning,
        destinations: job
            .destinations
            .iter()
            .map(|d| DestinationProgress {
                destination: d.clone(),
                bytes_written: 0,
                resumed_bytes: 0,
                status: "Waiting...".to_string(),
                last_digest: None,
            })
            .collect(),
        ..Default::default()
    };
    push_log(
        &mut state,
        format!(
            "Scanning {} source item(s) for {} destination(s)...",
            job.sources.len(),
            job.destinations.len()
        ),
    );
    emit(&progress_tx, &state, &repaint);

    let result = run_job_inner(&job, &mut state, &progress_tx, &cancel, &repaint).await;

    match result {
        Ok(()) => {
            if cancel.is_cancelled() {
                state.phase = TransferPhase::Cancelled;
                push_log(&mut state, "Transfer cancelled by user (partial .identimorph.part files preserved for resume).".to_string());
            } else {
                state.phase = TransferPhase::Completed;
                state.eta_secs = Some(0);
                let done_msg = format!(
                    "Identity morphism complete: {} file(s) ({}) replicated across {} target(s).",
                    state.total_files,
                    format_bytes(state.total_bytes),
                    job.destinations.len()
                );
                push_log(&mut state, done_msg);
            }
            emit(&progress_tx, &state, &repaint);
            Ok(())
        }
        Err(err) => {
            if cancel.is_cancelled() {
                state.phase = TransferPhase::Cancelled;
                push_log(&mut state, "Transfer cancelled.".to_string());
                emit(&progress_tx, &state, &repaint);
                Ok(())
            } else {
                let msg = format!("{err:#}");
                state.phase = TransferPhase::Failed(msg.clone());
                push_log(&mut state, format!("ERROR: {msg}"));
                emit(&progress_tx, &state, &repaint);
                Err(err)
            }
        }
    }
}

async fn run_job_inner(
    job: &MorphJob,
    state: &mut TransferProgress,
    progress_tx: &watch::Sender<TransferProgress>,
    cancel: &CancellationToken,
    repaint: &Option<RepaintCallback>,
) -> Result<()> {
    // 1. Enumerate all source files in deterministic order.
    let mut all_specs: Vec<RelativeFileSpec> = Vec::new();
    for src in &job.sources {
        if cancel.is_cancelled() {
            return Ok(());
        }
        for dst in &job.destinations {
            if src == dst {
                bail!("Source and destination cannot be identical ({src})");
            }
        }
        let mut specs = src.walk_files(job.options.skip_temp_artifacts).await?;
        all_specs.append(&mut specs);
    }

    if all_specs.is_empty() {
        bail!("No transferable files found in selected source(s)");
    }

    state.total_files = all_specs.len();
    state.total_bytes = all_specs.iter().map(|s| s.size).sum();
    state.phase = TransferPhase::Streaming;
    push_log(
        state,
        format!(
            "Discovered {} file(s), total payload {}.",
            state.total_files,
            format_bytes(state.total_bytes)
        ),
    );
    emit(progress_tx, state, repaint);

    // Ensure root destination directories exist.
    for dst in &job.destinations {
        dst.create_dir_all().await?;
    }

    let start_instant = Instant::now();
    let mut live_bytes_transferred: u64 = 0;
    let mut last_ui_emit = Instant::now();

    for spec in &all_specs {
        if cancel.is_cancelled() {
            return Ok(());
        }

        state.current_file = spec.rel_path.clone();
        state.phase = TransferPhase::Streaming;
        emit(progress_tx, state, repaint);

        // Prepare each destination sink (checking skip-if-complete or partial-resume offset).
        let mut sinks: Vec<Option<DestSink>> = Vec::with_capacity(job.destinations.len());
        let mut active_offsets: Vec<u64> = Vec::new();

        for (idx, dst_root) in job.destinations.iter().enumerate() {
            let final_path = dst_root.join(&spec.rel_path);
            let part_path = dst_root.join(&format!("{}.identimorph.part", spec.rel_path));

            if !job.options.overwrite_existing
                && let Some(st) = final_path.stat().await?
                && !st.is_dir
                && st.size == spec.size
            {
                state.destinations[idx].bytes_written += spec.size;
                state.destinations[idx].status = format!("Up-to-date ({})", spec.rel_path);
                sinks.push(None);
                continue;
            }

            let existing_part = if job.options.resume_partials {
                match part_path.stat().await? {
                    Some(st) if !st.is_dir && st.size < spec.size => st.size,
                    _ => 0,
                }
            } else {
                0
            };

            let sink = DestSink::open(idx, final_path, part_path, existing_part).await?;
            if existing_part > 0 {
                state.destinations[idx].bytes_written += existing_part;
                state.destinations[idx].resumed_bytes += existing_part;
                state.destinations[idx].status =
                    format!("Resuming +{}", format_bytes(existing_part));
                push_log(
                    state,
                    format!(
                        "Resuming {} on {} from {}",
                        spec.rel_path,
                        dst_root,
                        format_bytes(existing_part)
                    ),
                );
            } else {
                state.destinations[idx].status = format!("Streaming {}", spec.rel_path);
            }

            active_offsets.push(existing_part);
            sinks.push(Some(sink));
        }

        // If every destination already had an identical-sized complete file, advance counters.
        if active_offsets.is_empty() {
            state.bytes_done += spec.size;
            state.files_completed += 1;
            emit(progress_tx, state, repaint);
            continue;
        }

        let min_offset = active_offsets.into_iter().min().unwrap_or(0);
        state.bytes_done += min_offset;
        emit(progress_tx, state, repaint);

        let any_ssh_involved =
            spec.source_path.is_ssh() || sinks.iter().flatten().any(|s| s.final_path.is_ssh());

        // When starting at byte 0, compute BLAKE3 (and SHA-256 if SSH is involved) on the fly!
        let compute_inline_hash = job.options.verify_integrity && min_offset == 0;
        let mut blake3_hasher = blake3::Hasher::new();
        let mut sha256_hasher = Sha256::new();

        let mut reader = SourceStream::open(&spec.source_path, min_offset).await?;
        let mut file_offset = min_offset;
        let mut buf = vec![0u8; CHUNK_SIZE];

        loop {
            let read_res = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    reader.abort().await;
                    for s in sinks.into_iter().flatten() {
                        s.abort().await;
                    }
                    return Ok(());
                }
                r = reader.read(&mut buf) => r,
            };

            let n = read_res
                .with_context(|| format!("Read error while streaming {}", spec.source_path))?;
            if n == 0 {
                break;
            }

            let chunk = Bytes::copy_from_slice(&buf[..n]);
            if compute_inline_hash {
                blake3_hasher.update(&chunk);
                if any_ssh_involved {
                    sha256_hasher.update(&chunk);
                }
            }

            // Fan-out chunk concurrently to all active destinations.
            let write_futs = sinks
                .iter_mut()
                .flatten()
                .map(|sink| sink.write_at(file_offset, chunk.clone()));

            let per_dest_written = tokio::select! {
                biased;
                _ = cancel.cancelled() => {
                    reader.abort().await;
                    for s in sinks.into_iter().flatten() {
                        s.abort().await;
                    }
                    return Ok(());
                }
                res = try_join_all(write_futs) => res?,
            };

            for (dst_idx, newly_written) in per_dest_written {
                state.destinations[dst_idx].bytes_written += newly_written;
            }

            file_offset += n as u64;
            state.bytes_done += n as u64;
            live_bytes_transferred += n as u64;

            if last_ui_emit.elapsed() >= Duration::from_millis(45) {
                let elapsed = start_instant.elapsed().as_secs_f64().max(0.001);
                state.throughput_bps = live_bytes_transferred as f64 / elapsed;
                let remaining = state.total_bytes.saturating_sub(state.bytes_done);
                state.eta_secs = if state.throughput_bps > 1.0 {
                    Some((remaining as f64 / state.throughput_bps).ceil() as u64)
                } else {
                    None
                };
                emit(progress_tx, state, repaint);
                last_ui_emit = Instant::now();
            }
        }

        reader.finish().await?;

        if file_offset != spec.size {
            bail!(
                "Size mismatch on {}: expected {} bytes, read {} bytes",
                spec.source_path,
                spec.size,
                file_offset
            );
        }

        // Flush, close, and atomically rename `.identimorph.part` -> final filename.
        let finalize_futs = sinks.into_iter().flatten().map(|sink| sink.finalize());
        let finalized_targets = try_join_all(finalize_futs).await?;

        // Optional Cryptographic Identity Verification: id(x) == x
        if job.options.verify_integrity {
            state.phase = TransferPhase::Verifying;
            for (dst_idx, _) in &finalized_targets {
                state.destinations[*dst_idx].status = format!("Verifying {}...", spec.rel_path);
            }
            emit(progress_tx, state, repaint);

            let inline_b3 =
                compute_inline_hash.then(|| blake3_hasher.finalize().to_hex().to_string());
            let inline_sha = (compute_inline_hash && any_ssh_involved)
                .then(|| hex_encode(&sha256_hasher.finalize()));

            verify_replicated_files(
                &spec.source_path,
                &finalized_targets,
                inline_b3,
                inline_sha,
                state,
                cancel,
            )
            .await?;
        } else {
            for (dst_idx, _) in &finalized_targets {
                state.destinations[*dst_idx].status = format!("Synced {}", spec.rel_path);
            }
        }

        state.files_completed += 1;
        let elapsed = start_instant.elapsed().as_secs_f64().max(0.001);
        state.throughput_bps = live_bytes_transferred as f64 / elapsed;
        emit(progress_tx, state, repaint);
    }

    for d in &mut state.destinations {
        if let Some(dig) = &d.last_digest {
            d.status = format!("Verified ✓ ({dig})");
        } else {
            d.status = "Completed ✓".to_string();
        }
    }

    Ok(())
}

async fn verify_replicated_files(
    source_path: &EndpointPath,
    targets: &[(usize, EndpointPath)],
    inline_blake3: Option<String>,
    inline_sha256: Option<String>,
    state: &mut TransferProgress,
    cancel: &CancellationToken,
) -> Result<()> {
    let mut cached_src_blake3 = inline_blake3;
    let mut cached_src_sha256 = inline_sha256;

    for (dst_idx, final_dst) in targets {
        if cancel.is_cancelled() {
            return Ok(());
        }

        if !source_path.is_ssh() && !final_dst.is_ssh() {
            // Fast SIMD BLAKE3 verification on both local/SMB endpoints.
            let src_b3 = match &cached_src_blake3 {
                Some(h) => h.clone(),
                None => {
                    let h = compute_local_blake3(source_path).await?;
                    cached_src_blake3 = Some(h.clone());
                    h
                }
            };
            let dst_b3 = compute_local_blake3(final_dst).await?;
            if src_b3 != dst_b3 {
                bail!(
                    "BLAKE3 identity check failed for {final_dst}: source={src_b3}, dest={dst_b3}"
                );
            }
            let badge = format!("blake3:{}", &dst_b3[..12]);
            state.destinations[*dst_idx].last_digest = Some(badge.clone());
            state.destinations[*dst_idx].status = format!("Verified ✓ ({badge})");
        } else {
            // Cross-SSH verification via SHA-256 (universally available via sha256sum / shasum on POSIX).
            let src_sha = match &cached_src_sha256 {
                Some(h) => h.clone(),
                None => {
                    let h = compute_any_sha256(source_path).await?;
                    cached_src_sha256 = Some(h.clone());
                    h
                }
            };
            let dst_sha = compute_any_sha256(final_dst).await?;
            if src_sha != dst_sha {
                bail!(
                    "SHA-256 identity check failed for {final_dst}: source={src_sha}, dest={dst_sha}"
                );
            }
            let badge = format!("sha256:{}", &dst_sha[..12]);
            state.destinations[*dst_idx].last_digest = Some(badge.clone());
            state.destinations[*dst_idx].status = format!("Verified ✓ ({badge})");
        }
    }
    Ok(())
}

async fn compute_local_blake3(ep: &EndpointPath) -> Result<String> {
    let EndpointPath::Local(path) = ep else {
        bail!("compute_local_blake3 requires a local/UNC path");
    };
    let p = path.clone();
    tokio::task::spawn_blocking(move || -> Result<String> {
        let mut file = std::fs::File::open(&p)
            .with_context(|| format!("Failed to open {} for BLAKE3 verification", p.display()))?;
        let mut hasher = blake3::Hasher::new();
        let mut buf = vec![0u8; 1024 * 1024];
        loop {
            let n = std::io::Read::read(&mut file, &mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        Ok(hasher.finalize().to_hex().to_string())
    })
    .await
    .context("BLAKE3 hashing task panicked")?
}

async fn compute_any_sha256(ep: &EndpointPath) -> Result<String> {
    match ep {
        EndpointPath::Local(path) => {
            let p = path.clone();
            tokio::task::spawn_blocking(move || -> Result<String> {
                let mut file = std::fs::File::open(&p).with_context(|| {
                    format!("Failed to open {} for SHA-256 verification", p.display())
                })?;
                let mut hasher = Sha256::new();
                let mut buf = vec![0u8; 1024 * 1024];
                loop {
                    let n = std::io::Read::read(&mut file, &mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                }
                Ok(hex_encode(&hasher.finalize()))
            })
            .await
            .context("SHA-256 hashing task panicked")?
        }
        EndpointPath::Ssh { path, .. } => {
            let q = shell_quote(path);
            let script = format!(
                "if command -v sha256sum >/dev/null 2>&1; then sha256sum -- {q}; else shasum -a 256 -- {q}; fi"
            );
            let out = ep.run_ssh_command(&script).await?;
            let digest = out
                .split_whitespace()
                .next()
                .ok_or_else(|| anyhow!("Empty SHA-256 output from {ep}"))?
                .to_ascii_lowercase();
            Ok(digest)
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(&mut s, "{b:02x}");
    }
    s
}

enum SourceStream {
    Local(tokio::fs::File),
    Ssh {
        child: Box<Child>,
        stdout: tokio::process::ChildStdout,
    },
}

impl SourceStream {
    async fn open(source: &EndpointPath, start_offset: u64) -> Result<Self> {
        match source {
            EndpointPath::Local(path) => {
                let mut f = tokio::fs::File::open(path)
                    .await
                    .with_context(|| format!("Failed to open source file {}", path.display()))?;
                if start_offset > 0 {
                    f.seek(SeekFrom::Start(start_offset)).await?;
                }
                Ok(Self::Local(f))
            }
            EndpointPath::Ssh { path, .. } => {
                let (target, mut args) = source
                    .ssh_target_and_args()
                    .ok_or_else(|| anyhow!("Invalid SSH source"))?;
                let q = shell_quote(path);
                let remote_cmd = if start_offset == 0 {
                    format!("cat -- {q}")
                } else {
                    let one_based = start_offset + 1;
                    format!("tail -c +{one_based} -- {q}")
                };
                args.push(target.clone());
                args.push(remote_cmd);

                let mut child = configure_background_command(Command::new("ssh"))
                    .args(&args)
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .kill_on_drop(true)
                    .spawn()
                    .with_context(|| format!("Failed to start SSH read stream from {target}"))?;

                let stdout = child
                    .stdout
                    .take()
                    .ok_or_else(|| anyhow!("Missing stdout pipe on SSH reader"))?;
                Ok(Self::Ssh {
                    child: Box::new(child),
                    stdout,
                })
            }
        }
    }

    async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Local(f) => AsyncReadExt::read(f, buf).await,
            Self::Ssh { stdout, .. } => poll_read(stdout, buf).await,
        }
    }

    async fn finish(self) -> Result<()> {
        match self {
            Self::Local(_) => Ok(()),
            Self::Ssh { child, .. } => {
                let out = child.wait_with_output().await?;
                if !out.status.success() {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    bail!("Remote SSH read exited with error: {}", stderr.trim());
                }
                Ok(())
            }
        }
    }

    async fn abort(mut self) {
        if let Self::Ssh { child, .. } = &mut self {
            let _ = child.kill().await;
        }
    }
}

async fn poll_read<R: AsyncRead + Unpin>(reader: &mut R, buf: &mut [u8]) -> std::io::Result<usize> {
    reader.read(buf).await
}

struct DestSink {
    dest_idx: usize,
    final_path: EndpointPath,
    part_path: EndpointPath,
    existing_offset: u64,
    writer: DestWriterKind,
}

enum DestWriterKind {
    Local(tokio::fs::File),
    Ssh {
        child: Box<Child>,
        stdin: Option<ChildStdin>,
    },
}

impl DestSink {
    async fn open(
        dest_idx: usize,
        final_path: EndpointPath,
        part_path: EndpointPath,
        existing_offset: u64,
    ) -> Result<Self> {
        if let Some(parent) = part_path.parent() {
            parent.create_dir_all().await?;
        }

        let writer = match &part_path {
            EndpointPath::Local(p) => {
                let file = if existing_offset > 0 {
                    tokio::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(p)
                        .await
                        .with_context(|| format!("Failed to open {} for append", p.display()))?
                } else {
                    tokio::fs::File::create(p)
                        .await
                        .with_context(|| format!("Failed to create {}", p.display()))?
                };
                DestWriterKind::Local(file)
            }
            EndpointPath::Ssh { path, .. } => {
                let (target, mut args) = part_path
                    .ssh_target_and_args()
                    .ok_or_else(|| anyhow!("Invalid SSH destination"))?;
                let q = shell_quote(path);
                let redir = if existing_offset > 0 { ">>" } else { ">" };
                let remote_cmd = format!("cat {redir} {q}");
                args.push(target.clone());
                args.push(remote_cmd);

                let mut child = configure_background_command(Command::new("ssh"))
                    .args(&args)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::null())
                    .stderr(Stdio::piped())
                    .kill_on_drop(true)
                    .spawn()
                    .with_context(|| format!("Failed to start SSH write stream to {target}"))?;

                let stdin = child
                    .stdin
                    .take()
                    .ok_or_else(|| anyhow!("Missing stdin pipe on SSH writer"))?;
                DestWriterKind::Ssh {
                    child: Box::new(child),
                    stdin: Some(stdin),
                }
            }
        };

        Ok(Self {
            dest_idx,
            final_path,
            part_path,
            existing_offset,
            writer,
        })
    }

    /// Writes any portion of `chunk` beyond `self.existing_offset` and returns `(dest_idx, newly_written_bytes)`.
    async fn write_at(&mut self, chunk_offset: u64, chunk: Bytes) -> Result<(usize, u64)> {
        let chunk_end = chunk_offset + chunk.len() as u64;
        if chunk_end <= self.existing_offset {
            return Ok((self.dest_idx, 0));
        }

        let slice_start = self.existing_offset.saturating_sub(chunk_offset) as usize;
        let data = &chunk[slice_start..];

        match &mut self.writer {
            DestWriterKind::Local(f) => {
                f.write_all(data)
                    .await
                    .with_context(|| format!("Write error on destination {}", self.part_path))?;
            }
            DestWriterKind::Ssh { stdin, .. } => {
                let pipe = stdin
                    .as_mut()
                    .ok_or_else(|| anyhow!("SSH writer stdin closed unexpectedly"))?;
                pipe.write_all(data)
                    .await
                    .with_context(|| format!("SSH stream write error on {}", self.part_path))?;
            }
        }

        Ok((self.dest_idx, data.len() as u64))
    }

    async fn finalize(self) -> Result<(usize, EndpointPath)> {
        match self.writer {
            DestWriterKind::Local(mut f) => {
                f.flush().await?;
                drop(f);
                let (EndpointPath::Local(part_p), EndpointPath::Local(final_p)) =
                    (&self.part_path, &self.final_path)
                else {
                    unreachable!();
                };
                promote_local_part(part_p, final_p).await?;
            }
            DestWriterKind::Ssh { child, mut stdin } => {
                if let Some(mut pipe) = stdin.take() {
                    pipe.flush().await?;
                    drop(pipe);
                }
                let out = child.wait_with_output().await?;
                if !out.status.success() {
                    let stderr = String::from_utf8_lossy(&out.stderr);
                    bail!(
                        "Remote SSH write failed for {}: {}",
                        self.part_path,
                        stderr.trim()
                    );
                }
                let EndpointPath::Ssh { path: part_p, .. } = &self.part_path else {
                    unreachable!();
                };
                let EndpointPath::Ssh { path: final_p, .. } = &self.final_path else {
                    unreachable!();
                };
                let mv_cmd = format!("mv -f -- {} {}", shell_quote(part_p), shell_quote(final_p));
                self.final_path.run_ssh_command(&mv_cmd).await?;
            }
        }

        Ok((self.dest_idx, self.final_path))
    }

    async fn abort(self) {
        if let DestWriterKind::Ssh {
            mut child,
            mut stdin,
        } = self.writer
        {
            let _ = stdin.take();
            let _ = child.kill().await;
        }
    }
}

async fn promote_local_part(part_p: &Path, final_p: &Path) -> Result<()> {
    if tokio::fs::try_exists(final_p).await.unwrap_or(false) {
        let _ = tokio::fs::remove_file(final_p).await;
    }
    tokio::fs::rename(part_p, final_p).await.with_context(|| {
        format!(
            "Failed to promote staging file {} -> {}",
            part_p.display(),
            final_p.display()
        )
    })
}

fn push_log(state: &mut TransferProgress, line: String) {
    state.log_lines.push(line);
    if state.log_lines.len() > 200 {
        let excess = state.log_lines.len() - 200;
        state.log_lines.drain(0..excess);
    }
}

fn emit(
    tx: &watch::Sender<TransferProgress>,
    state: &TransferProgress,
    repaint: &Option<RepaintCallback>,
) {
    let _ = tx.send(state.clone());
    if let Some(cb) = repaint {
        cb();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn fans_out_directory_and_verifies_blake3_across_multiple_destinations() {
        let src_root = tempdir().unwrap();
        let model_dir = src_root.path().join("MyTestModel");
        std::fs::create_dir_all(model_dir.join(".cache")).unwrap();
        std::fs::create_dir_all(model_dir.join("weights v2")).unwrap();

        // Valid files
        let shard1 = vec![0x42u8; 512 * 1024];
        let cfg = br#"{"architectures":["Qwen3_5ForConditionalGeneration"]}"#;
        std::fs::write(
            model_dir.join("weights v2").join("model-00001.safetensors"),
            &shard1,
        )
        .unwrap();
        std::fs::write(model_dir.join("config.json"), cfg).unwrap();

        // Junk artifacts that must be skipped automatically
        std::fs::write(model_dir.join(".cache").join("junk.incomplete"), b"partial").unwrap();
        std::fs::write(model_dir.join("orphan.incomplete"), b"broken").unwrap();

        let dst1 = tempdir().unwrap();
        let dst2 = tempdir().unwrap();
        let dst3 = tempdir().unwrap();

        let job = MorphJob {
            sources: vec![EndpointPath::Local(model_dir.clone())],
            destinations: vec![
                EndpointPath::Local(dst1.path().to_path_buf()),
                EndpointPath::Local(dst2.path().to_path_buf()),
                EndpointPath::Local(dst3.path().to_path_buf()),
            ],
            options: MorphOptions::default(),
        };

        let (tx, rx) = watch::channel(TransferProgress::default());
        let cancel = CancellationToken::new();

        execute_morph_job(job, tx, cancel, None).await.unwrap();

        let final_state = rx.borrow().clone();
        assert_eq!(final_state.phase, TransferPhase::Completed);
        assert_eq!(final_state.files_completed, 2);

        for dst in [&dst1, &dst2, &dst3] {
            let out_dir = dst.path().join("MyTestModel");
            assert_eq!(
                std::fs::read(out_dir.join("weights v2").join("model-00001.safetensors")).unwrap(),
                shard1
            );
            assert_eq!(std::fs::read(out_dir.join("config.json")).unwrap(), cfg);
            assert!(!out_dir.join(".cache").exists());
            assert!(!out_dir.join("orphan.incomplete").exists());
        }
    }

    #[tokio::test]
    async fn resumes_partial_identimorph_part_file_and_verifies_blake3() {
        let src_dir = tempdir().unwrap();
        let dst_dir = tempdir().unwrap();

        let mut payload = Vec::with_capacity(900_000);
        for i in 0..900_000u32 {
            payload.push((i % 251) as u8);
        }

        let src_file = src_dir.path().join("Newq-Q8_0.gguf");
        std::fs::write(&src_file, &payload).unwrap();

        // Pre-seed a 400 KB partial `.identimorph.part` file in destination
        let part_file = dst_dir.path().join("Newq-Q8_0.gguf.identimorph.part");
        std::fs::write(&part_file, &payload[..400_000]).unwrap();

        let job = MorphJob {
            sources: vec![EndpointPath::Local(src_file)],
            destinations: vec![EndpointPath::Local(dst_dir.path().to_path_buf())],
            options: MorphOptions::default(),
        };

        let (tx, rx) = watch::channel(TransferProgress::default());
        execute_morph_job(job, tx, CancellationToken::new(), None)
            .await
            .unwrap();

        let snap = rx.borrow().clone();
        assert_eq!(snap.phase, TransferPhase::Completed);
        assert_eq!(snap.destinations[0].resumed_bytes, 400_000);
        assert!(snap.destinations[0].last_digest.is_some());

        let final_file = dst_dir.path().join("Newq-Q8_0.gguf");
        assert!(final_file.exists());
        assert!(!part_file.exists());
        assert_eq!(std::fs::read(&final_file).unwrap(), payload);
    }
}
