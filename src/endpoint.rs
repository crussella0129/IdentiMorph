use std::fmt;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use tokio::process::Command;

/// Represents a unified local, SMB/UNC, or remote SSH/Tailscale-SSH storage location.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EndpointPath {
    /// Local filesystem path or Windows UNC/SMB share (e.g. `D:\Models` or `\\ZIMABOARD2\Ark\Models`).
    Local(PathBuf),
    /// Remote POSIX filesystem accessed over OpenSSH / Tailscale SSH (`ssh://[user@]host[:port]/path`).
    Ssh {
        user: Option<String>,
        host: String,
        port: Option<u16>,
        path: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntryInfo {
    pub name: String,
    pub path: EndpointPath,
    pub is_dir: bool,
    pub size: u64,
    pub is_model_weight: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryStat {
    pub is_dir: bool,
    pub size: u64,
}

/// A single concrete file resolved relative to a transfer root.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RelativeFileSpec {
    /// Forward-slash separated relative path to materialize inside each destination root.
    pub rel_path: String,
    /// Full source location of this file.
    pub source_path: EndpointPath,
    /// Exact byte length of the source file.
    pub size: u64,
}

impl EndpointPath {
    /// Parses a user-entered string into an [`EndpointPath`].
    ///
    /// Supported forms:
    /// - `ssh://[user@]host[:port][/abs/path]`
    /// - `[user@]host:/abs/path` (SCP-style shorthand when `user@` is present)
    /// - Local paths (`C:\Models`, `/mnt/models`) and UNC shares (`\\ZIMABOARD2\Ark\Models`)
    pub fn parse(raw: &str) -> Result<Self> {
        let trimmed = raw.trim().trim_matches('"');
        if trimmed.is_empty() {
            bail!("Path cannot be empty");
        }

        if let Some(rest) = trimmed.strip_prefix("ssh://") {
            return Self::parse_ssh_uri_body(rest);
        }

        // Support SCP-style `user@host:/path` without colliding with Windows `C:\...`
        if let Some((lhs, rhs)) = trimmed.split_once(':')
            && lhs.contains('@')
            && !lhs.contains('\\')
            && !lhs.contains('/')
        {
            let (user_part, host_part) = lhs
                .split_once('@')
                .ok_or_else(|| anyhow!("Invalid SSH target '{trimmed}'"))?;
            if host_part.is_empty() {
                bail!("SSH host cannot be empty in '{trimmed}'");
            }
            let user = (!user_part.is_empty()).then(|| user_part.to_string());
            let norm_path = normalize_remote_path(rhs);
            return Ok(Self::Ssh {
                user,
                host: host_part.to_string(),
                port: None,
                path: norm_path,
            });
        }

        Ok(Self::Local(PathBuf::from(trimmed)))
    }

    fn parse_ssh_uri_body(rest: &str) -> Result<Self> {
        let (authority, raw_path) = match rest.find('/') {
            Some(idx) => (&rest[..idx], &rest[idx..]),
            None => (rest, "/"),
        };

        if authority.is_empty() {
            bail!("SSH URI is missing host (expected ssh://[user@]host[:port]/path)");
        }

        let (user, host_port) = match authority.rsplit_once('@') {
            Some((u, hp)) => {
                let user_opt = (!u.is_empty()).then(|| u.to_string());
                (user_opt, hp)
            }
            None => (None, authority),
        };

        let (host, port) = if let Some((h, p_str)) = host_port.rsplit_once(':')
            && !h.contains(':')
        {
            let parsed_port: u16 = p_str
                .parse()
                .with_context(|| format!("Invalid SSH port '{p_str}'"))?;
            (h.to_string(), Some(parsed_port))
        } else {
            (host_port.to_string(), None)
        };

        if host.is_empty() {
            bail!("SSH host cannot be empty");
        }

        Ok(Self::Ssh {
            user,
            host,
            port,
            path: normalize_remote_path(raw_path),
        })
    }

    pub fn is_ssh(&self) -> bool {
        matches!(self, Self::Ssh { .. })
    }

    /// Joins a relative `/` or `\` separated subpath onto this endpoint.
    pub fn join(&self, rel: &str) -> Self {
        let segments: Vec<&str> = rel
            .split(['/', '\\'])
            .filter(|seg| !seg.is_empty() && *seg != ".")
            .collect();

        match self {
            Self::Local(base) => {
                let mut out = base.clone();
                for seg in segments {
                    out.push(seg);
                }
                Self::Local(out)
            }
            Self::Ssh {
                user,
                host,
                port,
                path,
            } => {
                let mut combined = path.trim_end_matches('/').to_string();
                for seg in segments {
                    combined.push('/');
                    combined.push_str(seg);
                }
                if combined.is_empty() {
                    combined.push('/');
                }
                Self::Ssh {
                    user: user.clone(),
                    host: host.clone(),
                    port: *port,
                    path: normalize_remote_path(&combined),
                }
            }
        }
    }

    /// Returns the parent directory of this endpoint, if one exists.
    pub fn parent(&self) -> Option<Self> {
        match self {
            Self::Local(p) => p
                .parent()
                .filter(|par| !par.as_os_str().is_empty())
                .map(|par| Self::Local(par.to_path_buf())),
            Self::Ssh {
                user,
                host,
                port,
                path,
            } => {
                let norm = normalize_remote_path(path);
                if norm == "/" {
                    return None;
                }
                let parent_path = match norm.rfind('/') {
                    Some(0) | None => "/".to_string(),
                    Some(idx) => norm[..idx].to_string(),
                };
                Some(Self::Ssh {
                    user: user.clone(),
                    host: host.clone(),
                    port: *port,
                    path: parent_path,
                })
            }
        }
    }

    /// Returns the final file or directory name component.
    pub fn file_name(&self) -> Option<String> {
        match self {
            Self::Local(p) => p.file_name().map(|os| os.to_string_lossy().into_owned()),
            Self::Ssh { path, .. } => {
                let norm = normalize_remote_path(path);
                if norm == "/" {
                    None
                } else {
                    norm.rsplit('/').next().map(ToOwned::to_owned)
                }
            }
        }
    }

    /// Builds `ssh` target string (`user@host` or `host`) and base CLI arguments.
    pub fn ssh_target_and_args(&self) -> Option<(String, Vec<String>)> {
        match self {
            Self::Local(_) => None,
            Self::Ssh {
                user, host, port, ..
            } => {
                let target = match user {
                    Some(u) if !u.is_empty() => format!("{u}@{host}"),
                    _ => host.clone(),
                };
                let mut args = vec![
                    "-o".to_string(),
                    "BatchMode=yes".to_string(),
                    "-o".to_string(),
                    "Compression=no".to_string(),
                    "-o".to_string(),
                    "ConnectTimeout=10".to_string(),
                ];
                if let Some(p) = port {
                    args.push("-p".to_string());
                    args.push(p.to_string());
                }
                Some((target, args))
            }
        }
    }

    /// Lists directory entries sorted with directories first, then case-insensitive name.
    pub async fn list_dir(&self) -> Result<Vec<DirEntryInfo>> {
        let mut entries = match self {
            Self::Local(dir) => list_local_dir(dir).await?,
            Self::Ssh { path, .. } => self.list_ssh_dir(path).await?,
        };

        entries.sort_by(|a, b| {
            b.is_dir.cmp(&a.is_dir).then_with(|| {
                a.name
                    .to_ascii_lowercase()
                    .cmp(&b.name.to_ascii_lowercase())
            })
        });
        Ok(entries)
    }

    /// Creates a directory (and any missing parents) at this endpoint.
    pub async fn create_dir_all(&self) -> Result<()> {
        match self {
            Self::Local(dir) => {
                tokio::fs::create_dir_all(dir)
                    .await
                    .with_context(|| format!("Failed to create directory {}", dir.display()))?;
                Ok(())
            }
            Self::Ssh { path, .. } => {
                let cmd = format!("mkdir -p -- {}", shell_quote(path));
                self.run_ssh_command(&cmd)
                    .await
                    .with_context(|| format!("Failed to create remote directory {self}"))?;
                Ok(())
            }
        }
    }

    /// Queries metadata (`is_dir`, `size`) if the path exists, or returns `Ok(None)` if absent.
    pub async fn stat(&self) -> Result<Option<EntryStat>> {
        match self {
            Self::Local(p) => match tokio::fs::metadata(p).await {
                Ok(meta) => Ok(Some(EntryStat {
                    is_dir: meta.is_dir(),
                    size: if meta.is_file() { meta.len() } else { 0 },
                })),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e).with_context(|| format!("Failed to stat {}", p.display())),
            },
            Self::Ssh { path, .. } => {
                let quoted = shell_quote(path);
                let script = format!(
                    "if [ ! -e {q} ]; then echo MISSING; elif [ -d {q} ]; then echo DIR:0; else sz=$(wc -c < {q} | tr -d ' '); echo FILE:$sz; fi",
                    q = quoted
                );
                let out = self.run_ssh_command(&script).await?;
                let line = out.trim();
                if line == "MISSING" {
                    Ok(None)
                } else if line.starts_with("DIR:") {
                    Ok(Some(EntryStat {
                        is_dir: true,
                        size: 0,
                    }))
                } else if let Some(sz_str) = line.strip_prefix("FILE:") {
                    let size: u64 = sz_str.trim().parse().unwrap_or(0);
                    Ok(Some(EntryStat {
                        is_dir: false,
                        size,
                    }))
                } else {
                    bail!("Unexpected remote stat response for {self}: {line}");
                }
            }
        }
    }

    /// Resolves a file or directory endpoint into the complete ordered list of files to transfer.
    ///
    /// - If `self` is a single file `foo.gguf`, returns `rel_path = "foo.gguf"`.
    /// - If `self` is a directory `MyModel`, returns `rel_path = "MyModel/<subpath>"` so copying a
    ///   model directory into a destination parent folder preserves the model's folder container.
    pub async fn walk_files(&self, skip_temp_artifacts: bool) -> Result<Vec<RelativeFileSpec>> {
        match self {
            Self::Local(root) => walk_local_files(root, skip_temp_artifacts).await,
            Self::Ssh { path, .. } => self.walk_ssh_files(path, skip_temp_artifacts).await,
        }
    }

    pub async fn run_ssh_command(&self, remote_script: &str) -> Result<String> {
        let (target, mut args) = self
            .ssh_target_and_args()
            .ok_or_else(|| anyhow!("run_ssh_command called on non-SSH endpoint"))?;
        args.push(target.clone());
        args.push(remote_script.to_string());

        let output = configure_background_command(Command::new("ssh"))
            .args(&args)
            .output()
            .await
            .with_context(|| format!("Failed to spawn ssh to {target}"))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            bail!(
                "SSH command failed on {target} (exit {:?}): {}",
                output.status.code(),
                stderr.trim()
            );
        }
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    }

    async fn list_ssh_dir(&self, remote_path: &str) -> Result<Vec<DirEntryInfo>> {
        let quoted = shell_quote(remote_path);
        // Uses python3 when available for fast, exact JSON listing; falls back to POSIX sh + wc.
        let script = format!(
            concat!(
                "if command -v python3 >/dev/null 2>&1; then ",
                "python3 -c 'import json,os,sys; p=sys.argv[1]; out=[]; ",
                "[out.append([e.name, e.is_dir(follow_symlinks=True), ",
                "e.stat(follow_symlinks=True).st_size if not e.is_dir(follow_symlinks=True) else 0]) ",
                "for e in os.scandir(p)]; print(json.dumps(out))' {q}; ",
                "else ",
                "cd -- {q} && for f in * .[!.]* ..?*; do [ -e \"$f\" ] || continue; ",
                "if [ -d \"$f\" ]; then printf 'D\\t0\\t%s\\n' \"$f\"; ",
                "else sz=$(wc -c < \"$f\" | tr -d ' '); printf 'F\\t%s\\t%s\\n' \"$sz\" \"$f\"; fi; done; ",
                "fi"
            ),
            q = quoted
        );

        let stdout = self.run_ssh_command(&script).await?;
        let trimmed = stdout.trim();
        if trimmed.is_empty() {
            return Ok(Vec::new());
        }

        if trimmed.starts_with('[') {
            let rows: Vec<(String, bool, u64)> = serde_json::from_str(trimmed)
                .context("Failed to parse remote JSON directory listing")?;
            return Ok(rows
                .into_iter()
                .map(|(name, is_dir, size)| DirEntryInfo {
                    path: self.join(&name),
                    is_model_weight: !is_dir && is_model_weight_name(&name),
                    name,
                    is_dir,
                    size,
                })
                .collect());
        }

        let mut items = Vec::new();
        for line in trimmed.lines() {
            let mut parts = line.splitn(3, '\t');
            let (Some(kind), Some(sz_str), Some(name)) = (parts.next(), parts.next(), parts.next())
            else {
                continue;
            };
            let is_dir = kind == "D";
            let size = sz_str.parse::<u64>().unwrap_or(0);
            items.push(DirEntryInfo {
                path: self.join(name),
                is_model_weight: !is_dir && is_model_weight_name(name),
                name: name.to_string(),
                is_dir,
                size,
            });
        }
        Ok(items)
    }

    async fn walk_ssh_files(
        &self,
        remote_path: &str,
        skip_temp_artifacts: bool,
    ) -> Result<Vec<RelativeFileSpec>> {
        let stat = self
            .stat()
            .await?
            .ok_or_else(|| anyhow!("Remote source does not exist: {self}"))?;

        let root_name = self
            .file_name()
            .unwrap_or_else(|| "remote_root".to_string());

        if !stat.is_dir {
            return Ok(vec![RelativeFileSpec {
                rel_path: root_name,
                source_path: self.clone(),
                size: stat.size,
            }]);
        }

        let quoted = shell_quote(remote_path);
        let script = format!(
            concat!(
                "if command -v python3 >/dev/null 2>&1; then ",
                "python3 -c 'import json,os,sys; root=sys.argv[1]; out=[];\n",
                "for dp, dn, fn in os.walk(root):\n",
                "  rel_dir = os.path.relpath(dp, root)\n",
                "  for f in fn:\n",
                "    fp = os.path.join(dp, f)\n",
                "    if os.path.isfile(fp):\n",
                "      rp = f if rel_dir == \".\" else rel_dir.replace(\"\\\\\", \"/\") + \"/\" + f\n",
                "      out.append([rp, os.path.getsize(fp)])\n",
                "print(json.dumps(out))' {q}; ",
                "else ",
                "cd -- {q} && find . -type f | while IFS= read -r f; do ",
                "rel=\"${{f#./}}\"; sz=$(wc -c < \"$f\" | tr -d ' '); printf '%s\\t%s\\n' \"$sz\" \"$rel\"; done; ",
                "fi"
            ),
            q = quoted
        );

        let stdout = self.run_ssh_command(&script).await?;
        let trimmed = stdout.trim();
        let mut raw_files: Vec<(String, u64)> = Vec::new();

        if trimmed.starts_with('[') {
            raw_files =
                serde_json::from_str(trimmed).context("Failed to parse remote file walk JSON")?;
        } else if !trimmed.is_empty() {
            for line in trimmed.lines() {
                if let Some((sz_str, rel)) = line.split_once('\t') {
                    raw_files.push((rel.to_string(), sz_str.parse::<u64>().unwrap_or(0)));
                }
            }
        }

        raw_files.sort_by(|a, b| a.0.cmp(&b.0));

        let mut specs = Vec::new();
        for (sub_rel, size) in raw_files {
            if skip_temp_artifacts && path_contains_temp_artifact(&sub_rel) {
                continue;
            }
            let rel_path = format!("{root_name}/{}", sub_rel.trim_start_matches('/'));
            specs.push(RelativeFileSpec {
                rel_path,
                source_path: self.join(&sub_rel),
                size,
            });
        }
        Ok(specs)
    }
}

impl fmt::Display for EndpointPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(p) => write!(f, "{}", p.display()),
            Self::Ssh {
                user,
                host,
                port,
                path,
            } => {
                write!(f, "ssh://")?;
                if let Some(u) = user
                    && !u.is_empty()
                {
                    write!(f, "{u}@")?;
                }
                write!(f, "{host}")?;
                if let Some(p) = port {
                    write!(f, ":{p}")?;
                }
                write!(f, "{}", normalize_remote_path(path))
            }
        }
    }
}

async fn list_local_dir(dir: &Path) -> Result<Vec<DirEntryInfo>> {
    let mut rd = tokio::fs::read_dir(dir)
        .await
        .with_context(|| format!("Unable to open directory {}", dir.display()))?;
    let mut out = Vec::new();
    while let Some(entry) = rd.next_entry().await? {
        let name = entry.file_name().to_string_lossy().into_owned();
        let entry_path = entry.path();
        let (is_dir, size) = match tokio::fs::metadata(&entry_path).await {
            Ok(m) => (m.is_dir(), if m.is_file() { m.len() } else { 0 }),
            Err(_) => continue,
        };
        out.push(DirEntryInfo {
            is_model_weight: !is_dir && is_model_weight_name(&name),
            name,
            path: EndpointPath::Local(entry_path),
            is_dir,
            size,
        });
    }
    Ok(out)
}

async fn walk_local_files(root: &Path, skip_temp_artifacts: bool) -> Result<Vec<RelativeFileSpec>> {
    let meta = tokio::fs::metadata(root)
        .await
        .with_context(|| format!("Source path does not exist: {}", root.display()))?;

    let root_name = root
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "root".to_string());

    if meta.is_file() {
        return Ok(vec![RelativeFileSpec {
            rel_path: root_name,
            source_path: EndpointPath::Local(root.to_path_buf()),
            size: meta.len(),
        }]);
    }

    let mut specs = Vec::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let mut rd = tokio::fs::read_dir(&dir)
            .await
            .with_context(|| format!("Failed to read directory {}", dir.display()))?;
        while let Some(entry) = rd.next_entry().await? {
            let name = entry.file_name().to_string_lossy().into_owned();
            if skip_temp_artifacts && is_temp_artifact_name(&name) {
                continue;
            }
            let p = entry.path();
            let m = match tokio::fs::metadata(&p).await {
                Ok(m) => m,
                Err(_) => continue,
            };
            if m.is_dir() {
                stack.push(p);
            } else if m.is_file() {
                let sub = p
                    .strip_prefix(root)
                    .unwrap_or(&p)
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                specs.push(RelativeFileSpec {
                    rel_path: format!("{root_name}/{sub}"),
                    source_path: EndpointPath::Local(p),
                    size: m.len(),
                });
            }
        }
    }

    specs.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(specs)
}

/// Normalizes a POSIX remote path so it always starts with `/` and has no trailing `/` (unless root).
pub fn normalize_remote_path(raw: &str) -> String {
    let replaced = raw.trim().replace('\\', "/");
    let mut parts = Vec::new();
    for seg in replaced.split('/') {
        if seg.is_empty() || seg == "." {
            continue;
        }
        if seg == ".." {
            parts.pop();
        } else {
            parts.push(seg);
        }
    }
    if parts.is_empty() {
        "/".to_string()
    } else {
        format!("/{}", parts.join("/"))
    }
}

/// Safely quotes an arbitrary string for POSIX `sh` execution.
pub fn shell_quote(s: &str) -> String {
    format!("'{}'", s.replace('\'', "'\\''"))
}

/// Returns `true` for Hugging Face / transfer temporary artifacts that should be skipped by default.
pub fn is_temp_artifact_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower == ".cache"
        || lower.ends_with(".incomplete")
        || lower.ends_with(".lock")
        || lower.ends_with(".identimorph.part")
}

pub fn path_contains_temp_artifact(rel_path: &str) -> bool {
    rel_path
        .split(['/', '\\'])
        .any(|seg| !seg.is_empty() && is_temp_artifact_name(seg))
}

/// Highlights common AI model checkpoint formats in the explorer UI.
pub fn is_model_weight_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    [
        ".safetensors",
        ".gguf",
        ".ckpt",
        ".pt",
        ".pth",
        ".onnx",
        ".bin",
        ".mlx",
    ]
    .iter()
    .any(|ext| lower.ends_with(ext))
}

/// Detects a companion multimodal projector (`mmproj*.gguf`) in the same folder when selecting a `.gguf`.
pub fn find_companion_mmproj(
    selected_name: &str,
    siblings: &[DirEntryInfo],
) -> Option<DirEntryInfo> {
    let lower = selected_name.to_ascii_lowercase();
    if !lower.ends_with(".gguf") || lower.contains("mmproj") {
        return None;
    }

    let mmprojs: Vec<&DirEntryInfo> = siblings
        .iter()
        .filter(|e| {
            !e.is_dir && {
                let n = e.name.to_ascii_lowercase();
                n.ends_with(".gguf") && n.contains("mmproj")
            }
        })
        .collect();

    if mmprojs.is_empty() {
        return None;
    }
    if mmprojs.len() == 1 {
        return Some(mmprojs[0].clone());
    }

    // Score each candidate mmproj by shared alphanumeric tokens with the selected model stem.
    let stem = lower.trim_end_matches(".gguf");
    let tokens: Vec<&str> = stem
        .split(['-', '_', '.'])
        .filter(|t| t.len() >= 3)
        .collect();

    mmprojs
        .into_iter()
        .max_by_key(|cand| {
            let cn = cand.name.to_ascii_lowercase();
            tokens.iter().filter(|tok| cn.contains(**tok)).count()
        })
        .cloned()
}

/// Formats a byte count into human-readable binary units (`GiB`, `MiB`, `KiB`, `B`).
pub fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    const TIB: f64 = GIB * 1024.0;

    let b = bytes as f64;
    if b >= TIB {
        format!("{:.2} TiB", b / TIB)
    } else if b >= GIB {
        format!("{:.2} GiB", b / GIB)
    } else if b >= MIB {
        format!("{:.2} MiB", b / MIB)
    } else if b >= KIB {
        format!("{:.1} KiB", b / KIB)
    } else {
        format!("{bytes} B")
    }
}

/// Prevents flashing console windows on Windows when spawning background `ssh` / `tailscale` commands.
#[cfg(windows)]
pub fn configure_background_command(mut cmd: Command) -> Command {
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    cmd.creation_flags(CREATE_NO_WINDOW);
    cmd
}

/// Identity pass-through on non-Windows platforms.
#[cfg(not(windows))]
pub fn configure_background_command(cmd: Command) -> Command {
    cmd
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ssh_uris_and_scp_shorthand() {
        let ep = EndpointPath::parse("ssh://root@zimaboard2:2222/DATA/Ark/Models/").unwrap();
        assert_eq!(
            ep,
            EndpointPath::Ssh {
                user: Some("root".to_string()),
                host: "zimaboard2".to_string(),
                port: Some(2222),
                path: "/DATA/Ark/Models".to_string(),
            }
        );
        assert_eq!(ep.to_string(), "ssh://root@zimaboard2:2222/DATA/Ark/Models");

        let scp = EndpointPath::parse("charl@100.64.0.5:/mnt/models").unwrap();
        assert_eq!(
            scp,
            EndpointPath::Ssh {
                user: Some("charl".to_string()),
                host: "100.64.0.5".to_string(),
                port: None,
                path: "/mnt/models".to_string(),
            }
        );
    }

    #[test]
    fn parses_windows_local_and_unc_paths() {
        let local = EndpointPath::parse(r"D:\Models\gguf").unwrap();
        assert!(matches!(local, EndpointPath::Local(_)));

        let unc = EndpointPath::parse(r"\\ZIMABOARD2\Ark\Models").unwrap();
        assert!(matches!(unc, EndpointPath::Local(_)));
    }

    #[test]
    fn ssh_join_and_parent_navigation() {
        let root = EndpointPath::parse("ssh://root@zimaboard2/DATA/Ark").unwrap();
        let child = root.join("Models/gguf");
        assert_eq!(
            child.to_string(),
            "ssh://root@zimaboard2/DATA/Ark/Models/gguf"
        );
        assert_eq!(child.file_name().as_deref(), Some("gguf"));

        let p1 = child.parent().unwrap();
        assert_eq!(p1.to_string(), "ssh://root@zimaboard2/DATA/Ark/Models");
        let top = EndpointPath::parse("ssh://root@zimaboard2/").unwrap();
        assert!(top.parent().is_none());
    }

    #[test]
    fn detects_companion_mmproj() {
        let siblings = vec![
            DirEntryInfo {
                name: "Qwen3.8-27B-Uncensored-Cyber-IQ4_XS.gguf".to_string(),
                path: EndpointPath::Local(PathBuf::from("a")),
                is_dir: false,
                size: 100,
                is_model_weight: true,
            },
            DirEntryInfo {
                name: "mmproj-Qwen3.8-27B-Uncensored-Cyber-BF16.gguf".to_string(),
                path: EndpointPath::Local(PathBuf::from("b")),
                is_dir: false,
                size: 10,
                is_model_weight: true,
            },
            DirEntryInfo {
                name: "mmproj-Gemma-4-E4B-f16.gguf".to_string(),
                path: EndpointPath::Local(PathBuf::from("c")),
                is_dir: false,
                size: 10,
                is_model_weight: true,
            },
        ];

        let found =
            find_companion_mmproj("Qwen3.8-27B-Uncensored-Cyber-IQ4_XS.gguf", &siblings).unwrap();
        assert_eq!(found.name, "mmproj-Qwen3.8-27B-Uncensored-Cyber-BF16.gguf");
    }
}
