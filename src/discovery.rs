use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::Deserialize;
use tokio::process::Command;

use crate::endpoint::{EndpointPath, configure_background_command};

#[derive(Clone, Debug)]
pub struct LocalRoot {
    pub label: String,
    pub path: EndpointPath,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TailnetPeer {
    pub host_name: String,
    pub short_dns: String,
    pub full_dns: String,
    pub ipv4: Option<String>,
    pub os: String,
    pub online: bool,
    pub tailscale_ssh: bool,
    pub is_self: bool,
}

impl TailnetPeer {
    /// Best address for SSH connections (prefers short MagicDNS name, falls back to Tailscale IPv4 or HostName).
    pub fn preferred_host(&self) -> String {
        if !self.short_dns.is_empty() {
            self.short_dns.clone()
        } else if let Some(ip) = &self.ipv4 {
            ip.clone()
        } else {
            self.host_name.clone()
        }
    }

    /// Best host for Windows UNC SMB paths (`\\<ip-or-dns>\<share>`).
    /// Prefers the 100.x.y.z Tailscale IPv4 when available so SMB connects over WireGuard without NetBIOS hiccups.
    pub fn preferred_smb_host(&self) -> String {
        if let Some(ip) = &self.ipv4 {
            ip.clone()
        } else {
            self.preferred_host()
        }
    }

    pub fn display_label(&self) -> String {
        let name = if !self.short_dns.is_empty() {
            &self.short_dns
        } else {
            &self.host_name
        };
        if self.is_self {
            format!("{name} (this device)")
        } else {
            name.to_string()
        }
    }

    pub fn ssh_endpoint(&self, user: &str, remote_path: &str) -> EndpointPath {
        let trimmed_user = user.trim();
        EndpointPath::Ssh {
            user: (!trimmed_user.is_empty()).then(|| trimmed_user.to_string()),
            host: self.preferred_host(),
            port: None,
            path: crate::endpoint::normalize_remote_path(remote_path),
        }
    }

    pub fn smb_endpoint(&self, share: &str) -> EndpointPath {
        let host = self.preferred_smb_host();
        let trimmed_share = share.trim().trim_matches(['\\', '/']);
        let unc = if trimmed_share.is_empty() {
            format!(r"\\{host}")
        } else {
            format!(r"\\{host}\{}", trimmed_share.replace('/', "\\"))
        };
        EndpointPath::Local(PathBuf::from(unc))
    }
}

#[derive(Deserialize)]
struct RawTailscaleStatus {
    #[serde(rename = "BackendState", default)]
    backend_state: String,
    #[serde(rename = "Self")]
    self_node: Option<RawTailscaleNode>,
    #[serde(rename = "Peer", default)]
    peers: HashMap<String, RawTailscaleNode>,
}

#[derive(Deserialize)]
struct RawTailscaleNode {
    #[serde(rename = "HostName", default)]
    host_name: String,
    #[serde(rename = "DNSName", default)]
    dns_name: String,
    #[serde(rename = "TailscaleIPs", default)]
    tailscale_ips: Vec<String>,
    #[serde(rename = "OS", default)]
    os: String,
    #[serde(rename = "Online", default)]
    online: bool,
    #[serde(rename = "sshHostKeys", default)]
    ssh_host_keys: Vec<String>,
}

/// Discovers local drives and standard user directories asynchronously.
pub async fn discover_local_roots() -> Vec<LocalRoot> {
    tokio::task::spawn_blocking(|| {
        let mut roots = Vec::new();

        #[cfg(windows)]
        {
            for letter in b'A'..=b'Z' {
                let drive = format!("{}:\\", letter as char);
                let p = PathBuf::from(&drive);
                if p.exists() {
                    roots.push(LocalRoot {
                        label: format!("Drive ({}:)", letter as char),
                        path: EndpointPath::Local(p),
                    });
                }
            }
        }

        #[cfg(not(windows))]
        {
            roots.push(LocalRoot {
                label: "Root (/)".to_string(),
                path: EndpointPath::Local(PathBuf::from("/")),
            });
            for candidate in ["/mnt", "/Volumes", "/media"] {
                let p = PathBuf::from(candidate);
                if p.exists() {
                    roots.push(LocalRoot {
                        label: candidate.to_string(),
                        path: EndpointPath::Local(p),
                    });
                }
            }
        }

        if let Some(home) = dirs::home_dir() {
            roots.push(LocalRoot {
                label: "Home".to_string(),
                path: EndpointPath::Local(home),
            });
        }
        if let Some(dl) = dirs::download_dir() {
            roots.push(LocalRoot {
                label: "Downloads".to_string(),
                path: EndpointPath::Local(dl),
            });
        }
        if let Some(desk) = dirs::desktop_dir() {
            roots.push(LocalRoot {
                label: "Desktop".to_string(),
                path: EndpointPath::Local(desk),
            });
        }

        roots
    })
    .await
    .unwrap_or_default()
}

/// Queries the local Tailscale daemon (`tailscale status --json`) for all peers on the Tailnet.
pub async fn discover_tailnet_peers() -> Result<Vec<TailnetPeer>> {
    let bin = find_tailscale_binary();
    let output = configure_background_command(Command::new(&bin))
        .args(["status", "--json"])
        .output()
        .await
        .with_context(|| format!("Tailscale CLI not reachable ({})", bin.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("tailscale status exited with error: {}", stderr.trim());
    }

    parse_tailscale_status_json(&output.stdout)
}

pub fn parse_tailscale_status_json(json_bytes: &[u8]) -> Result<Vec<TailnetPeer>> {
    let raw: RawTailscaleStatus =
        serde_json::from_slice(json_bytes).context("Invalid JSON from tailscale status")?;

    if !raw.backend_state.is_empty() && raw.backend_state != "Running" {
        anyhow::bail!("Tailscale state is '{}'", raw.backend_state);
    }

    let mut peers = Vec::new();
    if let Some(me) = raw.self_node {
        peers.push(convert_raw_node(me, true));
    }
    for (_, node) in raw.peers {
        peers.push(convert_raw_node(node, false));
    }

    peers.sort_by(|a, b| {
        a.is_self
            .cmp(&b.is_self)
            .then_with(|| b.online.cmp(&a.online))
            .then_with(|| {
                a.display_label()
                    .to_ascii_lowercase()
                    .cmp(&b.display_label().to_ascii_lowercase())
            })
    });

    Ok(peers)
}

fn convert_raw_node(node: RawTailscaleNode, is_self: bool) -> TailnetPeer {
    let full_dns = node.dns_name.trim_end_matches('.').to_string();
    let short_dns = full_dns.split('.').next().unwrap_or("").to_string();
    let ipv4 = node
        .tailscale_ips
        .iter()
        .find(|ip| ip.contains('.'))
        .cloned()
        .or_else(|| node.tailscale_ips.first().cloned());

    TailnetPeer {
        host_name: node.host_name,
        short_dns,
        full_dns,
        ipv4,
        os: node.os,
        online: node.online || is_self,
        tailscale_ssh: !node.ssh_host_keys.is_empty(),
        is_self,
    }
}

fn find_tailscale_binary() -> PathBuf {
    #[cfg(windows)]
    {
        let standard = PathBuf::from(r"C:\Program Files\Tailscale\tailscale.exe");
        if standard.exists() {
            return standard;
        }
    }
    #[cfg(target_os = "macos")]
    {
        let mac_app = PathBuf::from("/Applications/Tailscale.app/Contents/MacOS/Tailscale");
        if mac_app.exists() {
            return mac_app;
        }
    }
    PathBuf::from("tailscale")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tailscale_status_payload() {
        let sample = br#"{
            "BackendState": "Running",
            "Self": {
                "HostName": "WORKSTATION",
                "DNSName": "workstation.tail1234.ts.net.",
                "TailscaleIPs": ["100.101.102.103", "fd7a:115c:a1e0::1"],
                "OS": "windows",
                "Online": true
            },
            "Peer": {
                "nodekey:abc": {
                    "HostName": "zimaboard2",
                    "DNSName": "zimaboard2.tail1234.ts.net.",
                    "TailscaleIPs": ["100.88.12.34"],
                    "OS": "linux",
                    "Online": true,
                    "sshHostKeys": ["ssh-ed25519 AAAAC3Nza..."]
                }
            }
        }"#;

        let peers = parse_tailscale_status_json(sample).unwrap();
        assert_eq!(peers.len(), 2);
        // External online peers sort ahead of `is_self`
        let zima = &peers[0];
        assert_eq!(zima.short_dns, "zimaboard2");
        assert_eq!(zima.ipv4.as_deref(), Some("100.88.12.34"));
        assert!(zima.tailscale_ssh);
        assert_eq!(
            zima.ssh_endpoint("root", "/DATA/Ark/Models").to_string(),
            "ssh://root@zimaboard2/DATA/Ark/Models"
        );
        assert_eq!(
            zima.smb_endpoint("Ark/Models").to_string(),
            r"\\100.88.12.34\Ark\Models"
        );
    }
}
