#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
opt_root="${HOME}/.local/opt/signal-headless"
lib_root="${HOME}/.local/lib/claude-messages"
unit_root="${HOME}/.config/systemd/user"

mkdir -p "$opt_root" "$lib_root" "$unit_root/legion-messages.service.d"

if command -v Xvfb >/dev/null 2>&1; then
  xvfb_bin="$(command -v Xvfb)"
else
  package_url="$(pacman -Sp --print-format '%l' xorg-server-xvfb 2>/dev/null | tail -n 1)"
  if [[ -z "$package_url" ]]; then
    echo "Could not resolve the xorg-server-xvfb package URL" >&2
    exit 2
  fi

  temp_dir="$(mktemp -d)"
  trap 'rm -rf "$temp_dir"' EXIT
  curl --fail --location --silent --show-error "$package_url" -o "$temp_dir/xvfb.pkg.tar.zst"
  bsdtar -xf "$temp_dir/xvfb.pkg.tar.zst" -C "$opt_root"
  xvfb_bin="$opt_root/usr/bin/Xvfb"
fi

install -m 0755 "$repo_root/scripts/run-signal-desktop-headless.sh" \
  "$lib_root/run-signal-desktop-headless.sh"
install -m 0644 "$repo_root/systemd/claude-signal-desktop.service" \
  "$unit_root/claude-signal-desktop.service"
install -m 0644 "$repo_root/systemd/legion-messages.service.d/signal-source.conf" \
  "$unit_root/legion-messages.service.d/signal-source.conf"

systemctl --user daemon-reload
echo "Prepared claude-signal-desktop.service with Xvfb at $xvfb_bin"
echo "No Signal credentials were changed and the service was not started."
