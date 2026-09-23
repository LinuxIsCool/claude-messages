#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
unit_root="${HOME}/.config/systemd/user"
bin_root="${HOME}/.local/bin"
opt_root="${HOME}/.local/opt"

version="$(curl -Ls -o /dev/null -w '%{url_effective}' \
  https://github.com/AsamK/signal-cli/releases/latest | sed -E 's#^.*/v##')"
if [[ -z "$version" ]]; then
  echo "Could not resolve the latest signal-cli version" >&2
  exit 2
fi

install_root="${opt_root}/signal-cli-${version}"
binary="${install_root}/signal-cli"
if [[ ! -x "$binary" ]]; then
  if [[ -e "$install_root" ]]; then
    echo "$install_root exists but is incomplete; inspect it before retrying" >&2
    exit 2
  fi
  temp_dir="$(mktemp -d)"
  trap 'rm -rf "$temp_dir"' EXIT
  archive="${temp_dir}/signal-cli.tar.gz"
  curl --fail --location --silent --show-error \
    "https://github.com/AsamK/signal-cli/releases/download/v${version}/signal-cli-${version}-Linux-native.tar.gz" \
    -o "$archive"
  mkdir -p "$temp_dir/extract"
  bsdtar -xf "$archive" -C "$temp_dir/extract"
  source_binary="$(find "$temp_dir/extract" -type f -name signal-cli -perm -u+x -print -quit)"
  if [[ -z "$source_binary" ]]; then
    echo "The signal-cli archive did not contain an executable" >&2
    exit 2
  fi
  mkdir -p "$install_root"
  install -m 0755 "$source_binary" "$binary"
fi

mkdir -p "$bin_root" "$unit_root/legion-messages.service.d"
ln -sfn "$binary" "$bin_root/signal-cli"
install -m 0644 "$repo_root/systemd/legion-messages.service.d/signal-source.conf" \
  "$unit_root/legion-messages.service.d/signal-source.conf"
systemctl --user daemon-reload

"$bin_root/signal-cli" --version
echo "signal-cli is installed but not linked. No Signal credentials were changed."
echo "Next: $bin_root/signal-cli link --name \"Legion Observation\""
