#!/usr/bin/env bash
# Installs AALP as a supervised, boot-surviving systemd --user service.
# Assumes this repository is checked out at $HOME/agent-api-lane-protocol
# (the unit file's WorkingDirectory/ExecStart use systemd's %h specifier,
# i.e. the invoking user's own home directory).
#
# Idempotent: safe to re-run after a `git pull` that changes aalp.service.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UNIT_DIR="$HOME/.config/systemd/user"

if [ "$REPO_ROOT" != "$HOME/agent-api-lane-protocol" ]; then
    echo "warning: this checkout is at $REPO_ROOT, not \$HOME/agent-api-lane-protocol" >&2
    echo "         aalp.service's %h-relative paths will not resolve correctly." >&2
fi

mkdir -p "$UNIT_DIR"
cp "$REPO_ROOT/deploy/aalp.service" "$UNIT_DIR/aalp.service"

systemctl --user daemon-reload
systemctl --user enable --now aalp.service

echo
echo "aalp.service installed and started:"
systemctl --user status aalp.service --no-pager -l | head -8

if [ "$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null)" != "yes" ]; then
    echo
    echo "note: user lingering is not enabled, so aalp.service will stop at logout"
    echo "      and not start on boot. To fix: sudo loginctl enable-linger $(id -un)"
fi
