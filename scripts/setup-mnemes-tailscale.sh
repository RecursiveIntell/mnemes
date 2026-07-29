#!/usr/bin/env bash
#
# Configure private Tailscale access to a loopback-only Mnemes server.
#
# Safe default: audit only. Use --apply to change host/Tailscale state.
# This script never prints an auth key or writes credentials to disk.
#
set -euo pipefail

apply=0
install_missing=0
configure_serve=1
force_serve=0
run_ssh=1
port="${MNEMES_TAILSCALE_PORT:-${MNEMES_PORT:-1738}}"
hostname_override="${MNEMES_TAILSCALE_HOSTNAME:-$(hostname -s 2>/dev/null || hostname)}"
auth_key_file="${MNEMES_TAILSCALE_AUTH_KEY_FILE:-}"

blue=$'\033[0;34m'
green=$'\033[0;32m'
yellow=$'\033[1;33m'
red=$'\033[0;31m'
purple=$'\033[0;35m'
reset=$'\033[0m'
info() { printf '%s\n' "${blue}ℹ${reset}  $*"; }
ok() { printf '%s\n' "${green}✓${reset}  $*"; }
warn() { printf '%s\n' "${yellow}⚠${reset}  $*"; }
err() { printf '%s\n' "${red}✗${reset}  $*" >&2; }
header() { printf '\n%s\n\n' "${purple}═══ $* ═══${reset}"; }

usage() {
    cat <<'USAGE'
Configure private Tailscale access to a loopback-only Mnemes server.

Usage:
  setup-mnemes-tailscale.sh --audit                 Read-only inspection (default)
  setup-mnemes-tailscale.sh --apply [options]       Enroll/configure this host

Options:
  --install-missing       Install Tailscale with the host package manager if absent
  --auth-key-file PATH    Use a one-time auth-key file without printing its contents
  --hostname NAME         Tailnet hostname (default: system short hostname)
  --port PORT             Local Mnemes port (default: MNEMES_PORT or 1738)
  --no-serve               Enroll Tailscale but do not configure Tailscale Serve
  --force-serve            Replace an existing Serve configuration (destructive to that proxy)
  --no-ssh                 Do not request/enable Tailscale SSH
  --help                   Show this help

Environment:
  MNEMES_TAILSCALE_AUTH_KEY_FILE, MNEMES_TAILSCALE_HOSTNAME,
  MNEMES_TAILSCALE_PORT, MNEMES_PORT

The apply flow keeps Mnemes bound to loopback and exposes it only through
Tailscale Serve over the tailnet. It does not configure Funnel or open LAN ports.
USAGE
}

while (($#)); do
    case "$1" in
        --audit) apply=0 ;;
        --apply) apply=1 ;;
        --install-missing) install_missing=1 ;;
        --auth-key-file)
            [[ $# -ge 2 ]] || { err "--auth-key-file requires a path"; exit 2; }
            auth_key_file=$2; shift
            ;;
        --hostname)
            [[ $# -ge 2 ]] || { err "--hostname requires a value"; exit 2; }
            hostname_override=$2; shift
            ;;
        --port)
            [[ $# -ge 2 ]] || { err "--port requires a value"; exit 2; }
            port=$2; shift
            ;;
        --no-serve) configure_serve=0 ;;
        --force-serve) force_serve=1 ;;
        --no-ssh) run_ssh=0 ;;
        --help|-h) usage; exit 0 ;;
        *) err "unknown option: $1"; usage >&2; exit 2 ;;
    esac
    shift
done

[[ $port =~ ^[0-9]+$ ]] && ((port > 0 && port < 65536)) || {
    err "invalid Mnemes port: $port"; exit 2;
}
[[ $hostname_override =~ ^[A-Za-z0-9][A-Za-z0-9.-]*$ ]] || {
    err "invalid Tailscale hostname: $hostname_override"; exit 2;
}
if [[ -n $auth_key_file && ! -r $auth_key_file ]]; then
    err "auth-key file is not readable: $auth_key_file"; exit 2
fi

TAILSCALE_BIN=$(command -v tailscale || true)

backend_state() {
    [[ -n $TAILSCALE_BIN ]] || return 0
    "$TAILSCALE_BIN" status --json 2>/dev/null \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("BackendState", "unknown"))' \
        2>/dev/null || true
}

local_health() {
    local path
    for path in /livez /v1/livez /v1/health; do
        if curl -fsS --max-time 3 "http://127.0.0.1:${port}${path}" >/dev/null 2>&1; then
            printf '%s' "$path"
            return 0
        fi
    done
    return 1
}

serve_status() {
    [[ -n $TAILSCALE_BIN ]] || return 1
    if (( EUID == 0 )); then
        "$TAILSCALE_BIN" serve status 2>/dev/null
    else
        sudo -n "$TAILSCALE_BIN" serve status 2>/dev/null
    fi
}

run_ts() {
    if (( EUID == 0 )); then
        "$TAILSCALE_BIN" "$@"
    else
        sudo "$TAILSCALE_BIN" "$@"
    fi
}

print_audit() {
    header "Mnemes Tailscale AUDIT"
    printf 'mode=audit (read-only)\n'
    printf 'hostname=%s\nport=%s\n' "$hostname_override" "$port"
    if [[ -z $TAILSCALE_BIN ]]; then
        warn "Tailscale is not installed"
    else
        ok "Tailscale: $($TAILSCALE_BIN version | head -1)"
        printf 'backend_state=%s\n' "$(backend_state)"
        local prefs
        prefs=$($TAILSCALE_BIN debug prefs 2>/dev/null || true)
        if grep -q '"RunSSH": true' <<<"$prefs"; then printf 'tailscale_ssh=enabled\n'; else printf 'tailscale_ssh=disabled-or-unknown\n'; fi
        if grep -q '"NetfilterMode":' <<<"$prefs"; then printf 'netfilter_prefs=present\n'; fi
        if serve_status >/tmp/mnemes-tailscale-serve-status.$$ 2>/dev/null; then
            if grep -qiE 'no serve config|not configured' /tmp/mnemes-tailscale-serve-status.$$; then
                printf 'serve=not-configured\n'
            else
                ok "Tailscale Serve configuration present"
                sed -n '1,12p' /tmp/mnemes-tailscale-serve-status.$$
            fi
            rm -f /tmp/mnemes-tailscale-serve-status.$$
        else
            printf 'serve=not-configured-or-unavailable\n'
            rm -f /tmp/mnemes-tailscale-serve-status.$$
        fi
    fi
    if path=$(local_health); then
        ok "Mnemes responds on loopback at ${path}"
    else
        warn "Mnemes is not healthy on loopback port ${port}; Serve will be deferred"
    fi
    printf 'scope=tailnet-only; funnel=disabled; LAN binding remains loopback\n'
}

install_tailscale() {
    if command -v tailscale >/dev/null 2>&1; then
        TAILSCALE_BIN=$(command -v tailscale)
        return 0
    fi
    (( install_missing )) || {
        err "Tailscale is missing; rerun with --install-missing or install it first"
        return 1
    }
    if command -v dnf >/dev/null 2>&1; then
        info "Installing Tailscale with dnf"
        sudo dnf install -y tailscale
    elif command -v apt-get >/dev/null 2>&1; then
        info "Installing Tailscale with the official Debian/Ubuntu installer"
        curl -fsSL https://tailscale.com/install.sh | sh
    else
        err "No supported package manager found; install Tailscale from https://tailscale.com/download"
        return 1
    fi
    TAILSCALE_BIN=$(command -v tailscale || true)
    [[ -n $TAILSCALE_BIN ]] || { err "Tailscale installation did not produce a tailscale binary"; return 1; }
}

apply_configuration() {
    header "Configuring Mnemes Tailscale access"
    install_tailscale
    info "Starting tailscaled (if needed)"
    sudo systemctl enable --now tailscaled

    local state
    state=$(backend_state)
    if [[ $state != Running ]]; then
        local args=(up --hostname "$hostname_override")
        (( run_ssh )) && args+=(--ssh)
        if [[ -n $auth_key_file ]]; then
            args+=(--auth-key "file:${auth_key_file}")
            info "Using the supplied auth-key file (contents are not displayed)"
        else
            info "Tailscale will show a browser authorization URL if this host is not enrolled"
        fi
        run_ts "${args[@]}"
    elif (( run_ssh )); then
        run_ts set --ssh=true
    fi

    state=$(backend_state)
    [[ $state == Running ]] || {
        err "Tailscale did not reach Running state (state=${state:-unknown})"
        return 1
    }
    ok "Tailscale authenticated and Running"

    if (( ! configure_serve )); then
        warn "Serve configuration skipped (--no-serve)"
        return 0
    fi
    local current
    current=$(serve_status || true)
    if [[ -n $current && $current != *"No serve config"* && $current != *"not configured"* ]]; then
        if (( ! force_serve )); then
            warn "Existing Tailscale Serve configuration detected; leaving it unchanged"
            info "Use --force-serve only after reviewing 'sudo tailscale serve status'"
            return 0
        fi
        warn "Replacing the existing Tailscale Serve configuration"
    fi
    if local_health >/dev/null; then
        local serve_output
        if (( EUID == 0 )); then
            if serve_output=$(timeout 30 "$TAILSCALE_BIN" serve --bg "$port" 2>&1); then
                ok "Mnemes is privately proxied through Tailscale Serve"
                printf '%s\n' "$serve_output"
                run_ts serve status | sed -n '1,20p'
            else
                printf '%s\n' "$serve_output"
                warn "Tailscale is enrolled, but Serve is not enabled for this tailnet or needs admin approval"
                info "Enable Serve in the Tailscale admin URL above, then rerun: $0 --apply --no-ssh"
            fi
        elif serve_output=$(timeout 30 sudo "$TAILSCALE_BIN" serve --bg "$port" 2>&1); then
            ok "Mnemes is privately proxied through Tailscale Serve"
            printf '%s\n' "$serve_output"
            run_ts serve status | sed -n '1,20p'
        else
            printf '%s\n' "$serve_output"
            warn "Tailscale is enrolled, but Serve is not enabled for this tailnet or needs admin approval"
            info "Enable Serve in the Tailscale admin URL above, then rerun: $0 --apply --no-ssh"
        fi
    else
        warn "Mnemes is not healthy on 127.0.0.1:${port}; Serve was not configured"
        info "Start mnemes.service, then rerun: $0 --apply --no-ssh"
    fi
}

if (( apply )); then
    apply_configuration
else
    print_audit
fi
