#!/usr/bin/env bash
#
# mnemes — install or update
#
# Usage:
#   ./install.sh              # install from crates.io
#   ./install.sh --from-source # build and install from local source
#   ./install.sh --check       # check if installed and up to date
#   ./install.sh --uninstall   # remove mnemes binaries
#
set -euo pipefail

CRATE_NAME="mnemes"
CRATE_VERSION="0.1.0"
BINS=("mnemes-server" "mnemes-admin")

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m'

info()  { echo -e "${BLUE}ℹ${NC}  $*"; }
ok()    { echo -e "${GREEN}✓${NC}  $*"; }
warn()  { echo -e "${YELLOW}⚠${NC}  $*"; }
err()   { echo -e "${RED}✗${NC}  $*"; }
header() { echo -e "\n${PURPLE}═══ $* ═══${NC}\n"; }

# --- Pre-flight checks ---

check_rust() {
    if ! command -v cargo &>/dev/null; then
        err "Rust/Cargo is not installed."
        echo ""
        info "Install Rust via rustup:"
        echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
        exit 1
    fi
}

get_installed_version() {
    cargo install --list 2>/dev/null | grep "^${CRATE_NAME} " | head -1 | awk '{print $2}' | tr -d '()'
}

is_installed() {
    local ver
    ver=$(get_installed_version 2>/dev/null || true)
    [[ -n "$ver" ]]
}

# --- Actions ---

do_check() {
    header "Checking ${CRATE_NAME}"
    if is_installed; then
        local ver
        ver=$(get_installed_version)
        ok "${CRATE_NAME} v${ver} is installed"
        if [[ "$ver" == "$CRATE_VERSION" ]]; then
            ok "Already at latest version (${CRATE_VERSION})"
        else
            warn "Installed: v${ver}, Latest: v${CRATE_VERSION}"
            info "Run ./install.sh to update"
        fi
        echo ""
        info "Binaries:"
        for bin in "${BINS[@]}"; do
            if command -v "$bin" &>/dev/null; then
                ok "  $bin → $(command -v "$bin")"
            else
                warn "  $bin not found in PATH"
            fi
        done
    else
        warn "${CRATE_NAME} is not installed"
        info "Run ./install.sh to install"
        exit 1
    fi
}

do_install_crate() {
    header "Installing ${CRATE_NAME} from crates.io"
    check_rust

    if is_installed; then
        local ver
        ver=$(get_installed_version)
        info "Currently installed: v${ver}"
        if [[ "$ver" == "$CRATE_VERSION" ]]; then
            ok "Already at target version v${CRATE_VERSION}"
            read -rp "Reinstall anyway? [y/N] " confirm
            [[ "$confirm" =~ ^[Yy]$ ]] || exit 0
        else
            info "Updating to v${CRATE_VERSION}..."
        fi
    else
        info "Installing v${CRATE_VERSION}..."
    fi

    cargo install "$CRATE_NAME" --locked --force
    ok "Installation complete"
    print_postinstall
}

do_install_source() {
    header "Installing ${CRATE_NAME} from source"
    check_rust

    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

    if [[ ! -f "$script_dir/Cargo.toml" ]]; then
        err "No Cargo.toml found in $script_dir"
        err "Run this script from the mnemes repository root"
        exit 1
    fi

    info "Building and installing from $script_dir..."
    cargo install --path "$script_dir" --locked --force
    ok "Installation complete"
    print_postinstall
}

do_uninstall() {
    header "Uninstalling ${CRATE_NAME}"
    check_rust

    if is_installed; then
        info "Removing ${CRATE_NAME}..."
        cargo uninstall "$CRATE_NAME"
        ok "Uninstalled"
    else
        warn "${CRATE_NAME} is not installed"
    fi
}

print_postinstall() {
    header "Next steps"
    echo "  1. Bootstrap a new store:"
    echo "     mnemes-admin bootstrap ~/.local/share/mnemes laptop linux \$(hostname)"
    echo ""
    echo "  2. Start the server:"
    echo "     mnemes-server 3000 ~/.local/share/mnemes"
    echo ""
    echo "  3. Check health:"
    echo "     curl http://127.0.0.1:3000/v1/health"
    echo ""
    echo "  4. As a library dependency:"
    echo "     # Cargo.toml"
    echo "     [dependencies]"
    echo "     mnemes = \"${CRATE_VERSION}\""
    echo ""
    info "Docs: https://docs.rs/mnemes"
    info "Repo: https://github.com/RecursiveIntell/mnemes"
}

# --- Main ---

main() {
    local action="${1:-install}"

    case "$action" in
        install|--install)
            do_install_crate
            ;;
        --from-source|from-source)
            do_install_source
            ;;
        --check|check)
            do_check
            ;;
        --uninstall|uninstall)
            do_uninstall
            ;;
        --help|-h|help)
            echo "mnemes install script"
            echo ""
            echo "Usage:"
            echo "  ./install.sh               Install from crates.io (default)"
            echo "  ./install.sh --from-source  Build and install from local source"
            echo "  ./install.sh --check        Check if installed and up to date"
            echo "  ./install.sh --uninstall    Remove mnemes binaries"
            echo ""
            echo "Requirements:"
            echo "  Rust 1.75+ (install via https://rustup.rs)"
            ;;
        *)
            err "Unknown action: $action"
            echo "Run ./install.sh --help for usage"
            exit 1
            ;;
    esac
}

main "$@"