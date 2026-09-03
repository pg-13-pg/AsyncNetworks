#!/usr/bin/env bash

# Install the native Ubuntu build, test, benchmark, and observability tools.
# Docker is intentionally not installed: UCP is deployed and benchmarked as a
# native Linux process.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/install_dependencies.sh [--build-only] [--help]

Install Ubuntu packages required by UCP Proxy Core.

  --build-only  Install only compiler and CMake dependencies.
  --help        Show this message.
EOF
}

build_only=false
while (($# > 0)); do
    case "$1" in
    --build-only)
        build_only=true
        ;;
    --help|-h)
        usage
        exit 0
        ;;
    *)
        printf 'Unknown option: %s\n' "$1" >&2
        usage >&2
        exit 2
        ;;
    esac
    shift
done

if [[ ! -r /etc/os-release ]]; then
    printf 'Unsupported system: /etc/os-release is unavailable. Ubuntu is required.\n' >&2
    exit 1
fi

# shellcheck disable=SC1091
. /etc/os-release
if [[ "${ID:-}" != "ubuntu" ]]; then
    printf 'Unsupported distribution: %s. This installer supports Ubuntu only.\n' "${PRETTY_NAME:-unknown}" >&2
    exit 1
fi

if ! command -v apt-get >/dev/null 2>&1; then
    printf 'apt-get is required but was not found.\n' >&2
    exit 1
fi

sudo_command=()
if ((EUID != 0)); then
    if ! command -v sudo >/dev/null 2>&1; then
        printf 'Run this script as root, or install sudo and run it again.\n' >&2
        exit 1
    fi
    sudo_command=(sudo)
fi

build_packages=(
    build-essential
    cmake
    pkg-config
    liburing-dev
    libfmt-dev
)

development_packages=(
    git
    ca-certificates
    curl
)

benchmark_packages=(
    wrk
    sysstat
    procps
    iproute2
)

packages=("${build_packages[@]}" "${development_packages[@]}")
if [[ "$build_only" == false ]]; then
    packages+=("${benchmark_packages[@]}")
fi

"${sudo_command[@]}" apt-get update
DEBIAN_FRONTEND=noninteractive "${sudo_command[@]}" apt-get install --yes --no-install-recommends "${packages[@]}"

printf '\nInstalled UCP native dependencies on %s.\n' "${PRETTY_NAME}"
printf 'Verify the toolchain with:\n'
printf '  cmake --version\n  c++ --version\n  pkg-config --modversion liburing fmt\n'
if [[ "$build_only" == false ]]; then
    printf '  wrk --version\n  mpstat -V\n  ss -V\n'
fi
