#!/bin/bash
#
# Generate a CycloneDX 1.6 "operations SBOM" for an environment that has
# Percona Backup for MongoDB installed from a Percona repository.
#
# What this script does:
#   1. Installs PBM from the requested Percona repo channel (laboratory /
#      testing / experimental / release) inside the current environment
#      (typically a fresh build container).
#   2. Runs Syft against the resulting filesystem with a focused set of
#      catalogers:
#        * dpkg-db-cataloger          - Debian/Ubuntu installed packages
#        * rpm-db-cataloger           - RHEL/Oracle/Amazon Linux installed packages
#        * go-module-binary-cataloger - Go modules embedded in pbm/pbm-agent
#   3. Writes a CycloneDX 1.6 JSON SBOM and validates it against the schema.
#
# This is an OPERATIONS SBOM (CycloneDX lifecycle phase = "operations"):
# it describes the state of the container/system AFTER PBM has been
# installed. It is meant to be published as a supplementary artifact next
# to the packages in the Percona downloads area.
#
# This script is NOT used to produce:
#   * The SBOM inside .deb / .rpm packages
#     (/usr/share/doc/percona-backup-mongodb/sbom.cdx.json) - that is
#     generated inline from debian/rules and the RPM spec %install
#     section using syft directly against the binary in the staging dir.
#   * The canonical SBOM in the repository root - produced by a dedicated
#     GitHub Actions workflow.
#   * The Docker image SBOM - produced by scanning the built image with
#     syft and attaching it via oras as an OCI 1.1 referrer artifact in
#     the percona-docker build pipeline.
#

set -euo pipefail

# cyclonedx-cli is a .NET application that by default requires libicu for
# globalization support. We don't need any locale-sensitive behaviour for
# JSON parsing / schema validation, so run it in invariant mode to avoid
# pulling libicu into the build container.
# See: https://aka.ms/dotnet-missing-libicu
export DOTNET_SYSTEM_GLOBALIZATION_INVARIANT=true

CYCLONEDX_CLI_VERSION="v0.27.2"
SYFT_INSTALL_URL="https://raw.githubusercontent.com/anchore/syft/main/install.sh"

# --- Defaults / required parameters --------------------------------------
WORKDIR="$(pwd -P)"
PBM_VERSION=""
REPO_TYPE=""
GIT_REPO=""
GIT_BRANCH=""
OUTPUT=""
SKIP_INSTALL="0"

usage() {
    cat <<EOF
Usage: $0 --pbm_version=VERSION [OPTIONS]

Generate a CycloneDX 1.6 SBOM for the Percona Backup for MongoDB package
installed in the current environment.

Required:
  --pbm_version=VERSION     Product version (e.g. 2.14.0). No default.

Legacy CLI compatibility (used by Jenkins job hetzner-pbm_generate_sbom):
  --builddir=DIR            Working directory (default: current dir)
  --repo_type=TYPE          Percona repository to enable:
                            laboratory | testing | experimental | release
  --git_repo=URL            Informational only (kept for compatibility)
  --git_branch=NAME         Informational only (kept for compatibility)

Optional:
  --output=PATH             Override output SBOM file path.
                            Default: <builddir>/pbm_sbom/sbom-percona-backup-mongodb-<version>-<platform>-<arch>.cdx.json
  --skip-install            Do not install PBM; assume it is already
                            installed in the current environment.
  --help                    Show this help and exit.

Example:
  $0 --pbm_version=2.14.0 --repo_type=testing
EOF
    exit 1
}

# --- Argument parsing ----------------------------------------------------
for arg in "$@"; do
    val="${arg#*=}"
    case "$arg" in
        --builddir=*)     WORKDIR="$val" ;;
        --pbm_version=*)  PBM_VERSION="$val" ;;
        --repo_type=*)    REPO_TYPE="$val" ;;
        --git_repo=*)     GIT_REPO="$val" ;;
        --git_branch=*)   GIT_BRANCH="$val" ;;
        --output=*)       OUTPUT="$val" ;;
        --skip-install)   SKIP_INSTALL="1" ;;
        --help|-h)        usage ;;
        *)
            echo "ERROR: unknown argument: $arg" >&2
            usage
            ;;
    esac
done

if [[ -z "$PBM_VERSION" ]]; then
    echo "ERROR: --pbm_version is required (no default fallback)." >&2
    usage
fi

if [[ "$SKIP_INSTALL" = "0" && -z "$REPO_TYPE" ]]; then
    echo "ERROR: --repo_type is required unless --skip-install is given." >&2
    usage
fi

export DEBIAN_FRONTEND=noninteractive

# --- Platform detection --------------------------------------------------
if [[ ! -f /etc/os-release ]]; then
    echo "ERROR: cannot detect OS (no /etc/os-release)." >&2
    exit 1
fi
# shellcheck disable=SC1091
. /etc/os-release
PLATFORM_ID="$(echo "${ID:-}"            | tr '[:upper:]' '[:lower:]')"
VERSION_ID="$(echo  "${VERSION_ID:-}"    | tr -d '"')"
CODENAME="$(echo    "${VERSION_CODENAME:-}" | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"

case "$PLATFORM_ID" in
    ol|rhel|centos|rocky|almalinux|oraclelinux)
        RHEL_MAJOR="$(rpm --eval %rhel)"
        PLATFORM="${PLATFORM_ID}${RHEL_MAJOR}"
        ;;
    amzn)
        AMZN_MAJOR="$(rpm --eval %amzn)"
        PLATFORM="${PLATFORM_ID}${AMZN_MAJOR}"
        ;;
    ubuntu|debian)
        if [[ -z "$CODENAME" ]]; then
            echo "ERROR: VERSION_CODENAME missing in /etc/os-release on ${PLATFORM_ID}." >&2
            exit 1
        fi
        PLATFORM="$CODENAME"
        ;;
    *)
        echo "ERROR: unsupported platform: $PLATFORM_ID" >&2
        exit 1
        ;;
esac

# --- Toolchain installation ---------------------------------------------
install_dependencies() {
    case "$PLATFORM_ID" in
        ol|rhel|centos|rocky|almalinux|oraclelinux)
            dnf install -y jq tar curl
            dnf install -y 'dnf-command(config-manager)' || true
            dnf config-manager --set-enabled "ol${RHEL_MAJOR}_codeready_builder" || true
            ;;
        amzn)
            dnf install -y jq tar curl
            dnf install -y 'dnf-command(config-manager)' || true
            ;;
        ubuntu|debian)
            apt-get update
            apt-get install -y --no-install-recommends curl gnupg jq lsb-release ca-certificates
            apt-get --fix-broken install -y || true
            ;;
    esac
}

install_percona_backup_mongodb() {
    case "$PLATFORM_ID" in
        ol|rhel|centos|rocky|almalinux|oraclelinux|amzn)
            curl -fsSL -O https://repo.percona.com/yum/percona-release-latest.noarch.rpm
            dnf install -y ./percona-release-latest.noarch.rpm
            percona-release enable pbm "$REPO_TYPE"
            dnf install -y percona-backup-mongodb
            ;;
        ubuntu|debian)
            curl -fsSL -O https://repo.percona.com/apt/percona-release_latest.generic_all.deb
            dpkg -i percona-release_latest.generic_all.deb || apt-get --fix-broken install -y
            apt-get update
            percona-release enable telemetry || true
            percona-release enable pbm "$REPO_TYPE"
            apt-get update
            apt-get install -y percona-backup-mongodb
            ;;
    esac
}

install_syft() {
    if ! command -v syft >/dev/null 2>&1; then
        curl -fsSL "$SYFT_INSTALL_URL" | sh -s -- -b /usr/local/bin
    fi
    syft version
}

install_cyclonedx_cli() {
    if command -v cyclonedx >/dev/null 2>&1; then
        cyclonedx --version
        return
    fi
    local asset
    case "$ARCH" in
        x86_64|amd64)   asset="cyclonedx-linux-x64"   ;;
        aarch64|arm64)  asset="cyclonedx-linux-arm64" ;;
        *)
            echo "ERROR: unsupported arch for cyclonedx-cli: $ARCH" >&2
            exit 1
            ;;
    esac
    curl -fsSL -o /usr/local/bin/cyclonedx \
        "https://github.com/CycloneDX/cyclonedx-cli/releases/download/${CYCLONEDX_CLI_VERSION}/${asset}"
    chmod +x /usr/local/bin/cyclonedx
    cyclonedx --version
}

install_dependencies
if [[ "$SKIP_INSTALL" = "0" ]]; then
    install_percona_backup_mongodb
fi
install_syft
install_cyclonedx_cli

# --- SBOM generation -----------------------------------------------------
SBOM_DIR="${WORKDIR}/pbm_sbom"
mkdir -p "$SBOM_DIR"

if [[ -z "$OUTPUT" ]]; then
    OUTPUT="${SBOM_DIR}/sbom-percona-backup-mongodb-${PBM_VERSION}-${PLATFORM}-${ARCH}.cdx.json"
fi

# Catalogers we want and ONLY them:
#  * dpkg-db-cataloger      - Debian/Ubuntu installed packages
#  * rpm-db-cataloger       - RHEL/Oracle/Amazon Linux installed packages
#  * go-module-binary-cataloger - Go modules embedded in pbm / pbm-agent
#
# We also explicitly disable the "file" cataloger group via --select-catalogers.
# Syft, by default, auto-adds executable/file-metadata/file-digest catalogers
# as a sidecar to package catalogers - which results in ~10x SBOM bloat with
# per-file hashes and metadata. Those are not consumed by Trivy, Grype, or any
# typical security workflow, so we stay at package granularity here.
# (See Syft WARN: "adding 'file' tag to the default cataloger selection,
#  to override add '-file' to the cataloger selection request".)
CATALOGERS="dpkg-db-cataloger,rpm-db-cataloger,go-module-binary-cataloger"

echo "Generating CycloneDX 1.6 SBOM ..."
echo "  product:     percona-backup-mongodb ${PBM_VERSION}"
echo "  platform:    ${PLATFORM} (${ARCH})"
echo "  catalogers:  ${CATALOGERS} (with -file)"
echo "  output:      ${OUTPUT}"

syft scan "dir:/" \
    --override-default-catalogers "$CATALOGERS" \
    --select-catalogers "-file" \
    --source-name "percona-backup-mongodb" \
    --source-version "$PBM_VERSION" \
    --output "cyclonedx-json@1.6=${OUTPUT}"

# --- Validation ----------------------------------------------------------
echo "Validating SBOM against CycloneDX 1.6 schema ..."
cyclonedx validate \
    --input-file "$OUTPUT" \
    --input-version v1_6 \
    --input-format json \
    --fail-on-errors

# --- Summary -------------------------------------------------------------
TOTAL_COMPONENTS="$(jq '.components | length' "$OUTPUT")"
echo ""
echo "SBOM written: ${OUTPUT}"
echo "  spec:        $(jq -r '.specVersion' "$OUTPUT")"
echo "  serial:      $(jq -r '.serialNumber' "$OUTPUT")"
echo "  components:  ${TOTAL_COMPONENTS}"
echo ""

if [[ "$TOTAL_COMPONENTS" -lt 10 ]]; then
    echo "WARNING: SBOM contains only ${TOTAL_COMPONENTS} components - this is" >&2
    echo "         suspicious. Expected at least OS packages + several Go modules." >&2
    echo "         Verify cataloger configuration and PBM installation." >&2
fi
