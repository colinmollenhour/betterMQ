#!/usr/bin/env bash
# Restore a BetterMQ data directory from backup.sh output.
# Usage: ./restore.sh backup.tgz /path/to/parent-dir [--force]
# Stop the broker first. --force retains the previous tree beside the restore.
set -euo pipefail

ARCHIVE="${1:?usage: restore.sh <backup.tgz> <parent-dir>}"
PARENT="${2:?usage: restore.sh <backup.tgz> <parent-dir>}"
FORCE="${3:-}"
[[ $# -le 3 && ( -z "$FORCE" || "$FORCE" == --force ) ]] || {
  echo "usage: restore.sh <backup.tgz> <parent-dir> [--force]" >&2; exit 1;
}
umask 077

if [[ ! -f "$ARCHIVE" ]]; then
  echo "error: archive not found: $ARCHIVE" >&2
  exit 1
fi

if [[ -f "${ARCHIVE}.sha256" ]]; then
  EXPECTED="$(awk '{print $1; exit}' "${ARCHIVE}.sha256")"
  if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL="$(sha256sum "$ARCHIVE" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    ACTUAL="$(shasum -a 256 "$ARCHIVE" | awk '{print $1}')"
  else
    echo "error: checksum sidecar exists but no SHA-256 utility is available" >&2
    exit 1
  fi
  [[ "$EXPECTED" == "$ACTUAL" ]] || {
    echo "error: backup checksum mismatch" >&2
    exit 1
  }
fi

# Check listing status explicitly (process substitution hides tar failures).
LISTING="$(tar -tzf "$ARCHIVE")"
ROOT=""
while IFS= read -r entry; do
  case "$entry" in
    /*|../*|*/../*|*/..|./*|.|"") echo "error: unsafe archive entry: $entry" >&2; exit 1 ;;
  esac
  root="${entry%%/*}"
  [[ -z "$ROOT" || "$ROOT" == "$root" ]] || {
    echo "error: archive must have exactly one root" >&2; exit 1;
  }
  ROOT="$root"
done <<< "$LISTING"

mkdir -p "$PARENT"
TARGET="$PARENT/$ROOT"
if [[ -e "$TARGET" || -L "$TARGET" ]]; then
  [[ "$FORCE" == --force ]] || {
    echo "error: $TARGET exists; stop the broker and use --force" >&2; exit 1;
  }
fi
STAGE="$(mktemp -d "$PARENT/.bettermq-restore.XXXXXXXX")"
trap 'rm -rf -- "$STAGE"' EXIT
tar -C "$STAGE" -xzf "$ARCHIVE" --no-same-owner
[[ -d "$STAGE/$ROOT" && ! -L "$STAGE/$ROOT" ]] || {
  echo "error: archive root must be a directory" >&2; exit 1;
}
if [[ -e "$TARGET" || -L "$TARGET" ]]; then
  PREVIOUS="$(mktemp -d "$PARENT/$ROOT.pre-restore-$(date -u +%Y%m%dT%H%M%SZ).XXXXXXXX")"
  mv -- "$TARGET" "$PREVIOUS/data"
  echo "previous tree retained at $PREVIOUS/data"
fi
mv -- "$STAGE/$ROOT" "$TARGET"
echo "restored into $TARGET"
echo "run: bettermq doctor --data-dir <restored-directory>"
echo "restart bettermq with --data-dir pointing at the restored directory"
echo "if using shared meta / Slate, restore those stores separately before start"
