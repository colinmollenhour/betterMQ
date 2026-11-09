#!/usr/bin/env bash
set -euo pipefail
SCRIPTS="$(cd "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf -- "$TMP"' EXIT
mkdir -p "$TMP/source/data/segments" "$TMP/dest/data/segments"
echo old > "$TMP/source/data/active.wal"
echo newer > "$TMP/dest/data/segments/new.wal"
echo job > "$TMP/dest/data/schedule.journal"
tar -C "$TMP/source" -czf "$TMP/backup.tgz" data
if bash "$SCRIPTS/restore.sh" "$TMP/backup.tgz" "$TMP/dest"; then
  echo 'must reject existing target' >&2; exit 1
fi
bash "$SCRIPTS/restore.sh" "$TMP/backup.tgz" "$TMP/dest" --force
test ! -e "$TMP/dest/data/segments/new.wal"
test ! -e "$TMP/dest/data/schedule.journal"
test "$(cat "$TMP/dest/data/active.wal")" = old
umask 022
bash "$SCRIPTS/backup.sh" "$TMP/source/data" "$TMP/private.tgz"
test "$(stat -c %a "$TMP/private.tgz")" = 600
test "$(stat -c %a "$TMP/private.tgz.sha256")" = 600
if bash "$SCRIPTS/backup.sh" "$TMP/source/data" "$TMP/private.tgz"; then
  echo 'must reject existing backup' >&2; exit 1
fi
test -f "$TMP"/dest/data.pre-restore-*/data/schedule.journal
echo broken > "$TMP/broken.tgz"
if bash "$SCRIPTS/restore.sh" "$TMP/broken.tgz" "$TMP/dest" --force; then
  exit 1
fi
test "$(cat "$TMP/dest/data/active.wal")" = old
