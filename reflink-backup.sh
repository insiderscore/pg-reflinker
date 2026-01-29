#!/bin/bash

set -e -x

DEST_BASE="/pgfollower-clones"
TAG=${TAG:-$(date --iso=s)}
TMP_BACKUP=".tmp-$TAG"
if [ $# -eq 0 ]; then
  SUBCMD="cp -a --reflink=always /var/lib/postgresql/data/pgdata $DEST_BASE/$TMP_BACKUP"
else
  SUBCMD=$(printf '%q ' "$@" | sed 's/ $//')
fi
psql -v ON_ERROR_STOP=1 -v TAG="$TAG" <<EOF
SELECT pg_backup_start('reflinker-' || :'TAG', true);
\! $SUBCMD
\t
\a
\o $DEST_BASE/$TMP_BACKUP/backup_label
SELECT labelfile from pg_backup_stop(false);
\o
EOF
mv $DEST_BASE/$TMP_BACKUP $DEST_BASE/$TAG
ln -sr $DEST_BASE/$TAG/ $DEST_BASE/latest