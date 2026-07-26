#!/usr/bin/env bash
# MySQL 每日全备(设计方案 12 章:RPO=1日;血缘/闭包可由 SQL 仓库重建,可再生)
# crontab: 30 1 * * * /data/datavein/deploy/backup.sh >> /var/log/datavein-backup.log 2>&1
set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/data/datavein/backup}"
RETENTION_DAYS="${BACKUP_RETENTION_DAYS:-14}"
STAMP="$(date +%F)"

docker compose -f "$(dirname "$0")/docker-compose.prod.yml" exec -T mysql \
  sh -c 'exec mysqldump --single-transaction --routines --set-gtid-purged=OFF \
         -uroot -p"$MYSQL_ROOT_PASSWORD" "$MYSQL_DATABASE"' \
  | gzip > "${BACKUP_DIR}/datavein_${STAMP}.sql.gz"

# 校验产物非空
[ -s "${BACKUP_DIR}/datavein_${STAMP}.sql.gz" ] || { echo "backup empty!"; exit 1; }

find "${BACKUP_DIR}" -name 'datavein_*.sql.gz' -mtime "+${RETENTION_DAYS}" -delete
echo "$(date -Is) backup ok: datavein_${STAMP}.sql.gz ($(du -h "${BACKUP_DIR}/datavein_${STAMP}.sql.gz" | cut -f1))"
