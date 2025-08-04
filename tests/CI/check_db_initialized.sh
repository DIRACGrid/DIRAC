#!/bin/bash
DB_USER="Dirac"
DB_PASS="Dirac"
DB_HOST="mysql"
DB_PORT=3306
DB_CMD=""

# Detect available client: maria or mysql
if command -v mariadb >/dev/null 2>&1; then
    DB_CMD="mariadb -u${DB_USER} -p${DB_PASS} -h${DB_HOST} -P${DB_PORT}"
elif command -v mysql >/dev/null 2>&1; then
    DB_CMD="mysql -u${DB_USER} -p${DB_PASS} -h${DB_HOST} -P${DB_PORT}"
else
    echo "❌ Neither mysql nor mariadb client found in PATH."
    exit 1
fi

echo "Using client: ${DB_CMD%% *}"

dbMissing=true
allDBs=(
  AccountingDB FTS3DB JobDB JobLoggingDB PilotAgentsDB ProductionDB
  ProxyDB ReqDB ResourceManagementDB ResourceStatusDB
  SandboxMetadataDB StorageManagementDB TaskQueueDB TransformationDB
)

while $dbMissing; do
  dbMissing=false
  allExistingDBs=$($DB_CMD -e "SHOW DATABASES;" 2>/dev/null)

  for db in "${allDBs[@]}"; do
    if grep -q "^${db}$" <<<"$allExistingDBs"; then
      echo "✅ ${db} exists"
    else
      echo "⚠️ ${db} not created yet"
      dbMissing=true
    fi
  done

  $dbMissing && sleep 1
done

echo "🎉 All databases are present."
