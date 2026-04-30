#!/bin/bash
set -e

echo "Installing dbt packages..."
dbt deps --project-dir /app/src/dbt_project --profiles-dir /app/src/dbt_project

echo "Generating dbt manifest..."
dbt parse --project-dir /app/src/dbt_project --profiles-dir /app/src/dbt_project

exec "$@"
