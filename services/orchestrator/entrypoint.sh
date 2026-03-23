#!/bin/bash
set -e

echo "Generating dbt manifest..."
dbt parse --project-dir /app/src/dbt_project --profiles-dir /app/src/dbt_project

exec "$@"
