#!/usr/bin/env bash
set -euo pipefail

# Personal SQL lint entrypoint.
# Source style guide (preferred local path):
#   /Users/am/Sync/fpl-workspace/.windsurf/rules/dbt-style-guide.md
# CI fallback copy:
#   docs/style/dbt-style-guide.md
# CI will execute this script if it exists.

DBT_PROJECT_DIR="${DBT_PROJECT_DIR:-.}"
DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-.github/dbt}"
STYLE_GUIDE_FILE="${STYLE_GUIDE_FILE:-/Users/am/Sync/fpl-workspace/.windsurf/rules/dbt-style-guide.md}"
STYLE_GUIDE_FALLBACK="${STYLE_GUIDE_FALLBACK:-docs/style/dbt-style-guide.md}"
SQLFLUFF_CONFIG="${SQLFLUFF_CONFIG:-.sqlfluff}"

CMD=(
  sqlfluff lint
  --dialect snowflake
  --templater dbt
  --project-dir "${DBT_PROJECT_DIR}"
  --profiles-dir "${DBT_PROFILES_DIR}"
  models tests macros
)

if [[ -f "${STYLE_GUIDE_FILE}" ]]; then
  echo "Using style guide: ${STYLE_GUIDE_FILE}"
elif [[ -f "${STYLE_GUIDE_FALLBACK}" ]]; then
  echo "Using style guide fallback: ${STYLE_GUIDE_FALLBACK}"
else
  echo "Style guide file not found at '${STYLE_GUIDE_FILE}' or '${STYLE_GUIDE_FALLBACK}'" >&2
  exit 1
fi

if [[ -f "${SQLFLUFF_CONFIG}" ]]; then
  CMD+=(--config "${SQLFLUFF_CONFIG}")
fi

"${CMD[@]}"
