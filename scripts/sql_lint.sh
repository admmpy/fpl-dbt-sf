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
  models tests macros
)

if [[ -f "${STYLE_GUIDE_FILE}" ]]; then
  echo "Using style guide: ${STYLE_GUIDE_FILE}"
elif [[ -f "${STYLE_GUIDE_FALLBACK}" ]]; then
  echo "Using style guide fallback: ${STYLE_GUIDE_FALLBACK}"
else
  echo "Style guide file not found at '${STYLE_GUIDE_FILE}' or '${STYLE_GUIDE_FALLBACK}'." >&2
  echo "Continuing with SQLFluff config only." >&2
fi

if [[ -f "${SQLFLUFF_CONFIG}" ]]; then
  CMD+=(--config "${SQLFLUFF_CONFIG}")
fi

"${CMD[@]}"

# Additional enforceable style-guide checks that are not fully covered by SQLFluff.
failed=0

echo "Running additional style-guide policy checks..."

# 1) Prefer ON over USING in JOINs.
if rg -n --glob '*.sql' -P '(?i)\busing\s*\(' models; then
  echo "Policy failed: Use ON over USING in JOIN conditions." >&2
  failed=1
fi

# 2) Prefer UNION ALL over UNION.
if rg -n --glob '*.sql' -P '(?i)\bunion\b(?!\s+all\b)' models; then
  echo "Policy failed: Use UNION ALL instead of UNION." >&2
  failed=1
fi

# 3) Prefer COALESCE over IFNULL/NVL.
if rg -n --glob '*.sql' -P '(?i)\b(ifnull|nvl)\s*\(' models; then
  echo "Policy failed: Use COALESCE instead of IFNULL/NVL." >&2
  failed=1
fi

# 4) If a model uses CTEs (WITH), require a final CTE named `final`.
while IFS= read -r file; do
  if rg -n -P '(?i)^\s*with\b' "${file}" >/dev/null; then
    if ! rg -n -P '(?i)\bfinal\s+as\s*\(' "${file}" >/dev/null; then
      echo "Policy failed: ${file} uses WITH but has no final CTE (final AS (...))." >&2
      failed=1
    fi
  fi
done < <(find models -type f -name '*.sql' | sort)

if [[ "${failed}" -ne 0 ]]; then
  echo "One or more style-guide policy checks failed." >&2
  exit 1
fi
