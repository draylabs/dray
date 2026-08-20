# What to run, so nobody has to remember the flags.
#
# The test command is the reason this file exists. Both `--group test` and
# `--extra dsql` have to be named, and a shorter one does not fail — it passes
# having run nothing, because `uv run pytest` resolves a pytest that has never
# heard of dray. A green run that tested nothing is the failure worth spending
# a file on.
#
# `just --list` shows only the last comment line above a recipe, so each one
# ends with a sentence that stands alone.

set shell := ["bash", "-uc"]

# Every core but one, so the machine running the suite is still usable while it
# runs. Asked of the machine rather than written down, because the number is
# different in development and on CI.
workers := `expr $(getconf _NPROCESSORS_ONLN) - 1`

default:
    @just --list

# The suite, across every core but one. Takes pytest's own arguments: `just test -k blob`.
test *args:
    uv run --group test --extra dsql pytest tests -q -n {{ workers }} {{ args }}

# The suite from nothing, so a pass cannot be borrowing an old environment.
fresh *args:
    rm -rf .venv
    uv run --group test --extra dsql pytest tests -q -n {{ workers }} {{ args }}

# A cluster's hostname, found by its Name tag: `export DRAY_DSQL_HOST=$(just connect)`.
connect tag="dray" region="":
    @set -o pipefail; \
    where="{{ region }}"; \
    [ -n "$where" ] || where=$(aws configure get region 2>/dev/null); \
    [ -n "$where" ] || { echo "no region given and none configured — pass one, or set it with \`aws configure\`" >&2; exit 1; }; \
    found=$(aws dsql list-clusters --region "$where" --query 'clusters[].arn' \
              --output text 2>/dev/null | tr '\t' '\n' | while read -r arn; do \
        [ -n "$arn" ] || continue; \
        name=$(aws dsql list-tags-for-resource --region "$where" \
                 --resource-arn "$arn" --query 'tags.Name' --output text 2>/dev/null); \
        case "$name" in *{{ tag }}*) echo "${arn##*/}.dsql.$where.on.aws";; esac; \
    done); \
    case $(printf '%s' "$found" | grep -c .) in \
      0) echo "no cluster in $where whose Name tag holds '{{ tag }}'" >&2; exit 1;; \
      1) printf '%s\n' "$found";; \
      *) echo "several match '{{ tag }}' — narrow it:" >&2; printf '%s\n' "$found" >&2; exit 1;; \
    esac

# The half local PostgreSQL cannot answer. Needs DRAY_DSQL_HOST.
cluster *args:
    uv run --group test --extra dsql python scripts/against_dsql.py {{ args }}

# One page's work as a tree, with DSQL's plan under each read. Needs DRAY_DSQL_HOST.
flow *args:
    uv run --group test --extra dsql python scripts/flow.py {{ args }}

# Redraw docs/reference.html. Needs no database.
reference:
    uv run --group test --extra dsql python scripts/reference.py

# What a pull request has to survive. Deliberately not the cluster.
check: test
