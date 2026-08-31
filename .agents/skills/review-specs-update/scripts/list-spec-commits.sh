#!/usr/bin/env bash
set -euo pipefail

# Usage: list-spec-commits.sh <PR_NUMBER>
#
# Prints the old/new tests/specifications submodule SHA for a given
# mongodb/mongo-php-library pull request, lists the commits between them,
# and highlights any DRIVERS-XXXX ticket references found in those commits.
#
# Read-only: does not call Jira or write anything to GitHub.

if [ $# -ne 1 ]; then
    echo "Usage: $0 <PR_NUMBER>" >&2
    exit 1
fi

PR_NUMBER="$1"
REPO="mongodb/mongo-php-library"
SUBMODULE_PATH="tests/specifications"
UPSTREAM_REPO="mongodb/specifications"

DIFF=$(gh pr diff "$PR_NUMBER" --repo "$REPO")

OLD_SHA=$(echo "$DIFF" | grep -m1 -- "-Subproject commit" | awk '{print $3}')
NEW_SHA=$(echo "$DIFF" | grep -m1 -- "+Subproject commit" | awk '{print $3}')

if [ -z "$OLD_SHA" ] || [ -z "$NEW_SHA" ]; then
    echo "Could not find a $SUBMODULE_PATH submodule pointer change in PR #$PR_NUMBER" >&2
    exit 1
fi

echo "tests/specifications: $OLD_SHA -> $NEW_SHA"
echo

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"

if git -C "$REPO_ROOT/$SUBMODULE_PATH" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    if ! git -C "$REPO_ROOT/$SUBMODULE_PATH" cat-file -e "$NEW_SHA" 2>/dev/null; then
        git -C "$REPO_ROOT/$SUBMODULE_PATH" fetch origin --quiet
    fi
    COMMITS=$(git -C "$REPO_ROOT/$SUBMODULE_PATH" log --oneline "$OLD_SHA..$NEW_SHA")

    # Classify a changed file into a category label, given its git status letter (A/M/D/...).
    classify_file() {
        local status="$1" path="$2" kind action

        case "$path" in
            */tests/*.yml | */tests/*.json)
                kind="unified test"
                ;;
            */tests/*.md)
                kind="prose test"
                ;;
            *.md)
                kind="spec"
                ;;
            *)
                kind="other file"
                ;;
        esac

        case "$status" in
            A) action="added" ;;
            D) action="deleted" ;;
            *) action="updated" ;;
        esac

        echo "$kind $action"
    }

    echo "$COMMITS" | while IFS= read -r line; do
        SHA=$(echo "$line" | cut -d' ' -f1)
        echo "$line"
        git -C "$REPO_ROOT/$SUBMODULE_PATH" show --name-status --pretty=format: "$SHA" | sed '/^$/d' | while IFS=$'\t' read -r status path new_path; do
            # Renames report as "R100 old/path new/path"; keep the new path and treat as an update.
            if [ -n "$new_path" ]; then
                status=M
                path="$new_path"
            fi
            label=$(classify_file "${status:0:1}" "$path")
            printf '    [%s] %s\n' "$label" "$path"
        done
        echo
    done
    echo "DRIVERS tickets referenced:"
    echo "$COMMITS" | grep -oE 'DRIVERS-[0-9]+' | sort -u -t- -k2 -n || echo "(none found)"
else
    echo "$SUBMODULE_PATH is not initialized locally." >&2
    echo "Run 'git submodule update --init $SUBMODULE_PATH' to list commits locally, or compare manually at:" >&2
    echo "https://github.com/$UPSTREAM_REPO/compare/$OLD_SHA...$NEW_SHA" >&2
    exit 0
fi
