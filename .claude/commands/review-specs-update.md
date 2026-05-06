# review-specs-update

Review a Dependabot PR that bumps the `tests/specifications` submodule.

## Usage

`/review-specs-update <PR number>`

## Workflow

### 1. Fetch PR details

```bash
gh pr view <PR> --repo mongodb/mongo-php-library --json title,body,commits,files
```

Extract the list of specifications commits from the PR body (the `<ul>` under "Commits"). Each entry contains a commit SHA and a DRIVERS-XXXX ticket reference.

### 2. List changed spec files

```bash
gh api repos/mongodb/specifications/compare/<old_sha>...<new_sha> --jq '.files[].filename'
```

The old and new SHAs are in the PR body (`from \`XXXXXXX\` to \`XXXXXXX\``).

### 3. Check CI status

```bash
gh pr checks <PR> --repo mongodb/mongo-php-library
```

### 4. For each DRIVERS ticket: find the PHPLIB/PHPC split-to ticket

Use WebFetch on `https://jira.mongodb.org/browse/DRIVERS-XXXX` — Jira is public.

Look for the **"Split to"** linked issues (PHPLIB-XXXX or PHPC-XXXX).

If WebFetch fails (login redirect), try a JQL search:

`https://jira.mongodb.org/issues/?jql=project%20in%20(PHPLIB%2C%20PHPC)%20AND%20text%20~%20%22DRIVERS-XXXX%22`

### 5. Identify if code/test changes are needed

Cross-reference the changed spec files with existing PHP tests:

- `source/transactions-convenient-api/` → `tests/UnifiedSpecTests/` + `withTransaction` tests
- `source/client-backpressure/` → search for backpressure in `tests/`
- `source/change-streams/` → `tests/UnifiedSpecTests/` change-stream tests
- `source/retryable-reads/` or `source/retryable-writes/` → corresponding spec tests

Check if new YAML/JSON test files in the specs have corresponding entries in PHP test runners or are automatically picked up.

### 6. Conclusion

**If CI passes and no code changes needed:**
1. Approve and squash-merge the PR:
```bash
gh pr review <PR> --repo mongodb/mongo-php-library --approve
gh pr merge <PR> --repo mongodb/mongo-php-library --squash
```
2. For each PHPLIB/PHPC ticket found, note that a comment should be added:
> "Spec tests updated in mongodb/mongo-php-library#PR"

**If CI fails:** Identify the failing test and add it to `$incompleteList` in the relevant test class to skip it. The PHPLIB/PHPC ticket already exists — just note it needs implementation work.

## Output format

Present a summary table:

| Commit | DRIVERS ticket | PHPLIB/PHPC ticket | Changed specs | Action needed |
|---|---|---|---|---|
| `abc1234` | DRIVERS-XXXX | PHPLIB-YYYY | `source/foo/` | Comment on PHPLIB-YYYY |

Then state clearly: **CI passes / fails** and what to do next.