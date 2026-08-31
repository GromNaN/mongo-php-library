---
name: review-specs-update
description: >-
  Use when reviewing a Dependabot PR that bumps the tests/specifications git submodule in
  mongodb/mongo-php-library: tracing DRIVERS-XXXX spec commits to PHPLIB/PHPC tickets, checking CI,
  skipping newly-broken tests with a ticket reference, and approving/squash-merging the PR.
---

# Review Specs Update

## Prerequisites

- `gh` CLI, authenticated for `mongodb/mongo-php-library` (check with `gh auth status`).
- `jira` CLI (`github.com/ankitpokhrel/jira-cli`), configured with `~/.netrc` credentials for `jira.mongodb.org`.

## Overview

The `tests/specifications` git submodule tracks `mongodb/specifications`. A Dependabot PR that bumps it
(e.g. `Bump tests/specifications from `92b3c0b` to `1de749a` (#1977)`) usually bundles several upstream commits, each
referencing a `DRIVERS-XXXX` Jira ticket. Any spec change that requires driver-specific work is normally "split" into a
`PHPLIB-XXXX` (pure PHP library) or `PHPC-XXXX` (C driver / extension) ticket. This skill walks through tracing each
`DRIVERS-XXXX` commit back to its split ticket, checking whether the bump broke CI, skipping newly-broken tests with a
reference to the existing ticket, and approving/merging the PR.

## Step 1 — List the submodule commits

Run the helper script:

```bash
.agents/skills/review-specs-update/scripts/list-spec-commits.sh <PR_NUMBER>
```

It prints the old/new SHA of `tests/specifications` and the commits between them, highlighting any `DRIVERS-XXXX`
reference found in the commit messages. For each commit, it also lists the changed files with a category and action,
e.g. `[unified test added]`, `[prose test updated]`, `[spec updated]` (unified tests live under `tests/**/*.yml` or
`tests/**/*.json`, prose tests under `tests/*.md`, everything else under `*.md` is spec prose, anything else is
`other file`). It is read-only: it does not query Jira or write anything.

For every commit **without** a `DRIVERS-XXXX` reference (typo fixes, formatting, changelog-only edits): do nothing if it
doesn't touch test files. If it changes a spec's normative text or a test in a meaningful way with no ticket attached,
flag it to the operator instead of silently accepting it — don't guess whether it's safe.

## Step 2 — Find the split PHPLIB/PHPC ticket for each DRIVERS-XXXX

Use the `jira` CLI directly:

```bash
# Search PHPLIB/PHPC for a ticket that references the DRIVERS ticket
jira issue list --jql "project in (PHPLIB, PHPC) AND text ~ \"DRIVERS-XXXX\"" --plain

# Inspect the DRIVERS ticket itself for an existing "split to" link
jira issue view DRIVERS-XXXX
```

If no split ticket exists yet, tell the operator — do not create one automatically without explicit confirmation.

## Step 3 — Check CI

```bash
gh pr checks <PR_NUMBER> --repo mongodb/mongo-php-library
```

- CI green: go to Step 5.
- CI red: find the failing tests, e.g. `gh run view --log-failed --repo mongodb/mongo-php-library <RUN_ID>`.

Before assuming a failure was caused by this bump, check whether the failing test belongs to a spec touched by the
commits from Step 1 (the unified/prose test files listed there). A failure in an unrelated test class (e.g. a
change-stream or connection test with no link to the changed specs) is more likely a pre-existing flake — flag it to
the operator and suggest re-running the job, rather than skipping it under Step 4a.

## Step 4a — CI red: skip the newly-broken tests

For each test broken by the bump **and traced back to one of the changed specs**, skip it following the repo's
existing convention — a short reason followed by the ticket key in parentheses:

```php
$this->markTestSkipped('Bundled libmongocrypt does not support Decimal128 (PHPC-2207)');
```

Reference the existing PHPLIB/PHPC ticket found in Step 2; don't open a new one if one already exists. Before
committing:

```bash
composer fix:cs && composer check:cs && composer check:psalm
```

Commit the skip and push it to the PR branch after confirming with the operator.

## Step 4b — Ticket-linked changes with no CI failure

Add a comment on the corresponding PHPLIB/PHPC ticket:

```bash
jira issue comment add PHPLIB-XXXX -b "Spec tests updated in mongodb/mongo-php-library#<PR_NUMBER>"
```

## Step 5 — Approve the PR

```bash
gh pr review <PR_NUMBER> --repo mongodb/mongo-php-library --approve --body "..."
```

The approval comment should list every PHPLIB/PHPC ticket involved, one per line, linking to
`https://jira.mongodb.org/browse/PHPLIB-XXXX`. Write it in plain language English, no em dashes.

## Step 6 — Squash-merge

Ask the operator for explicit confirmation before merging.

```bash
gh pr merge <PR_NUMBER> --repo mongodb/mongo-php-library --squash
```

Keep the Dependabot-generated title as the squash commit message
(`Bump tests/specifications from \`<old>\` to \`<new>\` (#<PR_NUMBER>)`) — don't reword it.

## Common Mistakes

| Mistake                                                     | Fix                                                          |
| ------------------------------------------------------------ | -------------------------------------------------------------- |
| Missing a `DRIVERS-XXXX` commit with no attached ticket       | Flag it, don't silently skip                                   |
| Opening a duplicate PHPLIB/PHPC ticket when a split one exists | Search Jira first (Step 2) before creating anything             |
| Merging without operator confirmation                         | Always confirm before `gh pr merge`                              |
| Committing a skip without running CS/Psalm checks              | Run `composer fix:cs && composer check:cs && composer check:psalm` first |
| Rewording the squash-merge commit title                        | Keep the Dependabot-generated title as-is                        |
