# add-operator

Add new MQL operator(s) or stage(s) to both `mql-specifications` and the PHP library builders.
This skill covers any definition change — triggered by a Jira ticket, a spec update, or manual need.

## Usage

`/add-operator [description of what to add]`

## Workflow

### 1. Research the operator spec

**From the MongoDB server source** (most reliable):

```bash
# IDL definition for a stage
gh api "repos/mongodb/mongo/contents/src/mongo/db/pipeline/document_source_<name>.idl" \
  --jq '.content' | base64 -d

# For expressions: search expression.cpp and expression.h
gh api "search/code?q=<name>+repo:mongodb/mongo+path:src/mongo/db/pipeline/expression" \
  --jq '.items[].path'

# Feature flag version
gh api "repos/mongodb/mongo/contents/src/mongo/db/query/query_feature_flags.idl" \
  --jq '.content' | base64 -d | grep -A 8 "featureFlag<Name>"
```

Use existing similar definitions as models:
- Stage with object args → `generator/mql-specifications/definitions/stage/scoreFusion.yaml`
- Single-arg expression → `generator/mql-specifications/definitions/expression/sin.yaml`
- Single-arg expression → `generator/mql-specifications/definitions/expression/exp.yaml`

### 2. Create the mql-specifications branch

```bash
cd generator/mql-specifications
git checkout main
git pull upstream main
git checkout -b <short-branch-name>
```

### 3. Create the YAML definition file(s)

Create `generator/mql-specifications/definitions/<category>/<name>.yaml`.

**Template:**
```yaml
# $schema: ../../schemas/operator.json
name: $operatorName
link: https://www.mongodb.com/docs/manual/reference/operator/aggregation/<operatorName>/
minVersion: '8.1'
type:
  - stage            # see type reference below
encode: object       # object | single | array
description: |
  One-sentence description.
arguments:
  - name: argName
    type:
      - object
    description: |
      Description.
  - name: optionalArg
    optional: true
    type:
      - bool
    default: false
    description: Description.
tests:
  - name: Example
    link: https://www.mongodb.com/docs/manual/reference/operator/aggregation/<name>/#examples
    pipeline:
      - $<name>:
          # IMPORTANT: copy the example pipeline verbatim from the official MongoDB docs.
          # Do not invent or adapt — use the exact operators, field names, and values shown.
```

**Type reference — operator type (what the operator is):**
- `stage` — regular aggregation stage
- `inputStage` — must be first stage in pipeline (`$search`, `$vectorSearch`, `$changeStream`)
- `outputStage` — must be last stage (`$out`, `$merge`)
- `updateStage` — usable in update pipelines
- `resolvesToDouble`, `resolvesToDecimal`, `resolvesToNumber`, `resolvesToBool`,
  `resolvesToString`, `resolvesToDate`, `resolvesToInt`, `resolvesToLong`, `resolvesToAny`

**Type reference — argument types:**
- `object`, `bool`, `int`, `string`, `array`
- `query`, `pipeline`, `expression`
- `resolvesToNumber`, `resolvesToString`, `resolvesToBool`, `resolvesToAny`

**`encode` values:**
- `object` — operator has named arguments (`{ $op: { arg1: ..., arg2: ... } }`)
- `single` — operator takes one positional argument (`{ $op: <expr> }`)
- `array` — operator takes an array of arguments

### 4. Format and validate YAML

```bash
# Run from generator/mql-specifications/
yamlfix definitions/<category>/<name>.yaml
yamlfix --check definitions/<category>/<name>.yaml   # must output 0 left unchanged

cd scripts/schema-validator
pnpm install
pnpm run validate   # must validate all files with no errors
```

### 5. Commit and push to fork, create mql-specifications PR

```bash
cd generator/mql-specifications
git add definitions/<category>/<name>.yaml
git commit -m "Add $<name> <category> definition"
git push origin <branch-name>

gh pr create \
  --repo mongodb/mql-specifications \
  --head GromNaN:<branch-name> \
  --base main \
  --title 'Add $<name> <category>' \
  --body "..."
```

### 6. Run the PHP code generator

The generator reads from the submodule working directory, so the new YAML files are
available immediately (no need to wait for the mql-specifications PR to be merged).

```bash
cd /Users/jerome/Develop/mongo-php-library/generator
./generate
```

**Generated files (do NOT edit manually — they are overwritten on each run):**
- `src/Builder/<Category>/<Name>Stage.php` (or `Operator.php`) — value object class
- `src/Builder/<Category>/FactoryTrait.php` — factory method added
- `src/Builder/<Category>/FluentFactoryTrait.php` — fluent method added
- `tests/Builder/<Category>/Pipelines.php` — expected JSON fixtures (auto-generated)
- `tests/Builder/<Category>/<Name>Test.php` — test class (regenerated, no AUTO-GENERATED header)

### 7. Review and complete the generated tests

The generator converts the YAML `tests` section into PHP. Review the generated test file:

```bash
cat tests/Builder/<Category>/<Name>Test.php
```

Check that:
- Builder API calls are idiomatic (use `Stage::`, `Expression::`, `Query::` factory methods)
- Complex nested structures use the proper PHP types (`Pipeline`, typed builders) not raw arrays
- The `@todo` comments left by the generator are resolved

To improve a test, **edit the YAML `tests` section** and re-run `./generate` — the test PHP
is always regenerated from YAML. Only fix things that the generator cannot express.

Run the tests to confirm they pass:

```bash
cd /Users/jerome/Develop/mongo-php-library
composer test -- --filter <NameTest>
```

### 8. Fix code style and static analysis

```bash
composer fix:cs
composer check:cs
composer check:psalm
```

### 9. Create the mongo-php-library PR

Commit and push simultaneously with (or right after) the mql-specifications PR:

```bash
git add generator/mql-specifications src/Builder/ tests/Builder/
git commit -m "Add $<name> support"
gh pr create --repo mongodb/mongo-php-library --base v2.x \
  --title 'Add $<name> <category>' \
  --body "..."
```

PR body should reference:
- The mql-specifications PR (mongodb/mql-specifications#XX)
- The Jira ticket if applicable (PHPLIB-XXXX / DRIVERS-XXXX)

## Notes

- The submodule `generator/mql-specifications` has two remotes:
  - `origin` → `GromNaN/mongodb-mql-specifications` (your fork — push here)
  - `upstream` → `mongodb/mql-specifications` (source of truth — pull from here)
- After the mql-specifications PR is merged, update the submodule pointer:
  ```bash
  cd generator/mql-specifications && git pull upstream main
  cd .. && git add generator/mql-specifications
  git commit -m "Update mql-specifications submodule"
  ```
- The `tests/Builder/<Category>/<Name>Test.php` has no `AUTO-GENERATED` header but IS
  regenerated on every `./generate` run — always improve via the YAML, not the PHP directly.