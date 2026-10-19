# Decoupling the library from ext-mongodb

## Status

This is a plan, not a proposal for merge. The implementation described here exists
and runs, on the branch `feature/decoupling-stage-0` of
`GromNaN/mongo-php-library`. This document is the plan that goes with it, so that
the approach can be discussed before any code is reviewed.

What the prototype covers: the BSON types and functions, CRUD and commands,
cursors with `getMore` and `killCursors`, type maps, implicit sessions,
unacknowledged writes, and command monitoring.

What it does not cover: authentication, topology monitoring, server selection,
connection pooling, retries, transactions, snapshot sessions and client-side
encryption.

## Context

`mongodb/mongodb` today hard-requires the C extension: `composer.json` has
`"ext-mongodb": "^2.3"` in `require`, so `composer install` fails on any host that
cannot build the extension. That blocks the main use case: running the library in
CI on a plain PHP image, with no extra flag.

A working prototype, derived from `GromNaN/mongodb-php-async-driver`, shows that
the library runs unchanged on top of a pure PHP implementation of `MongoDB\BSON`
and `MongoDB\Driver`. This plan is Stage 0 of that work: make the no-extension
path official and installable.

Outcome: `composer require mongodb/mongodb` succeeds with no flag on a host
without the extension, the extension still wins when it is loaded, a CI job
proves it, a parity check keeps the userland API honest, and the supported subset
is documented.

## Decisions already made

- **Where the code lives:** bundled into the library's own autoload, under
  `src/BSON/`, `src/Driver/` and `src/Internal/`.
- **Scope:** deliverables 1 to 5 of Stage 0. Deliverable 6 (the same job in a
  downstream consumer) is out of scope.
- **CI platform:** GitHub Actions.

## Scope and shape

117 new files under `src/`, four config files touched (`composer.json`,
`psalm.xml.dist`, `phpcs.xml.dist`, `rector.php`), one new workflow, one new
submodule, two new test scripts, and two new documentation files. This is a large
change and should land as one reviewable branch, not mixed with anything else.

The library's single PSR-4 prefix `MongoDB\ => src/` already resolves every new
namespace, so no autoload prefix is added.

---

## Step 1: Composer contract

### 1.1 `composer.json`

Replace the `require` block, add `suggest` and `conflict`, and extend
`autoload.files`:

```json
    "require": {
        "composer-runtime-api": "^2.0",
        "ext-ctype": "*",
        "php": "^8.1",
        "psr/log": "^1.1.4|^2|^3",
        "symfony/polyfill-php85": "^1.33"
    },
    "suggest": {
        "ext-mongodb": ">=2.3 enables the native driver and BSON implementation. Without it, the bundled PHP implementation in src/BSON, src/Driver and src/Internal is used instead.",
        "ext-gmp": "Required to encode and decode MongoDB\\BSON\\Decimal128 with the bundled PHP implementation."
    },
    "conflict": {
        "ext-mongodb": "<2.3"
    },
    "autoload": {
        "psr-4": { "MongoDB\\": "src/" },
        "files": [
            "src/functions.php",
            "src/BSON/functions.php",
            "src/Driver/functions.php",
            "src/Driver/Monitoring/functions.php"
        ]
    },
```

Also add `"nikic/php-parser": "^5.3"` to `require-dev` (needed by the parity test
in Step 5; it is only transitive today).

Why this shape:

- `ext-mongodb` leaves `require`, so installation succeeds with no flag. That is
  the whole point of this work.
- No `provide`. `provide` would tell every downstream package that needs the real
  extension (CSFLE, TLS, auth, pooling) that it is available, and it would then
  fail at runtime.
- `conflict: "<2.3"` keeps the lower bound that `require: "^2.3"` enforces today.
  An old extension fails at install time exactly as before; absence succeeds.
- Coexistence works because PHP never autoloads a class that is already declared,
  so the extension wins when loaded.
- `composer-runtime-api` must stay: `src/Model/DriverOptions.php` uses
  `Composer\InstalledVersions` for the handshake client metadata.
- `ext-ctype` is used by the moved code and is enabled by default in PHP.
- `ext-gmp` is deliberately only suggested: it is absent from the default
  official PHP Docker images, so requiring it would break the exact case this
  work exists for. It is reached only when a `Decimal128` crosses the wire.

### 1.2 Remove the `ext-mbstring` dependency instead of declaring it

`mb_check_encoding($s, 'UTF-8')` is called in the string path of the BSON
decoder, in `Driver/BulkWrite.php` and in `Driver/BulkWriteCommand.php`. Replace
all three with the PCRE equivalent already used by the BSON indexer:

```php
preg_match('//u', $s) === 1
```

Three lines, no new platform requirement.

### 1.3 Lock file

`composer.lock` is gitignored and untracked, so there is nothing to commit, but
it must be regenerated locally or the install keeps failing:

```sh
composer update --lock
composer install
```

Then confirm `composer validate --strict` passes.

---

## Step 2: Move the userland driver into `src/`

The prototype lives outside the repository, so this is a copy plus `git add`, not
a `git mv`.

### 2.1 Mapping

The move is 1:1 by namespace, no renames:

| Prototype tree | Destination | Files |
| --- | --- | --- |
| `BSON/` | `src/BSON/` | 31 |
| `Driver/` | `src/Driver/` | 21 |
| `Driver/Exception/` | `src/Driver/Exception/` | 14 |
| `Driver/Monitoring/` | `src/Driver/Monitoring/` | 29 |
| `Internal/BSON/` and `Internal/BSON/Index/` | `src/Internal/BSON/` | 10 |
| `Internal/Connection/` | `src/Internal/Connection/` | 2 |
| `Internal/Monitoring/` | `src/Internal/Monitoring/` | 1 |
| `Internal/Operation/` | `src/Internal/Operation/` | 1 |
| `Internal/Protocol/` | `src/Internal/Protocol/` | 4 (see 2.2) |
| `Internal/Session/` | `src/Internal/Session/` | 1 |
| `Internal/Uri/` | `src/Internal/Uri/` | 3 |

Representative paths: `src/BSON/Document.php`, `src/BSON/Int64.php`,
`src/BSON/Type.php`, `src/Driver/Manager.php`, `src/Driver/Server.php`,
`src/Driver/Cursor.php`, `src/Driver/Exception/RuntimeException.php`,
`src/Driver/Monitoring/Subscriber.php`, `src/Internal/BSON/BsonDecoder.php`,
`src/Internal/Connection/Connection.php`, `src/Internal/Uri/ConnectionString.php`.

Nine of these files are modified derivatives of the upstream project and carry
the real synchronous logic. Review them line by line rather than as a bulk copy:
`Driver/Manager.php`, `Driver/Server.php`, `Driver/Session.php`,
`Driver/Cursor.php`, `Driver/BulkWrite.php`, `Internal/Connection/Connection.php`,
`Internal/Connection/ClientMetadata.php`, `Internal/Protocol/OpMsgEncoder.php`,
`Internal/Session/LogicalSessionId.php`.

Do not carry over the async-only upstream code, which the prototype already
dropped: `Internal/Auth/`, `Internal/Connection/ConnectionPool.php`,
`Internal/Operation/OperationExecutor.php`, `Internal/Operation/RetryableError.php`,
`Internal/Session/SessionPool.php`, `Internal/SyncRunner.php`, `Internal/Topology/`.

### 2.2 Drop two dead files

`Internal/Protocol/OpCompressedEncoder.php` and `OpCompressedDecoder.php` have
zero references anywhere and compression is not implemented. Do not carry them
over; restore them from upstream when compression lands. Everything else stays,
including the throwing stubs and the SDAM/CMAP event classes, because they are
part of the extension's declared API surface that the parity test asserts, and
because the library references them: for example `src/ClientBulkWrite.php` and
`src/Client.php` use `BulkWriteCommand` and `BulkWriteCommandResult`.

Mark the userland-only public methods that the extension does not have
(`BulkWriteCommand::getOps()`, `getOptions()`, `getNsInfo()`,
`getInsertedIds()`, `create()`, `createFromInternal()`, `setSession()`) with
`@internal` before the parity test freezes the surface.

### 2.3 Fix the PHP 8.1 blocker

The prototype uses typed class constants (`public const int DOUBLE = 1;`), which
is PHP 8.3+ syntax, in 10 files: `src/BSON/Type.php` (21),
`src/Internal/BSON/BsonType.php` (21), `src/Driver/ServerDescription.php` (10),
`src/Driver/ClientEncryption.php` (7), `src/Driver/ReadPreference.php` (7),
`src/Driver/Monitoring/LogSubscriber.php` (6), `src/Driver/ReadConcern.php` (5),
`src/Driver/ServerApi.php` (1), `src/Driver/WriteConcern.php` (1),
`src/Internal/BSON/BsonDecoder.php` (1).

The library targets `php: ^8.1` and `phpcs.xml.dist` sets `php_version 80100`. On
PHP 8.1 and 8.2 every one of these files is a parse error the moment it is
autoloaded, and the eager `files` includes guarantee they load. Strip the type
token from each declaration. This must be done before the move lands. Raising the
library floor to 8.3 is a breaking change and is out of scope.

No other PHP 8.2+ syntax is present: no readonly classes, no `#[\Override]`, no
property hooks, no asymmetric visibility, no DNF types.

### 2.4 Guard the namespaced function files

A `namespace` declaration cannot live inside an `if` block, so the guard wraps
only the function declarations. `src/Driver/Monitoring/functions.php` already has
the right shape; mirror it.

`src/BSON/functions.php` (the extension 2.x no longer declares these, so the
guard is belt and braces, but it protects against extension 1.x and any future
partial provider):

```php
<?php

declare(strict_types=1);

namespace MongoDB\BSON;

use function function_exists;

if (! function_exists(__NAMESPACE__ . '\fromJSON')) {
    // function fromJSON(string $json): string { ... }
    // function fromPHP(array|object $value): string { ... }
    // function toCanonicalExtendedJSON(string $bson): string { ... }
    // function toJSON(string $bson): string { ... }
    // function toPHP(string $bson, ?array $typeMap = null): array|object { ... }
    // function toRelaxedExtendedJSON(string $bson): string { ... }
}
```

Keep the existing deprecation docblocks on each function, and keep the
`use function function_exists;` import: `phpcs.xml.dist` forbids unqualified
fallback global functions. Apply the same shape to `src/Driver/functions.php`
around `is_document()`. Do not change `src/Driver/Monitoring/functions.php`.

### 2.5 Licensing

The library is Apache-2.0 and the upstream project is Apache-2.0 with no
`LICENSE` file. Every new file carries the library's 16 line Apache header, the
same one as `src/functions.php`.

No separate `NOTICE` file and no per-file provenance block: the header already
names the licence, and the mapping in 2.1 records where the code comes from.

Also fix the stale docblock in `src/Internal/Monitoring/Dispatcher.php`, which
references `GlobalDispatcher`, `OperationExecutor` and `TopologyManager`, none of
which exist here.

### 2.6 Leave behind

Do not commit `bootstrap.php`, `tests-bootstrap.php`, `phpunit.xml`,
`phpunit-full.log` or `.phpunit.result.cache` from the prototype: the first three
are superseded by the real Composer autoloader, the last two are artifacts.

---

## Step 3: Align the tooling

### 3.1 phpcs and phpcbf

`phpcs.xml.dist` covers `src`, so the moved code must pass the Doctrine standard.
A dry run over the prototype reports 148 errors in 33 files, of which `phpcbf`
fixes 40. The rest are manual:

- 73 `Squiz.Commenting.FunctionComment.WrongStyle`: change `/* ... */` to
  `/** ... */` on function comments.
- 21 `Generic.NamingConventions.UpperCaseConstantName` in
  `src/Internal/BSON/BsonType.php`: rename PascalCase constants to UPPER_CASE
  (`Double` to `DOUBLE`, `CodeWithScope` to `CODE_WITH_SCOPE`) and update their
  97 usages.
- 14 `Squiz.NamingConventions.ValidVariableName.MemberNotCamelCaps`.

Two automatic fixes are semantic and need a look before accepting:
`ReferenceThrowableOnly` rewrites `catch (Exception $e)` to `catch (Throwable $e)`
in 7 places (this matches the library's house style, which never catches
`Exception`), and `RequireConstructorPropertyPromotion` promotes 4 constructor
parameters to properties.

`ReferenceThrowableOnly` also rewrites type declarations, which is how the two
parity mismatches listed in the risks section appeared. Check its output on
declarations, not only on catch blocks.

Run `phpcbf` before committing and verify with `phpcs`, per the project rules.

### 3.2 Psalm

Four changes:

1. Delete the six `stubs/Driver/*.stub.php` files and the whole `<stubs>` block in
   `psalm.xml.dist`. They declare the same classes as the files being moved in
   (`BulkWriteCommand`, `BulkWriteCommandException`, `BulkWriteCommandResult`,
   `Cursor`, `CursorInterface`, `WriteResult`), so keeping them is a guaranteed
   duplicate declaration. `stubs` is `export-ignore`, so nothing user-facing is
   lost.
2. Port the generics from the deleted stubs into `src/Driver/CursorInterface.php`
   and `src/Driver/Cursor.php` (`@template TValue of array|object`,
   `@template-implements Iterator<int, TValue>`, `@psalm-ignore-nullable-return`).
   Four library files already annotate `CursorInterface<TValue>` and will fail at
   `errorLevel="1"` without them: `src/Model/CodecCursor.php`,
   `src/Model/ChangeStreamIterator.php`, `src/Command/ListCollections.php`,
   `src/Operation/ListIndexes.php`.
3. Add `<disableExtensions><extension name="mongodb"/></disableExtensions>`.
   Removing `ext-mongodb` from `require` already disables Psalm's own extension
   stub, but making it explicit keeps the behaviour if the entry is ever re-added.
4. Expect false positives from Psalm's bundled call map. `disableExtensions` only
   disables the extension stub files; the call map in
   `vendor/vimeo/psalm/dictionaries/CallMap_*.php` is generated from the extension
   arginfo and still applies. Psalm therefore checks calls to the classes declared
   in `src/` against the extension signatures and reports "Too many arguments",
   for instance on `new MongoDB\Driver\Server(...)`. Those entries belong in the
   baseline, with a comment in `psalm.xml.dist` to explain why.

Then regenerate the baseline last, after `phpcbf` and `rector` have settled:
`composer fix:psalm:baseline`. `psalm.xml.dist` sets
`findUnusedBaselineEntry="true"`, so stale entries become errors.

### 3.3 Rector

`composer check:rector` will fail on the first run. `StrContainsRector` rewrites
`strpos(...) !== false` to `str_contains(...)` in
`src/Internal/Uri/ConnectionString.php`. Run `composer fix:rector`, then `phpcbf`
again, then `composer checks`.

Order: move, headers, guards, 8.1 fix, then `composer validate --strict`,
`composer update --lock && composer install`, `phpcbf`, manual phpcs fixes,
`rector process`, `phpcbf`, baseline, `composer checks`.

---

## Step 4: CI job without the extension

Add `.github/workflows/no-extension.yml`, not a job inside `tests.yml`. Every
existing workflow goes through `.github/actions/setup/action.yml`, which hard-wires
`extensions: "mongodb-<driver-version>"` and runs `php --ri mongodb`; there is no
input that turns those off, so the new job installs PHP itself.

```yaml
name: "No extension"

on:
  merge_group:
  pull_request:
    branches:
      - "v*.*"
      - "feature/*"
  push:
    branches:
      - "v*.*"
      - "feature/*"

jobs:
  no-extension:
    name: "No ext-mongodb"
    runs-on: "ubuntu-24.04"

    strategy:
      fail-fast: true
      matrix:
        php-version:
          - "8.1"
          - "8.4"

    steps:
      - name: "Checkout"
        uses: "actions/checkout@v7"
        with:
          fetch-depth: 2
          submodules: true

      # Deliberately not ./.github/actions/setup: that action hard-wires
      # extensions: "mongodb-<driver-version>" and runs `php --ri mongodb`.
      - name: "Install PHP without ext-mongodb"
        uses: "shivammathur/setup-php@v2"
        with:
          php-version: "${{ matrix.php-version }}"
          coverage: "none"
          tools: "composer"
          ini-values: "zend.assertions=1"

      - name: "Assert ext-mongodb is absent"
        run: |
          php -r 'if (extension_loaded("mongodb")) { fwrite(STDERR, "ext-mongodb must not be loaded\n"); exit(1); }'

      - name: "Install dependencies"
        run: "composer install"

      - name: "Assert the userland driver is used"
        run: |
          php -r '
            require "vendor/autoload.php";
            $r = new ReflectionClass(MongoDB\Driver\Manager::class);
            printf("Manager from %s\n", $r->getFileName());
            if ($r->isInternal()) { fwrite(STDERR, "Manager is the extension\n"); exit(1); }
          '

      - name: "Run unit test suite"
        run: "vendor/bin/phpunit --configuration phpunit.no-extension.xml --testsuite unit"

      - name: "Set up MongoDB"
        id: setup-mongodb
        uses: ./tests/drivers-evergreen-tools
        with:
          version: "8.0"
          topology: "replica_set"

      - name: "Smoke test"
        env:
          MONGODB_URI: ${{ steps.setup-mongodb.outputs.cluster-uri }}
        run: |
          php tests/Smoke/crud.php
          php tests/Smoke/edge-cases.php

      - name: "Parity: stub compatibility"
        run: "vendor/bin/phpunit --configuration phpunit.no-extension.xml --testsuite parity"
```

Notes:

- `topology: replica_set` matches the Stage 0 exit criteria and exercises replica
  set URI parsing, which is the Atlas Local configuration.
- The 8.1 leg is the declared floor and the leg that proves the typed-constant fix
  in Step 2.3. The 8.4 leg matches the validated prototype.
- `composer install` has no flags, which is the literal claim of this work.
- `tests/drivers-evergreen-tools` installs its own Python, so no separate
  `setup-python` step is needed.

### 4.1 `phpunit.no-extension.xml`

The library has no unit and functional split: `phpunit.xml.dist` scans all of
`./tests/`. Add a config with a `unit` testsuite holding only the serverless
directories, and a `parity` testsuite for Step 5:

```xml
    <testsuites>
        <testsuite name="unit">
            <directory>tests/Builder</directory>
            <directory>tests/Command</directory>
            <directory>tests/Comparator</directory>
            <directory>tests/Model</directory>
            <exclude>tests/Model/ChangeStreamIteratorTest.php</exclude>
            <exclude>tests/Model/IndexInfoFunctionalTest.php</exclude>
            <exclude>tests/Model/CodecCursorFunctionalTest.php</exclude>
            <exclude>tests/Model/CachingIteratorFunctionalTest.php</exclude>
            <file>tests/SpecTests/DocumentsMatchConstraintTest.php</file>
            <file>tests/UnifiedSpecTests/Constraint/IsBsonTypeTest.php</file>
            <file>tests/PedantryTest.php</file>
            <file>tests/PsrLogAdapterTest.php</file>
        </testsuite>
        <testsuite name="parity">
            <file>tests/Parity/StubCompatibilityTest.php</file>
        </testsuite>
    </testsuites>
```

The config sets `defaultTestSuite="unit"`, so a filtered run must name the
testsuite explicitly (`--testsuite parity`). A `--filter` alone selects nothing
and the step passes without running a test.

Pin the membership by running the suite with no server and pruning, and check
`tests/ClientTest.php` (3 client-side-encryption errors today) and
`tests/FunctionsTest.php`, `tests/LogNonGenuineHostTest.php` (may open a
connection) before adding them.

### 4.2 Smoke test

Copy the prototype's `crud.php` (29 checks) and `edge-cases.php` (28 checks) to
`tests/Smoke/`, and change their bootstrap require to
`require dirname(__DIR__, 2) . '/vendor/autoload.php';`. Keep their
`extension loaded: no` and driver-class header lines: they are the smoke test's
own assertion that the extension is absent.

### 4.3 Interim state

The workflow is blocked on Step 1 for the plain `composer install` line. If the
contract and the move are reviewed separately, land the workflow with
`composer install --ignore-platform-req=ext-mongodb` and a comment, keep it out of
`merge_group` until green, then flip the line in the same PR as the
`composer.json` change.

---

## Step 5: Parity check

### 5.1 Stub compatibility (blocking)

Port the async driver's `StubCompatibilityTest.php` (468 lines) to
`tests/Parity/StubCompatibilityTest.php`, namespace `MongoDB\Tests\Parity`. It
parses the extension's `.stub.php` files with `nikic/php-parser`, reflects on the
loaded userland classes, and asserts finality, constants, properties, method
visibility, final, static, return types and parameter names, types, variadic,
by-ref and optional. `phpunit.xml.dist` scans `./tests/` recursively, so it is
discovered with no config change.

Adaptations:

- Fix the stub path for the new location, and fail loudly if the submodule is
  absent rather than silently producing zero cases.
- Convert `@dataProvider` to `#[DataProvider(...)]`, matching
  `tests/Comparator/Int64ComparatorTest.php`.
- Skip when the extension is loaded:
  `if (extension_loaded('mongodb')) { $this->markTestSkipped(...); }`.
- Reformat with `phpcbf` for the Doctrine standard.

The library gets the extension stubs from a new shallow submodule. This is the
same checkout that provides `run-tests.php` for 5.2, and the existing
`submodules.yml` job and Dependabot already handle submodules.

```
[submodule "tests/references/mongo-php-driver"]
	path = tests/references/mongo-php-driver
	url = https://github.com/mongodb/mongo-php-driver
	branch = master
	shallow = true
```

Pin the gitlink to the tag matching the provided version. Because
`composer.json` has `"pre-install-cmd": "git submodule update --init"`, the
submodule is fetched on local installs too.

The new submodule forces tooling exclusions, since both linters scan all of
`tests/`:

- `phpcs.xml.dist`: add `<exclude-pattern>tests/references/*</exclude-pattern>`.
- `rector.php`: add `__DIR__ . '/tests/references'` to the skip paths.
- `phpunit.xml.dist`: add `<exclude>./tests/references</exclude>` to the default
  testsuite as a safety measure.
- `psalm.xml.dist` scans only `src`, `examples` and `tests/Type`, so no change.

### 5.2 Extension PHPT suite (advisory in this iteration)

Port the async driver's harness: `tests/run-phpt.sh`, `tests/Phpt/prepend.php`,
`tests/Phpt/php-ini/` and `tests/Phpt/skip_list.php`. `run-phpt.sh` runs the
extension's own PHPT suite against the userland implementation, with
`PHP_INI_SCAN_DIR` pointing at a committed ini that disables `ext-mongodb`.

The 28 KB skip list from the async driver is a starting point, not a solution. It
has two kinds of entries: permanently impossible in userland (BSON comparison and
arithmetic operators, `Decimal128` normalization, `mongodb.debug`, client-side
encryption) which carry over verbatim, and entries that reflect what the async
driver had not implemented, which must be re-derived. The Stage 0 prototype is far
less capable than the async driver (no auth, no SDAM, no pooling, no retries, no
transactions), so it will fail many more PHPTs.

Ship 5.2 as a non-blocking advisory job (`continue-on-error: true`) scoped to a
curated subset that the prototype actually passes, starting with `bson/`,
`manager/`, `server/`, `cursor/`. Promote it to blocking once the rebuilt skip
list is stable. Add two guards the upstream script lacks: fail if a skip-list key
no longer resolves, so the list cannot rot when the submodule is bumped, and
print the skip count by category.

---

## Step 6: Documentation

Add a user-facing document (plain language, no em dashes):

1. Summary: the library runs without `ext-mongodb`, the extension wins when
   loaded, no-extension mode is a supported fallback with a documented subset.
2. How the driver is chosen: autoloading, a class already declared is never
   autoloaded, nothing to configure.
3. Installation: `composer require mongodb/mongodb` works with no extension; how
   to install the extension for the production path; the `conflict` lower bound.
4. Supported subset: BSON types and functions, CRUD and commands, cursors with
   `getMore` and `killCursors`, type maps, implicit sessions, unacknowledged
   writes, command monitoring, the non-genuine host warning.
5. Known gaps, each with the consequence, not just the name: no authentication, no
   SDAM topology monitoring or server selection, no connection pooling, no
   retryable reads or writes, no transactions, no snapshot sessions, no
   client-side encryption (`autoEncryption` must be rejected, never ignored), no
   client bulk write, no wire compression or CSOT, no exhaust cursors, the
   handshake is not reported to command monitoring, and `Decimal128` needs
   `ext-gmp`.
6. Behaviour differences: the `Int64` loose comparison.
   `new MongoDB\BSON\Int64(8589934592) == 8589934592` is `false` without the
   extension and emits a notice, because PHP has no userland hook for the
   extension's C comparison handler, while `== "8589934592"` still works. Point
   at `tests/Comparator/Int64Comparator.php`.
7. Parity testing: what the stub test checks, what the PHPT harness runs, how to
   run both locally, which parts are advisory.

Also rewrite the `README.md` Installation section, which currently states the
library requires the extension, and point at the new document. No new
`UPGRADE-*.md` file: the `Int64` difference only appears when the extension is
absent, so nothing changes for existing users.

---

## Verification

Run from the repository root.

```sh
# Composer contract
composer validate --strict
composer update --lock
composer install                      # must succeed with no ext-mongodb and no flag

# The extension still wins when loaded
php -r 'require "vendor/autoload.php"; var_dump((new ReflectionClass("MongoDB\\Driver\\Manager"))->getFileName());'
# expect: false, meaning the extension class

# The userland driver is used when the extension is absent
php -n -r 'require "vendor/autoload.php"; var_dump((new ReflectionClass("MongoDB\\Driver\\Manager"))->getFileName());'
# expect: .../src/Driver/Manager.php

# PHP 8.1 parse check over the new code
find src/BSON src/Driver src/Internal -name '*.php' -print0 | xargs -0 -n1 php8.1 -l

# Style and analysis
composer fix:cs && composer checks

# Test suites, extension absent
php -n vendor/bin/phpunit --configuration phpunit.no-extension.xml --testsuite unit
php -n vendor/bin/phpunit --configuration phpunit.no-extension.xml --testsuite parity

# Smoke test against a live server
MONGODB_URI=mongodb://127.0.0.1:27017 php -n tests/Smoke/crud.php
MONGODB_URI=mongodb://127.0.0.1:27017 php -n tests/Smoke/edge-cases.php

# No regression with the extension present
vendor/bin/phpunit --configuration phpunit.evergreen.xml
```

Exit criteria: the library's unit and functional tests pass against a standalone
server and a replica set, without the extension and without authentication.

---

## Review guide

Most of the diff is mechanical: 117 files copied into `src/`, renamed constants,
docblock style, and a regenerated Psalm baseline. Four places deserve a real
reading:

1. `src/Driver/Manager.php`, `src/Driver/Server.php`, `src/Driver/Cursor.php` and
   `src/Driver/Session.php`: the synchronous logic and the server selection that
   replaces the topology.
2. `src/Internal/Connection/Connection.php` and
   `src/Internal/Protocol/OpMsgEncoder.php`: the wire protocol on plain PHP
   streams.
3. `composer.json` and `psalm.xml.dist`: the contract and the analysis setup.
4. `tests/Parity/StubCompatibilityTest.php`: what the parity check actually
   asserts.

## Out of scope

- Stage 1 (port the native BSON implementation with the lazy field index and run
  the BSON corpus tests) and later stages.
- The `Int64` operator limitation, which cannot be fixed in userland.
- Extending the no-extension CI matrix beyond PHP 8.1 and 8.4.
- The same job in a downstream consumer (deliverable 6).
- Evergreen: its global `pre:` block runs `fetch extension` for every task, so no
  Evergreen change is proposed here.

## Risks

- **Bulk of the review is style.** 148 phpcs errors, 73 of them docblock style,
  plus 21 constant renames across 97 usages. Mechanical but large.
- **Partial shadowing when the extension is loaded.** ext-mongodb 2.5.0 declares
  only 16 of the 28 `MongoDB\Driver\Monitoring` types, so the CMAP event classes
  are autoloaded from `src/` even with the extension present. Expected to work,
  but it is the one place where "the extension always wins" is false. The parity
  test should assert `class_exists()` for the full monitoring list in both modes.
- **Two parity mismatches are known today.** `CommandFailedEvent::$error` and
  `ServerHeartbeatFailedEvent::$error` are `\Exception` in the extension and
  `\Throwable` in the prototype, after the automatic `ReferenceThrowableOnly`
  fix. Narrow them before the parity job becomes blocking.
- **A coexisting provider package** that ships the same namespaces would make
  `composer dump-autoload -o` report ambiguous class resolution. Consider a
  `conflict` entry against any such package once one exists.
- **`function_exists('MongoDB\BSON\fromPHP')`** flips from false to true on every
  install with the 2.x extension loaded. Additive, but it is a public predicate.
- **The PHPT harness is not shippable as a blocking gate yet.** Treat 5.2 as
  advisory until the skip list is rebuilt.
- **The 8.1 leg may surface issues beyond the typed constants.** None were found
  by inspection, but the prototype was only ever run on PHP 8.5.
