<?php
/*
 * Copyright 2015-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

declare(strict_types=1);

namespace MongoDB\Driver;

use MongoDB\BSON\Document;
use MongoDB\BSON\Int64;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\Exception\CommandException;
use MongoDB\Driver\Exception\ExecutionTimeoutException;
use MongoDB\Driver\Exception\InvalidArgumentException;
use MongoDB\Driver\Exception\RuntimeException;
use MongoDB\Internal\Connection\Connection;
use MongoDB\Internal\Monitoring\Dispatcher;
use MongoDB\Internal\Operation\CommandHelper;
use MongoDB\Internal\Protocol\RequestIdGenerator;
use MongoDB\Internal\Session\LogicalSessionId;
use stdClass;
use Throwable;

use function array_map;
use function array_values;
use function count;
use function explode;
use function get_object_vars;
use function hrtime;
use function implode;
use function in_array;
use function intdiv;
use function is_array;
use function is_object;
use function sprintf;
use function time;

/**
 * A single MongoDB server the driver can send commands to.
 *
 * This prototype implements the subset of the Server API that the high level
 * library uses for CRUD operations. Topology monitoring is not implemented:
 * the server object wraps one connection to one address, whose type is derived
 * from the hello response captured at connection time.
 *
 * @see https://php.net/manual/en/class.mongodb-driver-server.php
 */
final class Server
{
    public const TYPE_UNKNOWN = 0;
    public const TYPE_STANDALONE = 1;
    public const TYPE_MONGOS = 2;
    public const TYPE_POSSIBLE_PRIMARY = 3;
    public const TYPE_RS_PRIMARY = 4;
    public const TYPE_RS_SECONDARY = 5;
    public const TYPE_RS_ARBITER = 6;
    public const TYPE_RS_OTHER = 7;
    public const TYPE_RS_GHOST = 8;
    public const TYPE_LOAD_BALANCER = 9;

    private ?int $type = null;

    /**
     * Logical session ID attached to commands that do not carry one.
     *
     * The prototype keeps a single implicit session instead of pooling them,
     * since it never retries an operation.
     */
    private ?LogicalSessionId $implicitSessionId = null;

    public function __debugInfo(): array
    {
        return [
            'host' => $this->getHost(),
            'port' => $this->getPort(),
            'type' => $this->getType(),
            'is_primary' => $this->isPrimary(),
            'is_secondary' => $this->isSecondary(),
            'last_hello_response' => $this->getInfo(),
            'round_trip_time' => $this->connection->getRoundTripTime(),
        ];
    }

    private function __construct(
        private readonly Connection $connection,
        private readonly ReadConcern $defaultReadConcern,
        private readonly ReadPreference $defaultReadPreference,
        private readonly WriteConcern $defaultWriteConcern,
        private readonly Dispatcher $dispatcher,
        private readonly ?ServerApi $serverApi = null,
    ) {
    }

    /** @param array{writeConcern?: WriteConcern, session?: Session}|null $options */
    public function executeBulkWrite(string $namespace, BulkWrite $bulkWrite, ?array $options = null): WriteResult
    {
        if ($bulkWrite->count() === 0) {
            throw new InvalidArgumentException('Cannot do an empty bulk write');
        }

        if ($bulkWrite->isExecuted()) {
            throw new InvalidArgumentException(
                'BulkWrite objects may only be executed once and this instance has already been executed',
            );
        }

        [$db, $collection] = self::splitNamespace($namespace);

        $writeConcern = $options['writeConcern'] ?? $this->defaultWriteConcern;
        $session = $options['session'] ?? null;

        $bulkOptions = $bulkWrite->getOptions();
        $ordered = (bool) ($bulkOptions['ordered'] ?? true);

        $acknowledged = $writeConcern->getW() !== 0;

        $totalInserted = 0;
        $totalMatched = 0;
        $totalModified = 0;
        $totalDeleted = 0;
        $totalUpserted = 0;
        $upsertedIds = [];
        $writeErrors = [];
        $errorReplies = [];
        $writeConcernError = null;
        $batchException = null;

        foreach (self::batchOperations($bulkWrite->getOperations()) as $batch) {
            try {
                $result = $this->executeBatch($db, $collection, $batch, $bulkOptions, $ordered, $writeConcern, $session);
            } catch (CommandException $e) {
                $errorReplies[] = $e->getResultDocument();

                if ($ordered) {
                    break;
                }

                continue;
            } catch (Throwable $e) {
                if ($ordered) {
                    $batchException = $e;
                    break;
                }

                continue;
            }

            $batchType = $batch['type'];
            $globalIndices = $batch['globalIndices'];

            if ($batchType === 'insert') {
                $totalInserted += (int) ($result['n'] ?? count($batch['ops']));
            } elseif ($batchType === 'update') {
                $upsertedInBatch = (array) ($result['upserted'] ?? []);
                $totalMatched += (int) ($result['n'] ?? 0) - count($upsertedInBatch);
                $totalModified += (int) ($result['nModified'] ?? 0);

                foreach ($upsertedInBatch as $upserted) {
                    $upserted = (array) $upserted;
                    $localIndex = (int) $upserted['index'];
                    $globalIndex = $globalIndices[$localIndex] ?? $localIndex;
                    $upsertedIds[$globalIndex] = $upserted['_id'];
                    ++$totalUpserted;
                }
            } elseif ($batchType === 'delete') {
                $totalDeleted += (int) ($result['n'] ?? 0);
            }

            // Per-batch write errors carry a batch-local index that must be
            // remapped to the position of the operation in the original bulk.
            foreach ((array) ($result['writeErrors'] ?? []) as $error) {
                $localIndex = (int) ($error->index ?? 0);
                $writeErrors[] = WriteError::create(
                    code: (int) ($error->code ?? 0),
                    index: $globalIndices[$localIndex] ?? $localIndex,
                    message: (string) ($error->errmsg ?? ''),
                    info: $error->errInfo ?? null,
                );
            }

            if ($writeConcernError === null && isset($result['writeConcernError'])) {
                $concernError = (array) $result['writeConcernError'];
                $writeConcernError = WriteConcernError::create(
                    code: (int) ($concernError['code'] ?? 0),
                    message: (string) ($concernError['errmsg'] ?? ''),
                );
            }
        }

        $bulkWrite->markExecuted($db, $collection, 0, $writeConcern);

        $writeResult = WriteResult::createFromInternal(
            insertedCount: $acknowledged ? $totalInserted : null,
            matchedCount: $acknowledged ? $totalMatched : null,
            modifiedCount: $acknowledged ? $totalModified : null,
            deletedCount: $acknowledged ? $totalDeleted : null,
            upsertedCount: $acknowledged ? $totalUpserted : null,
            upsertedIds: $upsertedIds,
            server: $this,
            writeConcernError: $writeConcernError,
            writeErrors: $writeErrors,
            acknowledged: $acknowledged,
            writeConcern: $writeConcern,
            errorReplies: $errorReplies,
        );

        if ($batchException !== null) {
            throw BulkWriteException::create(
                sprintf('Bulk write failed due to previous %s: %s', $batchException::class, $batchException->getMessage()),
                0,
                $writeResult,
                null,
                $batchException,
            );
        }

        if ($errorReplies !== []) {
            $reply = (array) $errorReplies[0];

            throw BulkWriteException::create(
                (string) ($reply['errmsg'] ?? ''),
                (int) ($reply['code'] ?? 0),
                $writeResult,
                (object) $reply,
            );
        }

        if ($writeErrors !== []) {
            $message = count($writeErrors) === 1
                ? $writeErrors[0]->getMessage()
                : sprintf(
                    'Multiple write errors: %s',
                    implode(', ', array_map(static fn (WriteError $e) => sprintf('"%s"', $e->getMessage()), $writeErrors)),
                );

            throw BulkWriteException::create($message, $writeErrors[0]->getCode(), $writeResult);
        }

        return $writeResult;
    }

    /**
     * The `bulkWrite` command (MongoDB 8.0+) is not implemented in this prototype.
     *
     * The result object has no public constructor and the driver cannot build one
     * from a userland reply document, which is one of the extension changes listed
     * in the design document.
     */
    public function executeBulkWriteCommand(BulkWriteCommand $bulkWriteCommand, ?array $options = null): BulkWriteCommandResult
    {
        throw new RuntimeException('BulkWriteCommand is not supported by the decoupling prototype');
    }

    /** @param array{readPreference?: ReadPreference, readConcern?: ReadConcern, writeConcern?: WriteConcern, session?: Session}|null $options */
    public function executeCommand(string $db, Command $command, ?array $options = null): CursorInterface
    {
        return $this->runCommand(
            $db,
            $command,
            $options['readPreference'] ?? $this->defaultReadPreference,
            $options['readConcern'] ?? $this->defaultReadConcern,
            $options['writeConcern'] ?? $this->defaultWriteConcern,
            $options['session'] ?? null,
        );
    }

    /** @param array{readPreference?: ReadPreference, session?: Session}|null $options */
    public function executeQuery(string $namespace, Query $query, ?array $options = null): CursorInterface
    {
        [$db, $collection] = self::splitNamespace($namespace);

        $readPreference = $options['readPreference'] ?? $this->defaultReadPreference;

        $filter = $query->getFilter();
        $command = ['find' => $collection, 'filter' => is_array($filter) ? (object) $filter : $filter];

        // Option keys accepted by the find command. Document-valued options must
        // stay documents even when empty, otherwise an empty sort or projection
        // would be encoded as a BSON array and rejected by the server.
        $documentOptions = ['sort', 'projection', 'hint', 'min', 'max', 'let', 'collation'];

        foreach (
            [
                'sort', 'projection', 'skip', 'limit', 'batchSize', 'singleBatch',
                'comment', 'maxTimeMS', 'hint', 'let', 'allowDiskUse',
                'allowPartialResults', 'noCursorTimeout', 'tailable', 'awaitData',
                'returnKey', 'showRecordId', 'min', 'max', 'rawData', 'collation',
            ] as $option
        ) {
            $value = $query->getOptions()[$option] ?? null;

            if ($value === null) {
                continue;
            }

            $command[$option] = in_array($option, $documentOptions, true) && is_array($value)
                ? (object) $value
                : $value;
        }

        // A batchSize equal to limit would trigger a superfluous getMore.
        if (isset($command['batchSize'], $command['limit']) && $command['batchSize'] === $command['limit']) {
            $command['batchSize'] = $command['limit'] + 1;
        }

        $maxAwaitTimeMS = (int) ($query->getOptions()['maxAwaitTimeMS'] ?? 0);

        return $this->runCommand(
            $db,
            new Command($command),
            $readPreference,
            null,
            null,
            $options['session'] ?? null,
            $maxAwaitTimeMS,
            $query,
        );
    }

    /** @param array{readPreference?: ReadPreference, readConcern?: ReadConcern, session?: Session}|null $options */
    public function executeReadCommand(string $db, Command $command, ?array $options = null): CursorInterface
    {
        return $this->runCommand(
            $db,
            $command,
            $options['readPreference'] ?? $this->defaultReadPreference,
            $options['readConcern'] ?? $this->defaultReadConcern,
            null,
            $options['session'] ?? null,
        );
    }

    /** @param array{readConcern?: ReadConcern, writeConcern?: WriteConcern, session?: Session}|null $options */
    public function executeReadWriteCommand(string $db, Command $command, ?array $options = null): CursorInterface
    {
        return $this->runCommand(
            $db,
            $command,
            $this->defaultReadPreference,
            $options['readConcern'] ?? $this->defaultReadConcern,
            $options['writeConcern'] ?? $this->defaultWriteConcern,
            $options['session'] ?? null,
        );
    }

    /** @param array{writeConcern?: WriteConcern, session?: Session}|null $options */
    public function executeWriteCommand(string $db, Command $command, ?array $options = null): CursorInterface
    {
        return $this->runCommand(
            $db,
            $command,
            $this->defaultReadPreference,
            null,
            $options['writeConcern'] ?? $this->defaultWriteConcern,
            $options['session'] ?? null,
        );
    }

    public function getHost(): string
    {
        return $this->connection->getHost();
    }

    /**
     * Return the server's hello response, as an array.
     *
     * @return array<string, mixed>
     */
    public function getInfo(): array
    {
        return $this->connection->getHelloResponse();
    }

    public function getLatency(): ?int
    {
        $roundTripTime = $this->connection->getRoundTripTime();

        return $roundTripTime !== null ? (int) $roundTripTime : null;
    }

    public function getPort(): int
    {
        return $this->connection->getPort();
    }

    public function getServerDescription(): ServerDescription
    {
        return ServerDescription::createFromInternal(
            $this->getHost(),
            $this->getPort(),
            $this->getType(),
            $this->connection->getRoundTripTime(),
            $this->getInfo(),
            time(),
        );
    }

    /** @return array<string, string> */
    public function getTags(): array
    {
        $tags = $this->getInfo()['tags'] ?? [];

        return is_array($tags) ? $tags : [];
    }

    public function getType(): int
    {
        return $this->type ??= $this->detectType();
    }

    public function isArbiter(): bool
    {
        return $this->getType() === self::TYPE_RS_ARBITER;
    }

    public function isHidden(): bool
    {
        return (bool) ($this->getInfo()['hidden'] ?? false);
    }

    public function isPassive(): bool
    {
        return (bool) ($this->getInfo()['passive'] ?? false);
    }

    public function isPrimary(): bool
    {
        return $this->getType() === self::TYPE_RS_PRIMARY || $this->getType() === self::TYPE_STANDALONE;
    }

    public function isSecondary(): bool
    {
        return $this->getType() === self::TYPE_RS_SECONDARY;
    }

    /** @internal Close the underlying connection. */
    public function close(): void
    {
        $this->connection->close();
    }

    /** @internal Use Manager::selectServer() to obtain a Server instance. */
    public static function create(
        Connection $connection,
        ReadConcern $defaultReadConcern,
        ReadPreference $defaultReadPreference,
        WriteConcern $defaultWriteConcern,
        Dispatcher $dispatcher,
        ?ServerApi $serverApi = null,
    ): self {
        return new self($connection, $defaultReadConcern, $defaultReadPreference, $defaultWriteConcern, $dispatcher, $serverApi);
    }

    /**
     * Kill a server-side cursor.
     *
     * Called when a Cursor object is destroyed before it is exhausted, which
     * mirrors the free-object handler of the extension. The command must run on
     * the same logical session that created the cursor.
     *
     * @internal
     */
    public function killCursor(string $namespace, int $cursorId, ?Session $session = null): void
    {
        [$db, $collection] = self::splitNamespace($namespace);

        $prepared = CommandHelper::prepareCommand(
            command: ['killCursors' => $collection, 'cursors' => [new Int64($cursorId)]],
            db: $db,
            session: $session,
            serverApi: $this->serverApi,
        );

        $this->send($db, 'killCursors', $prepared);
    }

    /**
     * Attach an implicit logical session ID to a command that does not have one.
     *
     * The driver sends an lsid with every command, so that the server can track
     * causally related operations. Unacknowledged writes are the exception: the
     * server rejects an lsid on a command whose write concern is w:0.
     *
     * @param array<string, mixed> $prepared
     *
     * @return array<string, mixed>
     */
    private function applyImplicitSessionId(array $prepared): array
    {
        if (isset($prepared['lsid']) || self::isUnacknowledged($prepared)) {
            return $prepared;
        }

        $prepared['lsid'] = $this->implicitSessionId ??= LogicalSessionId::generate();

        return $prepared;
    }

    /**
     * Group consecutive operations of the same type into batches.
     *
     * Consecutive grouping keeps the original operation order, which ordered
     * bulk writes require, while still reducing the number of round trips.
     *
     * @param list<array> $operations
     *
     * @return list<array{type: string, ops: list<array>, globalIndices: list<int>}>
     */
    private static function batchOperations(array $operations): array
    {
        $batches = [];
        $currentBatch = null;

        foreach ($operations as $globalIndex => $operation) {
            $type = $operation[0];

            if ($currentBatch === null || $currentBatch['type'] !== $type) {
                if ($currentBatch !== null) {
                    $batches[] = $currentBatch;
                }

                $currentBatch = ['type' => $type, 'ops' => [], 'globalIndices' => []];
            }

            $currentBatch['ops'][] = $operation;
            $currentBatch['globalIndices'][] = $globalIndex;
        }

        if ($currentBatch !== null) {
            $batches[] = $currentBatch;
        }

        return $batches;
    }

    /**
     * Build a cursor from a decoded command reply.
     *
     * Commands returning a `cursor` sub-document (find, aggregate, listCollections)
     * get a getMore callback so the caller can iterate past the first batch.
     * Every other command yields a one-element cursor over the reply document.
     *
     * @param array<string, mixed> $body
     */
    private function buildCursor(
        array $body,
        string $db,
        int $maxAwaitTimeMS,
        int $batchSize,
        mixed $comment,
        ?Session $session,
        ?Command $debugCommand,
        ?Query $debugQuery,
    ): CursorInterface {
        $rawCursor = $body['cursor'] ?? null;

        if ($rawCursor === null) {
            return Cursor::createFromArray([(object) $body], $this, $db, $debugCommand);
        }

        $rawCursor = (array) $rawCursor;
        $cursorId = self::toInt($rawCursor['id'] ?? 0);
        $namespace = (string) ($rawCursor['ns'] ?? $db);

        $getMoreFn = function (int $cursorId, string $namespace) use ($db, $maxAwaitTimeMS, $batchSize, $comment, $session): array {
            $getMore = [
                'getMore' => new Int64($cursorId),
                'collection' => explode('.', $namespace, 2)[1] ?? $namespace,
            ];

            if ($batchSize > 0) {
                $getMore['batchSize'] = $batchSize;
            }

            if ($comment !== null) {
                $getMore['comment'] = $comment;
            }

            if ($maxAwaitTimeMS > 0) {
                $getMore['maxTimeMS'] = $maxAwaitTimeMS;
            }

            $prepared = CommandHelper::prepareCommand(
                command: $getMore,
                db: $db,
                session: $session,
                serverApi: $this->serverApi,
            );

            $reply = $this->send($db, 'getMore', $prepared);
            $cursor = (array) ($reply['cursor'] ?? []);

            return [(array) ($cursor['nextBatch'] ?? []), self::toInt($cursor['id'] ?? 0)];
        };

        return Cursor::createFromCommandResult(
            items: (array) ($rawCursor['firstBatch'] ?? []),
            cursorId: $cursorId,
            namespace: $namespace,
            server: $this,
            typeMap: [],
            getMoreFn: $getMoreFn,
            database: $db,
            command: $debugCommand,
            query: $debugQuery,
            session: $session,
        );
    }

    /**
     * @param list<array>          $operations
     * @param array<string, mixed> $bulkOptions
     *
     * @return array<string, mixed>
     */
    private static function buildDeleteCommand(string $collection, array $operations, array $bulkOptions, bool $ordered): array
    {
        $deletes = array_map(static function (array $op): array {
            [, $filter, $options] = $op;

            $spec = [
                'q' => is_array($filter) ? (object) $filter : $filter,
                'limit' => $options['limit'] ?? 1 ? 1 : 0,
            ];

            foreach (['collation', 'hint'] as $key) {
                if (isset($options[$key])) {
                    $spec[$key] = $options[$key];
                }
            }

            return $spec;
        }, $operations);

        $command = ['delete' => $collection, 'deletes' => $deletes, 'ordered' => $ordered];

        if (isset($bulkOptions['comment'])) {
            $command['comment'] = $bulkOptions['comment'];
        }

        if (isset($bulkOptions['let'])) {
            $command['let'] = $bulkOptions['let'];
        }

        if (isset($bulkOptions['rawData'])) {
            $command['rawData'] = $bulkOptions['rawData'];
        }

        return $command;
    }

    /**
     * @param list<array>          $operations
     * @param array<string, mixed> $bulkOptions
     *
     * @return array<string, mixed>
     */
    private static function buildInsertCommand(string $collection, array $operations, array $bulkOptions, bool $ordered): array
    {
        $command = [
            'insert' => $collection,
            'documents' => array_map(static fn ($op) => $op[1], $operations),
            'ordered' => $ordered,
        ];

        if (isset($bulkOptions['comment'])) {
            $command['comment'] = $bulkOptions['comment'];
        }

        if ($bulkOptions['bypassDocumentValidation'] ?? false) {
            $command['bypassDocumentValidation'] = true;
        }

        if (isset($bulkOptions['rawData'])) {
            $command['rawData'] = $bulkOptions['rawData'];
        }

        return $command;
    }

    /**
     * @param list<array>          $operations
     * @param array<string, mixed> $bulkOptions
     *
     * @return array<string, mixed>
     */
    private static function buildUpdateCommand(string $collection, array $operations, array $bulkOptions, bool $ordered): array
    {
        $updates = array_map(static function (array $op): array {
            [, $filter, $newObject, $options] = $op;

            $spec = [
                'q' => is_array($filter) ? (object) $filter : $filter,
                'u' => self::normalizeUpdateDocument($newObject),
            ];

            foreach (['multi', 'upsert', 'arrayFilters', 'hint', 'collation', 'sort'] as $key) {
                if (isset($options[$key]) && $options[$key] !== false) {
                    $spec[$key] = $options[$key];
                }
            }

            return $spec;
        }, $operations);

        $command = ['update' => $collection, 'updates' => $updates, 'ordered' => $ordered];

        if (isset($bulkOptions['comment'])) {
            $command['comment'] = $bulkOptions['comment'];
        }

        if (isset($bulkOptions['let'])) {
            $command['let'] = $bulkOptions['let'];
        }

        if ($bulkOptions['bypassDocumentValidation'] ?? false) {
            $command['bypassDocumentValidation'] = true;
        }

        if (isset($bulkOptions['rawData'])) {
            $command['rawData'] = $bulkOptions['rawData'];
        }

        return $command;
    }

    /**
     * Derive the server type from the hello response.
     *
     * @see https://github.com/mongodb/specifications/blob/master/source/server-discovery-and-monitoring/server-discovery-and-monitoring.md#server-type
     */
    private function detectType(): int
    {
        $info = $this->getInfo();

        if (($info['ok'] ?? 0) != 1) {
            return self::TYPE_UNKNOWN;
        }

        if (($info['serviceId'] ?? null) !== null && ($info['isWritablePrimary'] ?? false)) {
            return self::TYPE_LOAD_BALANCER;
        }

        if (($info['msg'] ?? null) === 'isdbgrid') {
            return self::TYPE_MONGOS;
        }

        $setName = $info['setName'] ?? null;

        if ($setName === null) {
            return $info['isWritablePrimary'] ?? false ? self::TYPE_STANDALONE : self::TYPE_UNKNOWN;
        }

        if ($info['isWritablePrimary'] ?? false) {
            return self::TYPE_RS_PRIMARY;
        }

        if ($info['secondary'] ?? false) {
            return self::TYPE_RS_SECONDARY;
        }

        if ($info['arbiterOnly'] ?? false) {
            return self::TYPE_RS_ARBITER;
        }

        if ($info['isreplicaset'] ?? false) {
            return self::TYPE_RS_GHOST;
        }

        return self::TYPE_RS_OTHER;
    }

    /**
     * Execute one batch of a bulk write and return the raw reply document.
     *
     * @param array{type: string, ops: list<array>, globalIndices: list<int>} $batch
     * @param array<string, mixed>                                            $bulkOptions
     *
     * @return array<string, mixed>
     */
    private function executeBatch(
        string $db,
        string $collection,
        array $batch,
        array $bulkOptions,
        bool $ordered,
        WriteConcern $writeConcern,
        ?Session $session,
    ): array {
        $command = match ($batch['type']) {
            'insert' => self::buildInsertCommand($collection, $batch['ops'], $bulkOptions, $ordered),
            'update' => self::buildUpdateCommand($collection, $batch['ops'], $bulkOptions, $ordered),
            'delete' => self::buildDeleteCommand($collection, $batch['ops'], $bulkOptions, $ordered),
            default => throw new InvalidArgumentException(sprintf('Unsupported bulk operation type "%s"', $batch['type'])),
        };

        $prepared = CommandHelper::prepareCommand(
            command: $command,
            db: $db,
            writeConcern: $writeConcern,
            session: $session,
            serverApi: $this->serverApi,
        );

        // The `documents`, `updates`, and `deletes` arrays travel as OP_MSG
        // kind-1 document sequences, not as part of the command body.
        $sequenceId = match ($batch['type']) {
            'insert' => 'documents',
            'update' => 'updates',
            'delete' => 'deletes',
        };

        $docs = array_values($prepared[$sequenceId]);

        return $this->send(
            $db,
            $batch['type'],
            $prepared,
            [['id' => $sequenceId, 'docs' => $docs]],
        );
    }

    /**
     * Read the batch size that the follow-up getMore commands must repeat.
     *
     * The find command carries it as a top-level `batchSize` field, while
     * aggregate carries it inside its `cursor` document.
     *
     * @param array<string, mixed> $prepared
     */
    private static function getBatchSize(array $prepared): int
    {
        if (isset($prepared['batchSize'])) {
            return (int) $prepared['batchSize'];
        }

        $cursor = $prepared['cursor'] ?? null;

        if (is_array($cursor)) {
            return (int) ($cursor['batchSize'] ?? 0);
        }

        if (is_object($cursor)) {
            return (int) ($cursor->batchSize ?? 0);
        }

        return 0;
    }

    /**
     * Whether the command is an unacknowledged write, that is a write whose
     * write concern is w:0.
     *
     * @param array<string, mixed> $prepared
     */
    private static function isUnacknowledged(array $prepared): bool
    {
        $writeConcern = $prepared['writeConcern'] ?? null;

        return is_array($writeConcern) && ($writeConcern['w'] ?? 1) === 0;
    }

    /**
     * Normalize an update document for wire encoding.
     *
     * An empty PHP array becomes an empty document, so it is sent as a
     * replacement rather than as an empty aggregation pipeline. A plain object
     * with consecutive integer keys is an update pipeline and becomes a list.
     */
    private static function normalizeUpdateDocument(array|object $document): array|object
    {
        if (is_array($document)) {
            return count($document) === 0 ? (object) $document : $document;
        }

        if (! $document instanceof stdClass) {
            return $document;
        }

        $properties = get_object_vars($document);

        if ($properties === []) {
            return $document;
        }

        $expectedKey = 0;

        foreach ($properties as $key => $value) {
            if ((string) $key !== (string) $expectedKey) {
                return $document;
            }

            ++$expectedKey;
        }

        return array_values($properties);
    }

    /**
     * Send one command and wrap the reply in a cursor.
     *
     * @param array<string, mixed>|object $command
     */
    private function runCommand(
        string $db,
        Command|array $command,
        ?ReadPreference $readPreference,
        ?ReadConcern $readConcern,
        ?WriteConcern $writeConcern,
        ?Session $session,
        int $maxAwaitTimeMS = 0,
        ?Query $debugQuery = null,
    ): CursorInterface {
        if ($command instanceof Command) {
            $rawCommand = $command->getDocument();
            $debugCommand = $command;
        } else {
            $rawCommand = $command;
            $debugCommand = null;
        }

        $commandName = CommandHelper::getCommandName($rawCommand);

        // A standalone server ignores read preference, so it must not be sent.
        $effectiveReadPreference = $this->getType() === self::TYPE_STANDALONE ? null : $readPreference;

        $prepared = CommandHelper::prepareCommand(
            command: $rawCommand,
            db: $db,
            readPreference: $effectiveReadPreference,
            readConcern: $readConcern,
            writeConcern: $writeConcern,
            session: $session,
            serverApi: $this->serverApi,
        );

        $body = $this->send($db, $commandName, $prepared);

        return $this->buildCursor(
            $body,
            $db,
            $maxAwaitTimeMS,
            self::getBatchSize($prepared),
            $prepared['comment'] ?? null,
            $session,
            $debugCommand,
            $debugQuery,
        );
    }

    /**
     * Send a prepared command and notify the command subscribers.
     *
     * Command monitoring is implemented; SDAM, heartbeat, and connection pool
     * events are not, since the prototype has no topology.
     *
     * @param array<string, mixed>                                    $prepared     Command document, including the bulk payload
     * @param array<int, array{id: string, docs: list<array|object>}> $docSequences
     *
     * @return array<string, mixed> The decoded reply body
     */
    private function send(string $db, string $commandName, array $prepared, array $docSequences = []): array
    {
        $requestId = RequestIdGenerator::next();
        $startedAt = hrtime(true);
        $serverConnectionId = isset($this->getInfo()['connectionId']) ? (int) $this->getInfo()['connectionId'] : null;

        $prepared = $this->applyImplicitSessionId($prepared);
        $moreToCome = self::isUnacknowledged($prepared);

        $this->dispatcher->dispatchCommandStarted(
            $commandName,
            static fn (): object => Document::fromPHP($prepared)->toPHP(['root' => 'object', 'document' => 'object']),
            $db,
            $requestId,
            $this->getHost(),
            $this->getPort(),
            $serverConnectionId,
            $requestId,
        );

        try {
            if ($docSequences === []) {
                $body = $this->connection->sendCommand($db, $prepared, $moreToCome);
            } else {
                $body = $this->connection->sendCommandWithSequences($db, $prepared, $docSequences, $moreToCome);
            }
        } catch (Throwable $e) {
            $reply = $e instanceof CommandException || $e instanceof ExecutionTimeoutException
                ? $e->getResultDocument()
                : null;

            $this->dispatcher->dispatchCommandFailed(
                $commandName,
                $e,
                $db,
                $requestId,
                intdiv(hrtime(true) - $startedAt, 1_000),
                $this->getHost(),
                $this->getPort(),
                $reply,
                $serverConnectionId,
                $requestId,
            );

            throw $e;
        }

        if ($moreToCome) {
            // The server does not reply to a fire-and-forget write. Report the
            // synthetic success reply that ext-mongodb exposes to subscribers.
            $body = ['ok' => 1];
        }

        $this->dispatcher->dispatchCommandSucceeded(
            $commandName,
            (object) $body,
            $db,
            $requestId,
            intdiv(hrtime(true) - $startedAt, 1_000),
            $this->getHost(),
            $this->getPort(),
            $serverConnectionId,
            $requestId,
        );

        return $body;
    }

    /** @return array{string, string} */
    private static function splitNamespace(string $namespace): array
    {
        $parts = explode('.', $namespace, 2);

        if (count($parts) !== 2 || $parts[0] === '' || $parts[1] === '') {
            throw new InvalidArgumentException(sprintf('Invalid namespace "%s": expected "database.collection"', $namespace));
        }

        return [$parts[0], $parts[1]];
    }

    private static function toInt(mixed $value): int
    {
        if ($value instanceof Int64) {
            return (int) (string) $value;
        }

        return (int) $value;
    }
}
