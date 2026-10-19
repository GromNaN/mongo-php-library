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

use Exception;
use InvalidArgumentException as PhpInvalidArgumentException;
use MongoDB\Driver\Exception\InvalidArgumentException;
use MongoDB\Driver\Monitoring\LogSubscriber;
use MongoDB\Driver\Monitoring\Subscriber;
use MongoDB\Internal\Connection\ClientMetadata;
use MongoDB\Internal\Connection\Connection;
use MongoDB\Internal\Monitoring\Dispatcher;
use MongoDB\Internal\Uri\ConnectionString;
use MongoDB\Internal\Uri\SrvResolver;
use MongoDB\Internal\Uri\UriOptions;

use function array_key_exists;
use function array_merge;
use function count;
use function get_debug_type;
use function is_array;
use function is_int;
use function is_string;
use function sprintf;
use function str_ends_with;
use function strtolower;

/**
 * The entry point of the driver: holds the connection settings and hands out
 * servers and sessions.
 *
 * This prototype opens a single connection to the first host of the URI and
 * reuses it for every operation. Topology monitoring, connection pooling, and
 * server selection are not implemented: the server is always considered
 * available, and the read preference is forwarded to the server as-is.
 *
 * @see https://php.net/manual/en/class.mongodb-driver-manager.php
 */
final class Manager
{
    private const DEFAULT_URI = 'mongodb://127.0.0.1:27017';

    private readonly string $uri;
    private readonly ConnectionString $connectionString;
    private readonly UriOptions $uriOptions;
    private readonly ReadConcern $readConcern;
    private readonly ReadPreference $readPreference;
    private readonly WriteConcern $writeConcern;
    private readonly ?ServerApi $serverApi;

    /** @var list<array{host: string, port: int}> */
    private readonly array $hosts;

    private readonly Dispatcher $dispatcher;

    private ?Server $server = null;

    /**
     * @param array<string, mixed>|null $uriOptions    Connection string options
     * @param array<string, mixed>|null $driverOptions Driver-specific options
     */
    public function __construct(
        ?string $uri = null,
        ?array $uriOptions = null,
        ?array $driverOptions = null,
    ) {
        $this->uri = $uri ?? self::DEFAULT_URI;

        try {
            $this->connectionString = new ConnectionString($this->uri);
        } catch (PhpInvalidArgumentException $e) {
            throw new InvalidArgumentException($e->getMessage(), 0, $e);
        }

        $serverApi = $driverOptions['serverApi'] ?? null;

        if ($serverApi !== null && ! $serverApi instanceof ServerApi) {
            throw InvalidArgumentException::invalidDriverOptionType('serverApi', $serverApi, ServerApi::class);
        }

        $this->serverApi = $serverApi;

        /* The check runs on the seed list, before SRV resolution, so that a
         * non-genuine host is reported even when the lookup fails. */
        self::logNonGenuineHosts($this->connectionString->getHosts());

        $this->hosts = $this->resolveHosts();

        $driverInfo = $driverOptions['driver'] ?? [];

        if (! is_array($driverInfo)) {
            throw InvalidArgumentException::invalidDriverOptionType('driver', $driverInfo, 'array');
        }

        self::validateDriverInfo($driverInfo);

        $options = $this->mergeOptions($uriOptions ?? [], $driverInfo);

        $this->readPreference = self::buildReadPreference($options);
        $this->writeConcern = self::buildWriteConcern($options);
        $this->readConcern = self::buildReadConcern($options);
        $this->uriOptions = UriOptions::fromArray($options);
        $this->dispatcher = new Dispatcher();
    }

    public function __debugInfo(): array
    {
        return [
            'uri' => $this->uri,
            'cluster' => [
                [
                    'host' => $this->hosts[0]['host'],
                    'port' => $this->hosts[0]['port'],
                    'type' => $this->server?->getType() ?? Server::TYPE_UNKNOWN,
                    'is_primary' => $this->server?->isPrimary() ?? false,
                    'is_secondary' => $this->server?->isSecondary() ?? false,
                    'is_arbiter' => $this->server?->isArbiter() ?? false,
                    'is_hidden' => $this->server?->isHidden() ?? false,
                    'is_passive' => $this->server?->isPassive() ?? false,
                    'last_hello_response' => $this->server?->getInfo() ?? [],
                    'round_trip_time' => $this->server?->getLatency(),
                ],
            ],
            'cryptSharedVersion' => null,
        ];
    }

    public function __destruct()
    {
        $this->server?->close();
    }

    public function __sleep(): array
    {
        throw new Exception("Serialization of 'MongoDB\\Driver\\Manager' is not allowed");
    }

    public function __wakeup(): void
    {
        throw new Exception("Unserialization of 'MongoDB\\Driver\\Manager' is not allowed");
    }

    /**
     * Command subscribers are notified. SDAM, heartbeat, and connection pool
     * events are never emitted, since the prototype has no topology.
     */
    public function addSubscriber(Subscriber $subscriber): void
    {
        $this->dispatcher->addSubscriber($subscriber);
    }

    /** @param array<string, mixed> $options */
    public function createClientEncryption(array $options): ClientEncryption
    {
        return new ClientEncryption($options);
    }

    /** @param array{writeConcern?: WriteConcern, session?: Session}|null $options */
    public function executeBulkWrite(string $namespace, BulkWrite $bulk, ?array $options = null): WriteResult
    {
        return $this->selectServer()->executeBulkWrite($namespace, $bulk, $options);
    }

    public function executeBulkWriteCommand(BulkWriteCommand $bulkWriteCommand, ?array $options = null): BulkWriteCommandResult
    {
        return $this->selectServer()->executeBulkWriteCommand($bulkWriteCommand, $options);
    }

    /** @param array{readPreference?: ReadPreference, session?: Session}|null $options */
    public function executeCommand(string $db, Command $command, ?array $options = null): CursorInterface
    {
        return $this->selectServer()->executeCommand($db, $command, $options);
    }

    /** @param array{readPreference?: ReadPreference, session?: Session}|null $options */
    public function executeQuery(string $namespace, Query $query, ?array $options = null): CursorInterface
    {
        return $this->selectServer()->executeQuery($namespace, $query, $options);
    }

    /** @param array{readConcern?: ReadConcern, readPreference?: ReadPreference, session?: Session}|null $options */
    public function executeReadCommand(string $db, Command $command, ?array $options = null): CursorInterface
    {
        return $this->selectServer()->executeReadCommand($db, $command, $options);
    }

    /** @param array{readConcern?: ReadConcern, readPreference?: ReadPreference, session?: Session, writeConcern?: WriteConcern}|null $options */
    public function executeReadWriteCommand(string $db, Command $command, ?array $options = null): CursorInterface
    {
        return $this->selectServer()->executeReadWriteCommand($db, $command, $options);
    }

    /** @param array{writeConcern?: WriteConcern, session?: Session}|null $options */
    public function executeWriteCommand(string $db, Command $command, ?array $options = null): CursorInterface
    {
        return $this->selectServer()->executeWriteCommand($db, $command, $options);
    }

    public function getEncryptedFieldsMap(): array|object|null
    {
        return null;
    }

    public function getReadConcern(): ReadConcern
    {
        return $this->readConcern;
    }

    public function getReadPreference(): ReadPreference
    {
        return $this->readPreference;
    }

    /** @return list<Server> */
    public function getServers(): array
    {
        return [$this->selectServer()];
    }

    public function getWriteConcern(): WriteConcern
    {
        return $this->writeConcern;
    }

    public function removeSubscriber(Subscriber $subscriber): void
    {
        $this->dispatcher->removeSubscriber($subscriber);
    }

    /**
     * Return the server to run an operation on.
     *
     * The prototype connects to the first host of the URI and ignores the read
     * preference, since there is no topology to select from.
     */
    public function selectServer(?ReadPreference $readPreference = null): Server
    {
        if ($this->server !== null) {
            return $this->server;
        }

        $host = $this->hosts[0];
        $connection = new Connection($host['host'], $host['port'], $this->uriOptions);
        $connection->connect();

        return $this->server = Server::create(
            $connection,
            $this->readConcern,
            $readPreference ?? $this->readPreference,
            $this->writeConcern,
            $this->dispatcher,
            $this->serverApi,
        );
    }

    /** @param array<string, mixed>|null $options */
    public function startSession(?array $options = null): Session
    {
        return Session::create(fn (): Server => $this->selectServer(), $options ?? []);
    }

    /** @param array<string, mixed> $options */
    private static function buildReadConcern(array $options): ReadConcern
    {
        $level = $options['readConcernLevel'] ?? null;

        if ($level !== null && ! is_string($level)) {
            throw new InvalidArgumentException(
                sprintf('Expected string for "readConcernLevel" URI option, %s given', get_debug_type($level)),
            );
        }

        return new ReadConcern($level);
    }

    /** @param array<string, mixed> $options */
    private static function buildReadPreference(array $options): ReadPreference
    {
        $maxStaleness = $options['maxStalenessSeconds'] ?? null;

        if ($maxStaleness !== null && ! is_int($maxStaleness)) {
            throw new InvalidArgumentException(
                sprintf('Expected integer for "maxStalenessSeconds" URI option, %s given', get_debug_type($maxStaleness)),
            );
        }

        return new ReadPreference(
            $options['readPreference'] ?? ReadPreference::PRIMARY,
            $options['readPreferenceTags'] ?? null,
            $maxStaleness !== null ? ['maxStalenessSeconds' => $maxStaleness] : null,
        );
    }

    /** @param array<string, mixed> $options */
    private static function buildWriteConcern(array $options): WriteConcern
    {
        $w = $options['w'] ?? null;
        $wtimeout = isset($options['wTimeoutMS']) ? (int) $options['wTimeoutMS'] : null;
        $journal = $options['journal'] ?? null;

        if ($w === '' || ($w === null && $wtimeout === null && $journal === null)) {
            return WriteConcern::createDefault();
        }

        return new WriteConcern($w ?? WriteConcern::MAJORITY, $wtimeout ?? 0, $journal);
    }

    /**
     * Report a seed list that points to a cluster that is not genuine MongoDB.
     *
     * Some MongoDB-compatible services do not support the full feature set, so
     * the driver tells the application about it through the logging subsystem.
     *
     * @see https://jira.mongodb.org/browse/DRIVERS-2583
     *
     * @param list<array{host: string, port: int}> $hosts
     */
    private static function logNonGenuineHosts(array $hosts): void
    {
        foreach ($hosts as $host) {
            $hostname = strtolower($host['host']);

            if (str_ends_with($hostname, '.mongo.cosmos.azure.com')) {
                Dispatcher::log(
                    LogSubscriber::LEVEL_INFO,
                    'mongoc',
                    'You appear to be connected to a CosmosDB cluster. For more information regarding feature compatibility and support please visit https://www.mongodb.com/supportability/cosmosdb',
                );

                return;
            }

            if (
                str_ends_with($hostname, '.docdb.amazonaws.com')
                || str_ends_with($hostname, '.docdb-elastic.amazonaws.com')
            ) {
                Dispatcher::log(
                    LogSubscriber::LEVEL_INFO,
                    'mongoc',
                    'You appear to be connected to a DocumentDB cluster. For more information regarding feature compatibility and support please visit https://www.mongodb.com/supportability/documentdb',
                );

                return;
            }
        }
    }

    /**
     * Merge URI options, credentials, and code options into one option map.
     *
     * @param array<string, mixed> $uriOptions
     * @param array<string, mixed> $driverInfo
     *
     * @return array<string, mixed>
     */
    private function mergeOptions(array $uriOptions, array $driverInfo): array
    {
        $normalized = [];

        foreach ($uriOptions as $key => $value) {
            $normalized[ConnectionString::normalizeOptionKey((string) $key)] = $value;
        }

        $credentialOptions = [];
        $username = $this->connectionString->getUsername();

        if ($username !== null) {
            $credentialOptions['username'] = $username;
            $credentialOptions['password'] = $this->connectionString->getPassword() ?? '';
        }

        // Priority, lowest first: URI query string, credentials, code options.
        $merged = array_merge($this->connectionString->getOptions(), $credentialOptions, $normalized);

        if (isset($merged['username'])) {
            $merged['authSource'] ??= $this->connectionString->getDatabase() ?? 'admin';
        }

        $this->validateOptions($merged);

        $merged['clientMetadata'] = ClientMetadata::build($merged['appname'] ?? null, $driverInfo);

        return $merged;
    }

    /**
     * Resolve the seed list, following SRV and TXT records for mongodb+srv:// URIs.
     *
     * @return list<array{host: string, port: int}>
     */
    private function resolveHosts(): array
    {
        $hosts = $this->connectionString->getHosts();

        if (! $this->connectionString->isSrv()) {
            return $hosts;
        }

        $options = $this->connectionString->getOptions();
        $serviceName = (string) ($options['srvServiceName'] ?? 'mongodb');
        $maxHosts = (int) ($options['srvMaxHosts'] ?? 0);

        return SrvResolver::resolve($hosts[0]['host'], $serviceName, $maxHosts)->hosts;
    }

    /**
     * The "driver" driver option carries the name, version, and platform
     * appended to the handshake metadata. Every value must be a string.
     *
     * @param array<string, mixed> $driverInfo
     */
    private static function validateDriverInfo(array $driverInfo): void
    {
        foreach ($driverInfo as $key => $value) {
            if (! is_string($value)) {
                throw new InvalidArgumentException(
                    sprintf('Expected string for "driver.%s" option, %s given', $key, get_debug_type($value)),
                );
            }
        }
    }

    /** @param array<string, mixed> $options */
    private function validateOptions(array $options): void
    {
        if (($options['journal'] ?? null) === true && ($options['w'] ?? null) === 0) {
            throw new InvalidArgumentException('Journal conflicts with w value: 0');
        }

        if (array_key_exists('authSource', $options) && $options['authSource'] === '') {
            throw new InvalidArgumentException(
                'Failed to parse URI options: authSource may not be specified as an empty string',
            );
        }

        if (($options['directConnection'] ?? false) && count($this->connectionString->getHosts()) > 1) {
            throw new InvalidArgumentException(
                'Failed to parse URI options: Multiple seeds not allowed with directConnection option',
            );
        }
    }
}
