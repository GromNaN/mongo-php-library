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

namespace MongoDB\Internal\Connection;

use MongoDB\Driver\Exception\CommandException;
use MongoDB\Driver\Exception\ConnectionException;
use MongoDB\Driver\Exception\ConnectionTimeoutException;
use MongoDB\Driver\Exception\ExecutionTimeoutException;
use MongoDB\Internal\Protocol\MessageHeader;
use MongoDB\Internal\Protocol\OpMsgDecoder;
use MongoDB\Internal\Protocol\OpMsgEncoder;
use MongoDB\Internal\Uri\UriOptions;

use function fclose;
use function fread;
use function fwrite;
use function hrtime;
use function is_array;
use function is_resource;
use function sprintf;
use function stream_context_create;
use function stream_get_meta_data;
use function stream_set_blocking;
use function stream_set_timeout;
use function stream_socket_client;
use function strlen;
use function substr;
use function unpack;

use const STREAM_CLIENT_CONNECT;

/**
 * A single blocking TCP connection to a MongoDB server.
 *
 * The transport uses plain PHP streams: `stream_socket_client()` for the
 * connection, `fwrite()` and `fread()` for I/O, and `stream_set_timeout()` for
 * the socket timeout. No event loop is involved, so every call blocks until the
 * server replies. This is the default transport described by decision D6 of the
 * decoupling design document.
 *
 * Authentication is not implemented: the connection is usable against servers
 * that do not require credentials.
 *
 * @internal
 */
final class Connection
{
    /** Default connect timeout, per the URI options spec. */
    private const DEFAULT_CONNECT_TIMEOUT_MS = 10_000;

    /** Maximum number of bytes read from the socket at once. */
    private const READ_CHUNK_SIZE = 65536;

    /** @var resource|null */
    private $stream = null;

    private bool $ready = false;
    private int $maxWireVersion = 0;
    private int $minWireVersion = 0;
    private ?string $serviceId = null;
    private ?float $roundTripTime = null;

    /** @var array<string, mixed> */
    private array $helloResponse = [];

    private float $socketTimeoutSecs = 0.0;

    public function __construct(
        private readonly string $host,
        private readonly int $port,
        private readonly ?UriOptions $options = null,
    ) {
    }

    public function __destruct()
    {
        $this->close();
    }

    public function close(): void
    {
        if (is_resource($this->stream)) {
            fclose($this->stream);
        }

        $this->stream = null;
        $this->ready = false;
    }

    /**
     * Open the TCP (or TLS) connection and run the hello handshake.
     *
     * @throws ConnectionTimeoutException if the connection cannot be established in time
     * @throws ConnectionException        on any other I/O error
     */
    public function connect(): void
    {
        $connectTimeoutMs = isset($this->options->connectTimeoutMS) && $this->options->connectTimeoutMS > 0
            ? $this->options->connectTimeoutMS
            : self::DEFAULT_CONNECT_TIMEOUT_MS;

        $scheme = $this->isTlsEnabled() ? 'ssl' : 'tcp';
        $context = stream_context_create($this->buildStreamContextOptions());

        $stream = @stream_socket_client(
            sprintf('%s://%s:%d', $scheme, $this->host, $this->port),
            $errorCode,
            $errorMessage,
            $connectTimeoutMs / 1000,
            STREAM_CLIENT_CONNECT,
            $context,
        );

        if ($stream === false) {
            throw new ConnectionTimeoutException(
                sprintf(
                    'Connection to %s:%d failed: %s (error code %d)',
                    $this->host,
                    $this->port,
                    $errorMessage,
                    $errorCode,
                ),
                $errorCode,
            );
        }

        $this->stream = $stream;
        stream_set_blocking($stream, true);

        // During the handshake, apply connectTimeoutMS as the I/O timeout.
        $this->setTimeout($connectTimeoutMs / 1000);
        $this->runHello();

        // After the handshake, the per-operation socket timeout takes over.
        $this->setTimeout(
            isset($this->options->socketTimeoutMS) && $this->options->socketTimeoutMS > 0
                ? $this->options->socketTimeoutMS / 1000
                : 0.0,
        );

        $this->ready = true;
    }

    /** @return array<string, mixed> */
    public function getHelloResponse(): array
    {
        return $this->helloResponse;
    }

    public function getHost(): string
    {
        return $this->host;
    }

    public function getMaxWireVersion(): int
    {
        return $this->maxWireVersion;
    }

    public function getMinWireVersion(): int
    {
        return $this->minWireVersion;
    }

    public function getPort(): int
    {
        return $this->port;
    }

    public function getRoundTripTime(): ?float
    {
        return $this->roundTripTime;
    }

    public function getServiceId(): ?string
    {
        return $this->serviceId;
    }

    public function isReady(): bool
    {
        return $this->ready;
    }

    /**
     * Send a command to the given database and return the decoded reply body.
     *
     * @param array|object $command    Command document; `$db` is injected automatically.
     * @param bool         $moreToCome Unacknowledged write: the server is asked
     *                                 not to reply, and no reply is read.
     *
     * @throws CommandException     when the server replies with ok != 1
     * @throws ExecutionTimeoutException when the server reports a maxTimeMS expiry (code 50)
     */
    public function sendCommand(string $db, array|object $command, bool $moreToCome = false): array
    {
        if (is_array($command)) {
            $command['$db'] = $db;
        } else {
            $command->{'$db'} = $db;
        }

        [$bytes] = OpMsgEncoder::encodeWithRequestId(
            $command,
            [],
            $moreToCome ? OpMsgEncoder::FLAG_MORE_TO_COME : 0,
        );

        if ($moreToCome) {
            $this->writeAll($bytes);

            return [];
        }

        return $this->checkReply($this->decodeBody($this->sendMessage($bytes)));
    }

    /**
     * Send a command whose bulk payload travels as an OP_MSG kind-1 document sequence.
     *
     * The payload must not appear in the command body as well, so the field named
     * by each sequence is removed before encoding.
     *
     * @param array<int, array{id: string, docs: list<array|object>}> $docSequences
     * @param bool                                                    $moreToCome   Unacknowledged write: no reply is expected or read.
     */
    public function sendCommandWithSequences(
        string $db,
        array|object $command,
        array $docSequences,
        bool $moreToCome = false,
    ): array {
        $body = is_array($command) ? $command : (array) $command;
        $body['$db'] = $db;

        foreach ($docSequences as $sequence) {
            unset($body[$sequence['id']]);
        }

        [$bytes] = OpMsgEncoder::encodeWithRequestId(
            $body,
            $docSequences,
            $moreToCome ? OpMsgEncoder::FLAG_MORE_TO_COME : 0,
        );

        if ($moreToCome) {
            $this->writeAll($bytes);

            return [];
        }

        return $this->checkReply($this->decodeBody($this->sendMessage($bytes)));
    }

    /**
     * Write raw wire-protocol bytes and read back exactly one reply message.
     *
     * @throws ConnectionException on I/O or framing errors
     */
    public function sendMessage(string $bytes): string
    {
        if ($this->stream === null) {
            throw new ConnectionException('Cannot send message: connection is not open');
        }

        $this->writeAll($bytes);

        // The first four bytes of every wire message hold the total length.
        $prefix = $this->readExactly(4);

        /** @var array{1: int} $unpacked */
        $unpacked = unpack('V', $prefix);
        $messageLength = $unpacked[1];

        if ($messageLength < MessageHeader::HEADER_SIZE) {
            throw new ConnectionException(
                sprintf('Received malformed message: length %d is too small', $messageLength),
            );
        }

        return $prefix . $this->readExactly($messageLength - 4);
    }

    private function buildStreamContextOptions(): array
    {
        if (! $this->isTlsEnabled()) {
            return [];
        }

        $ssl = [
            'verify_peer' => ! ($this->options->tlsAllowInvalidCertificates ?? false),
            'verify_peer_name' => ! ($this->options->tlsAllowInvalidHostnames ?? false),
        ];

        if (isset($this->options->tlsCAFile)) {
            $ssl['cafile'] = $this->options->tlsCAFile;
        }

        if (isset($this->options->tlsCertificateKeyFile)) {
            $ssl['local_cert'] = $this->options->tlsCertificateKeyFile;

            if (isset($this->options->tlsCertificateKeyFilePassword)) {
                $ssl['passphrase'] = $this->options->tlsCertificateKeyFilePassword;
            }
        }

        return ['ssl' => $ssl];
    }

    /**
     * @param array<string, mixed> $body
     *
     * @return array<string, mixed>
     */
    private function checkReply(array $body): array
    {
        if ((int) ($body['ok'] ?? 0) === 1) {
            return $body;
        }

        $message = (string) ($body['errmsg'] ?? 'Unknown error');
        $code = (int) ($body['code'] ?? 0);

        if ($code === 50) {
            throw ExecutionTimeoutException::createFromServerReply($message, $code, (object) $body);
        }

        throw CommandException::createFromServerReply($message, $code, (object) $body);
    }

    /** @return array<string, mixed> */
    private function decodeBody(string $bytes): array
    {
        $decoded = OpMsgDecoder::decode($bytes);
        $body = $decoded['body'];

        return is_array($body) ? $body : (array) $body;
    }

    private function isTlsEnabled(): bool
    {
        if ($this->options === null) {
            return false;
        }

        return ($this->options->tls ?? false)
            || ($this->options->ssl ?? false)
            || isset($this->options->tlsCAFile)
            || isset($this->options->tlsCertificateKeyFile);
    }

    private function readExactly(int $length): string
    {
        $buffer = '';

        while (strlen($buffer) < $length) {
            $remaining = $length - strlen($buffer);
            $chunk = @fread($this->stream, $remaining > self::READ_CHUNK_SIZE ? self::READ_CHUNK_SIZE : $remaining);

            if ($chunk === false || $chunk === '') {
                $meta = stream_get_meta_data($this->stream);

                if (! empty($meta['timed_out'])) {
                    throw new ConnectionTimeoutException(
                        sprintf(
                            'Reading from %s:%d timed out after %.3f seconds',
                            $this->host,
                            $this->port,
                            $this->socketTimeoutSecs,
                        ),
                    );
                }

                throw new ConnectionException(
                    sprintf(
                        'Connection to %s:%d closed while reading: got %d of %d bytes',
                        $this->host,
                        $this->port,
                        strlen($buffer),
                        $length,
                    ),
                );
            }

            $buffer .= $chunk;
        }

        return $buffer;
    }

    /**
     * Run the hello handshake and record the server's capabilities.
     */
    private function runHello(): void
    {
        $startedAt = hrtime(true);

        $hello = ['hello' => 1, '$db' => 'admin'];

        $clientMetadata = $this->options?->clientMetadata;
        if ($clientMetadata !== null) {
            $hello['client'] = $clientMetadata;
        }

        [$bytes] = OpMsgEncoder::encodeWithRequestId($hello);

        // Decode without the ok check: an older server may answer ok:0 to
        // "hello" while still being usable through the legacy "isMaster" name.
        $body = $this->decodeBody($this->sendMessage($bytes));

        $this->roundTripTime = (hrtime(true) - $startedAt) / 1e6;
        $this->helloResponse = $body;
        $this->maxWireVersion = (int) ($body['maxWireVersion'] ?? 0);
        $this->minWireVersion = (int) ($body['minWireVersion'] ?? 0);
        $this->serviceId = isset($body['serviceId']) ? (string) $body['serviceId'] : null;
    }

    private function setTimeout(float $seconds): void
    {
        $this->socketTimeoutSecs = $seconds;

        if ($seconds <= 0.0) {
            // Zero means "no timeout": stream_set_timeout(0) would return
            // immediately, so keep the default (unlimited) blocking behaviour.
            return;
        }

        stream_set_timeout(
            $this->stream,
            (int) $seconds,
            (int) (($seconds - (int) $seconds) * 1_000_000),
        );
    }

    private function writeAll(string $bytes): void
    {
        $offset = 0;
        $length = strlen($bytes);

        while ($offset < $length) {
            $written = @fwrite($this->stream, substr($bytes, $offset));

            if ($written === false || $written === 0) {
                throw new ConnectionException(
                    sprintf('Failed to write %d bytes to %s:%d', $length - $offset, $this->host, $this->port),
                );
            }

            $offset += $written;
        }
    }
}
