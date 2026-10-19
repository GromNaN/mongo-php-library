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

use MongoDB\BSON\Serializable;
use MongoDB\Driver\Exception\InvalidArgumentException;
use stdClass;

use function ctype_digit;
use function is_bool;
use function is_int;
use function is_string;
use function sprintf;
use function str_starts_with;
use function substr;

final class WriteConcern implements Serializable
{
    public const MAJORITY = 'majority';

    /** The effective w value, null when the concern is the driver default. */
    public string|int|null $w = null;

    /** Whether journaling is enabled, null when not requested. */
    public ?bool $j = null;

    /** The timeout in milliseconds, 0 when not requested. */
    public int $wtimeout = 0;

    public function __construct(string|int $w, ?int $wtimeout = null, ?bool $journal = null)
    {
        // Validate w range
        if (is_int($w) && $w < -3) {
            throw new InvalidArgumentException(
                'Expected w to be >= -3, ' . $w . ' given',
            );
        }

        // Validate wtimeout range
        if ($wtimeout !== null) {
            if ($wtimeout < 0) {
                throw new InvalidArgumentException(
                    'Expected wtimeout to be >= 0, ' . $wtimeout . ' given',
                );
            }
        }

        if ($journal === true && $w === 0) {
            throw new InvalidArgumentException('Cannot enable journaling when using w = 0');
        }

        // Map special integer w values
        if ($w === -2) {
            // isDefault — no w property set
            $effectiveW = null;
        } elseif ($w === -3) {
            $effectiveW = self::MAJORITY;
        } else {
            $effectiveW = $w;
        }

        $this->applyState($effectiveW, $wtimeout ?? 0, $journal);
    }

    public function __debugInfo(): array
    {
        $info = [];
        if ($this->w !== null) {
            $info['w'] = $this->w;
        }

        if ($this->j !== null) {
            $info['j'] = $this->j;
        }

        if ($this->wtimeout !== 0) {
            $info['wtimeout'] = $this->wtimeout;
        }

        return $info;
    }

    public function __serialize(): array
    {
        $data = [];
        if ($this->w !== null) {
            $data['w'] = $this->w;
        }

        if ($this->j !== null) {
            $data['j'] = $this->j;
        }

        if ($this->wtimeout !== 0) {
            $data['wtimeout'] = $this->wtimeout;
        }

        return $data;
    }

    public static function __set_state(array $properties): static
    {
        $w        = $properties['w'] ?? null;
        $journal  = $properties['j'] ?? null;
        $wtimeout = $properties['wtimeout'] ?? null;

        // Validate w
        if ($w !== null) {
            if (! is_int($w) && ! is_string($w)) {
                throw InvalidArgumentException::initializationRequiresFieldType(self::class, 'w', 'integer or string');
            }

            if (is_int($w) && $w < -3) {
                throw new InvalidArgumentException(
                    'MongoDB\Driver\WriteConcern initialization requires "w" integer field to be >= -3',
                );
            }
        }

        // Validate wtimeout
        if ($wtimeout !== null) {
            if (is_string($wtimeout)) {
                // Accept numeric strings, which ext-mongodb also coerces to int
                $positive = str_starts_with($wtimeout, '-') ? substr($wtimeout, 1) : $wtimeout;
                if (! ctype_digit($positive) || $positive === '') {
                    throw new InvalidArgumentException(
                        sprintf('Error parsing "%s" as 64-bit value for MongoDB\\Driver\\WriteConcern initialization', $wtimeout),
                    );
                }

                // Negative string → same error as integer < 0
                if (str_starts_with($wtimeout, '-')) {
                    throw new InvalidArgumentException(
                        'MongoDB\Driver\WriteConcern initialization requires "wtimeout" integer field to be >= 0',
                    );
                }
            } elseif (! is_int($wtimeout)) {
                throw InvalidArgumentException::initializationRequiresFieldType(self::class, 'wtimeout', 'integer or string');
            }

            if ($wtimeout < 0) {
                throw new InvalidArgumentException(
                    'MongoDB\Driver\WriteConcern initialization requires "wtimeout" integer field to be >= 0',
                );
            }
        }

        // Validate journal
        if ($journal !== null && ! is_bool($journal)) {
            throw InvalidArgumentException::initializationRequiresFieldType(self::class, 'j', 'boolean');
        }

        // Map w=-3 to 'majority', w=-2 to null (isDefault)
        if ($w === -3) {
            $w = self::MAJORITY;
        } elseif ($w === -2) {
            $w = null;
        }

        $obj = new static(-2); // Start with isDefault
        $obj->applyState($w, $wtimeout ?? 0, $journal);

        return $obj;
    }

    public function __unserialize(array $data): void
    {
        $w        = $data['w'] ?? null;
        $wtimeout = $data['wtimeout'] ?? 0;
        $journal  = isset($data['j']) ? (bool) $data['j'] : null;

        if ($journal === true && $w === 0) {
            throw new InvalidArgumentException('Cannot enable journaling when using w = 0');
        }

        $this->applyState($w, is_string($wtimeout) ? $wtimeout : (int) $wtimeout, $journal);
    }

    public function bsonSerialize(): stdClass
    {
        $doc = new stdClass();

        if ($this->w !== null) {
            $doc->w = $this->w;
        }

        if ($this->j !== null) {
            $doc->j = $this->j;
        }

        if ($this->wtimeout !== 0) {
            $doc->wtimeout = $this->wtimeout;
        }

        return $doc;
    }

    public function getJournal(): ?bool
    {
        return $this->j;
    }

    public function getW(): string|int|null
    {
        return $this->w;
    }

    public function getWtimeout(): int
    {
        return $this->wtimeout;
    }

    public function isDefault(): bool
    {
        return $this->w === null;
    }

    /** @internal Create a WriteConcern that reports isDefault() = true */
    public static function createDefault(): self
    {
        return new self(-2);
    }

    private function applyState(string|int|null $w, int|string $wtimeout, ?bool $journal): void
    {
        $this->w        = $w;
        $this->j        = $journal;
        $this->wtimeout = (int) $wtimeout;
    }
}
