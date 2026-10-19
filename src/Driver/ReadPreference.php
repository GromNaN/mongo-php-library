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

use MongoDB\BSON\PackedArray;
use MongoDB\BSON\Serializable;
use MongoDB\Driver\Exception\InvalidArgumentException;
use MongoDB\Driver\Exception\UnexpectedValueException;
use stdClass;

use function array_map;
use function in_array;
use function is_array;
use function is_object;
use function is_string;
use function sprintf;
use function strtolower;
use function trigger_error;

use const E_USER_DEPRECATED;

final class ReadPreference implements Serializable
{
    public const PRIMARY = 'primary';
    public const PRIMARY_PREFERRED = 'primaryPreferred';
    public const SECONDARY = 'secondary';
    public const SECONDARY_PREFERRED = 'secondaryPreferred';
    public const NEAREST = 'nearest';

    public const NO_MAX_STALENESS = -1;
    public const SMALLEST_MAX_STALENESS_SECONDS = 90;

    /** The read preference mode, one of the class constants. */
    public string $mode;

    /** The tag sets as a list of documents, null when none are set. */
    public ?array $tags = null;

    /** The staleness limit in seconds, NO_MAX_STALENESS when unset. */
    public int $maxStalenessSeconds = self::NO_MAX_STALENESS;

    /** The hedge document, null when not set. */
    public ?object $hedge = null;

    private const MODE_MAP = [
        'primary'            => self::PRIMARY,
        'primarypreferred'   => self::PRIMARY_PREFERRED,
        'secondary'          => self::SECONDARY,
        'secondarypreferred' => self::SECONDARY_PREFERRED,
        'nearest'            => self::NEAREST,
    ];

    public function __construct(string $mode, ?array $tagSets = null, ?array $options = null)
    {
        $canonicalMode = self::MODE_MAP[strtolower($mode)] ?? null;
        if ($canonicalMode === null) {
            throw new InvalidArgumentException(sprintf("Unsupported readPreference value: '%s'", $mode));
        }

        $mode = $canonicalMode;

        // Validate and convert tagSets
        $normalizedTagSets = null;
        if ($tagSets !== null && $tagSets !== []) {
            // First validate each item's type
            foreach ($tagSets as $tagSet) {
                if (! is_array($tagSet) && ! is_object($tagSet)) {
                    throw new InvalidArgumentException(
                        'tagSets must be an array of zero or more documents',
                    );
                }
            }

            // Then check primary mode restriction
            if ($mode === self::PRIMARY) {
                throw new InvalidArgumentException('tagSets may not be used with primary mode');
            }

            // Convert array items to stdClass (without modifying original)
            $normalizedTagSets = [];
            foreach ($tagSets as $tagSet) {
                $normalizedTagSets[] = is_array($tagSet) ? (object) $tagSet : $tagSet;
            }
        }

        // Validate maxStalenessSeconds
        $maxStalenessSeconds = self::NO_MAX_STALENESS;
        if (isset($options['maxStalenessSeconds'])) {
            $ms = $options['maxStalenessSeconds'];
            if ($mode === self::PRIMARY) {
                throw new InvalidArgumentException('Primary read preference mode conflicts with maxStalenessSeconds');
            }

            if ($ms > 2147483647) {
                throw new InvalidArgumentException(
                    'Expected maxStalenessSeconds to be <= 2147483647, ' . $ms . ' given',
                );
            }

            if ($ms !== self::NO_MAX_STALENESS && $ms < self::SMALLEST_MAX_STALENESS_SECONDS) {
                throw new InvalidArgumentException(
                    'Expected maxStalenessSeconds to be >= 90, ' . $ms . ' given',
                );
            }

            $maxStalenessSeconds = $ms;
        }

        // Handle hedge option
        $hedge = null;
        if (isset($options['hedge'])) {
            $hedgeVal = $options['hedge'];
            trigger_error(
                'MongoDB\Driver\ReadPreference::__construct(): The "hedge" option is deprecated as of MongoDB 8.0 and will be removed in a future release',
                E_USER_DEPRECATED,
            );

            if ($hedgeVal instanceof PackedArray) {
                throw UnexpectedValueException::documentRequiredAsRoot();
            }

            if (! is_array($hedgeVal) && ! is_object($hedgeVal)) {
                throw new InvalidArgumentException('hedge must be an array or object');
            }

            if ($mode === self::PRIMARY) {
                throw new InvalidArgumentException('hedge may not be used with primary mode');
            }

            $hedgeObj = (object) $hedgeVal;
            $hedge    = ((array) $hedgeObj) !== [] ? $hedgeObj : null;
        }

        $this->applyState($mode, $normalizedTagSets, $maxStalenessSeconds, $hedge);
    }

    public function __serialize(): array
    {
        $data = ['mode' => $this->mode];

        if ($this->tags !== null) {
            $data['tags'] = $this->tags;
        }

        if ($this->maxStalenessSeconds !== self::NO_MAX_STALENESS) {
            $data['maxStalenessSeconds'] = $this->maxStalenessSeconds;
        }

        if ($this->hedge !== null) {
            $data['hedge'] = $this->hedge;
        }

        return $data;
    }

    public static function __set_state(array $properties): static
    {
        $mode = $properties['mode'] ?? null;

        // Validate mode type
        if (! is_string($mode)) {
            throw new InvalidArgumentException(
                'MongoDB\Driver\ReadPreference initialization requires "mode" field to be string',
            );
        }

        // Validate mode value
        if (! in_array($mode, self::MODE_MAP, true)) {
            throw new InvalidArgumentException(
                'MongoDB\Driver\ReadPreference initialization requires specific values for "mode" string field',
            );
        }

        // Validate tags
        $tags = null;
        if (isset($properties['tags'])) {
            if (! is_array($properties['tags'])) {
                throw new InvalidArgumentException(
                    'MongoDB\Driver\ReadPreference initialization requires "tags" field to be array',
                );
            }

            foreach ($properties['tags'] as $tagSet) {
                if (! is_array($tagSet) && ! is_object($tagSet)) {
                    throw new InvalidArgumentException(
                        'MongoDB\Driver\ReadPreference initialization requires "tags" array field to have zero or more documents',
                    );
                }
            }

            if ($properties['tags'] !== [] && $mode === self::PRIMARY) {
                throw new InvalidArgumentException(
                    'MongoDB\Driver\ReadPreference initialization requires "tags" array field to not be present with "primary" mode',
                );
            }

            // Convert arrays to stdClass
            $normalizedTags = [];
            foreach ($properties['tags'] as $tagSet) {
                $normalizedTags[] = is_array($tagSet) ? (object) $tagSet : $tagSet;
            }

            $tags = $normalizedTags !== [] ? $normalizedTags : null;
        }

        // Validate maxStalenessSeconds
        $maxStalenessSeconds = self::NO_MAX_STALENESS;
        if (isset($properties['maxStalenessSeconds'])) {
            $ms = $properties['maxStalenessSeconds'];
            if ($mode === self::PRIMARY) {
                throw new InvalidArgumentException(
                    'MongoDB\Driver\ReadPreference initialization requires "maxStalenessSeconds" field to not be present with "primary" mode',
                );
            }

            if ($ms > 2147483647) {
                throw new InvalidArgumentException(
                    'MongoDB\Driver\ReadPreference initialization requires "maxStalenessSeconds" integer field to be <= 2147483647',
                );
            }

            if ($ms < self::SMALLEST_MAX_STALENESS_SECONDS && $ms !== self::NO_MAX_STALENESS) {
                throw new InvalidArgumentException(
                    'MongoDB\Driver\ReadPreference initialization requires "maxStalenessSeconds" integer field to be >= 90',
                );
            }

            $maxStalenessSeconds = $ms;
        }

        // Validate hedge
        $hedge = null;
        if (isset($properties['hedge'])) {
            $hedgeVal = $properties['hedge'];

            if (! is_array($hedgeVal) && ! is_object($hedgeVal)) {
                throw new InvalidArgumentException(
                    'MongoDB\Driver\ReadPreference initialization requires "hedge" field to be an array or object',
                );
            }

            trigger_error(
                'MongoDB\Driver\ReadPreference::__set_state(): The "hedge" option is deprecated as of MongoDB 8.0 and will be removed in a future release',
                E_USER_DEPRECATED,
            );

            if ($hedgeVal instanceof PackedArray) {
                throw UnexpectedValueException::documentRequiredAsRoot();
            }

            if ($mode === self::PRIMARY) {
                throw new InvalidArgumentException(
                    'MongoDB\Driver\ReadPreference initialization requires "hedge" field to not be present with "primary" mode',
                );
            }

            $hedgeObj = (object) $hedgeVal;
            $hedge    = ((array) $hedgeObj) !== [] ? $hedgeObj : null;
        }

        $obj = new static($mode);
        $obj->applyState($mode, $tags, $maxStalenessSeconds, $hedge);

        return $obj;
    }

    public function __unserialize(array $data): void
    {
        $mode = $data['mode'] ?? null;

        if (! is_string($mode)) {
            throw new InvalidArgumentException(
                'MongoDB\Driver\ReadPreference initialization requires "mode" field to be string',
            );
        }

        if (! in_array($mode, self::MODE_MAP, true)) {
            throw new InvalidArgumentException(
                'MongoDB\Driver\ReadPreference initialization requires specific values for "mode" string field',
            );
        }

        $tags               = $data['tags'] ?? null;
        $maxStalenessSeconds = $data['maxStalenessSeconds'] ?? self::NO_MAX_STALENESS;
        $hedge              = $data['hedge'] ?? null;

        if ($hedge !== null) {
            trigger_error(
                'MongoDB\Driver\ReadPreference::__unserialize(): The "hedge" option is deprecated as of MongoDB 8.0 and will be removed in a future release',
                E_USER_DEPRECATED,
            );
        }

        $this->applyState($mode, $tags, $maxStalenessSeconds, $hedge);
    }

    public function bsonSerialize(): stdClass
    {
        $doc       = new stdClass();
        $doc->mode = $this->mode;

        if ($this->tags !== null) {
            $doc->tags = $this->tags;
        }

        if ($this->maxStalenessSeconds !== self::NO_MAX_STALENESS) {
            $doc->maxStalenessSeconds = $this->maxStalenessSeconds;
        }

        if ($this->hedge !== null) {
            $doc->hedge = $this->hedge;
        }

        return $doc;
    }

    public function getHedge(): ?object
    {
        trigger_error(
            'Method MongoDB\Driver\ReadPreference::getHedge() is deprecated',
            E_USER_DEPRECATED,
        );

        return $this->hedge;
    }

    public function getMaxStalenessSeconds(): int
    {
        return $this->maxStalenessSeconds;
    }

    public function getModeString(): string
    {
        return $this->mode;
    }

    public function getTagSets(): array
    {
        if ($this->tags === null) {
            return [];
        }

        return array_map(static fn (object $t) => (array) $t, $this->tags);
    }

    private function applyState(string $mode, ?array $tags, int $maxStalenessSeconds, ?object $hedge): void
    {
        $this->mode                = $mode;
        $this->tags                = $tags === [] ? null : $tags;
        $this->maxStalenessSeconds = $maxStalenessSeconds;
        $this->hedge               = $hedge;
    }
}
