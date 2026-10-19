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

namespace MongoDB\Driver\Exception;

use InvalidArgumentException as PhpInvalidArgumentException;

use function get_debug_type;
use function sprintf;

class InvalidArgumentException extends PhpInvalidArgumentException implements Exception
{
    /**
     * @internal
     * 'Expected "sort" option to be array or object, int given'
     */
    public static function expectedDocumentOption(string $option, mixed $value): self
    {
        return new self(sprintf(
            'Expected "%s" option to be array or object, %s given',
            $option,
            get_debug_type($value),
        ));
    }

    /**
     * @internal
     * 'Expected "hint" option to be string, array, or object, int given'
     */
    public static function expectedHintOption(string $option, mixed $value): self
    {
        return new self(sprintf(
            'Expected "%s" option to be string, array, or object, %s given',
            $option,
            get_debug_type($value),
        ));
    }

    /**
     * @internal
     * 'MongoDB\Driver\WriteConcern initialization requires "w" field to be integer or string'
     */
    public static function initializationRequiresFieldType(string $class, string $field, string $type): self
    {
        return new self(sprintf('%s initialization requires "%s" field to be %s', $class, $field, $type));
    }

    /**
     * @internal
     * 'MongoDB\Driver\ReadConcern initialization requires "level" string field'
     */
    public static function initializationRequiresStringField(string $class, string $field): self
    {
        return new self(sprintf('%s initialization requires "%s" string field', $class, $field));
    }

    /**
     * @internal
     * 'Expected "serverApi" driver option to be MongoDB\Driver\ServerApi, string given'
     */
    public static function invalidDriverOptionType(string $option, mixed $value, string $expectedType): self
    {
        return new self(sprintf(
            'Expected "%s" driver option to be %s, %s given',
            $option,
            $expectedType,
            get_debug_type($value),
        ));
    }

    /**
     * @internal
     * 'Expected "readConcern" option to be MongoDB\Driver\ReadConcern, string given'
     */
    public static function invalidOptionType(string $option, mixed $value, string $expectedType): self
    {
        return new self(sprintf(
            'Expected "%s" option to be %s, %s given',
            $option,
            $expectedType,
            get_debug_type($value),
        ));
    }
}
