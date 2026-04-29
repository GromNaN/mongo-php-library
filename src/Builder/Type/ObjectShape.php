<?php

declare(strict_types=1);

namespace MongoDB\Builder\Type;

use MongoDB\Exception\InvalidArgumentException;
use stdClass;

use function array_diff;
use function array_keys;
use function get_object_vars;
use function implode;
use function is_array;
use function sprintf;

/**
 * Validates that an array or stdClass value matches an expected object shape.
 * BSON types (Document, Serializable) are not validated and pass through as-is.
 *
 * @internal
 */
final class ObjectShape
{
    /**
     * @param list<string> $requiredFields
     * @param list<string> $optionalFields
     */
    public static function validate(
        mixed $value,
        string $argName,
        array $requiredFields,
        array $optionalFields = [],
    ): void {
        if (! is_array($value) && ! $value instanceof stdClass) {
            return;
        }

        $keys = is_array($value) ? array_keys($value) : array_keys(get_object_vars($value));
        $allowedFields = [...$requiredFields, ...$optionalFields];

        $unknownFields = array_diff($keys, $allowedFields);
        if ($unknownFields !== []) {
            throw new InvalidArgumentException(sprintf(
                'Unknown field(s) "%s" for argument $%s. Accepted fields are: "%s".',
                implode('", "', $unknownFields),
                $argName,
                implode('", "', $allowedFields),
            ));
        }

        $missingFields = array_diff($requiredFields, $keys);
        if ($missingFields !== []) {
            throw new InvalidArgumentException(sprintf(
                'Missing required field(s) "%s" for argument $%s.',
                implode('", "', $missingFields),
                $argName,
            ));
        }
    }
}
