<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Query;

use MongoDB\BSON\Type;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\FieldQueryInterface;
use MongoDB\Builder\Type\OperatorInterface;
use MongoDB\Exception\InvalidArgumentException;
use stdClass;

use function array_is_list;

/**
 * Matches arrays that contain all elements specified in the query.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/query/all/
 */
class AllOperator implements FieldQueryInterface, OperatorInterface
{
    public const ENCODE = Encode::Single;

    /** @var list<Type|array|bool|float|int|non-empty-string|null|stdClass> $value */
    public readonly array $value;

    /**
     * @param Type|array|bool|float|int|non-empty-string|null|stdClass ...$value
     * @no-named-arguments
     */
    public function __construct(Type|stdClass|array|bool|float|int|null|string ...$value)
    {
        if (\count($value) < 1) {
            throw new \InvalidArgumentException(\sprintf('Expected at least %d values for $value, got %d.', 1, \count($value)));
        }

        if (! array_is_list($value)) {
            throw new InvalidArgumentException('Expected $value arguments to be a list (array), named arguments are not supported');
        }

        $this->value = $value;
    }

    public function getOperator(): string
    {
        return '$all';
    }
}
