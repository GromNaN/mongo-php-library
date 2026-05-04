<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Query;

use DateTimeInterface;
use MongoDB\BSON\Type;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\FieldQueryInterface;
use MongoDB\Builder\Type\OperatorInterface;
use stdClass;

/**
 * Inverts the effect of a query expression and returns documents that do not match the query expression.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/query/not/
 * @internal
 */
final class NotOperator implements FieldQueryInterface, OperatorInterface
{
    public const ENCODE = Encode::Single;
    public const NAME = '$not';
    public const PROPERTIES = ['expression' => 'expression'];

    /** @var DateTimeInterface|FieldQueryInterface|Type|array|bool|float|int|stdClass|string|null $expression */
    public readonly DateTimeInterface|Type|FieldQueryInterface|stdClass|array|bool|float|int|string|null $expression;

    /** @param DateTimeInterface|FieldQueryInterface|Type|array|bool|float|int|stdClass|string|null $expression */
    public function __construct(
        DateTimeInterface|Type|FieldQueryInterface|stdClass|array|bool|float|int|string|null $expression,
    ) {
        $this->expression = $expression;
    }
}
