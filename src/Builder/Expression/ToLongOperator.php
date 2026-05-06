<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Expression;

use DateTimeInterface;
use MongoDB\BSON\Type;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\ExpressionInterface;
use MongoDB\Builder\Type\OperatorInterface;
use stdClass;

/**
 * Converts value to a long.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/toLong/
 * @internal
 * @psalm-type ToLongOperatorShape = DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null|ToLongOperator
 */
final class ToLongOperator implements ResolvesToLong, OperatorInterface
{
    public const ENCODE = Encode::Single;
    public const NAME = '$toLong';
    public const PROPERTIES = ['expression' => 'expression'];

    /** @var DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null $expression */
    public readonly DateTimeInterface|Type|ExpressionInterface|stdClass|array|bool|float|int|string|null $expression;

    /** @param DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null $expression */
    public function __construct(
        DateTimeInterface|Type|ExpressionInterface|stdClass|array|bool|float|int|string|null $expression,
    ) {
        $this->expression = $expression;
    }
}
