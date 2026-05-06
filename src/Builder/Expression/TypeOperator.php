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
 * Return the BSON data type of the field.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/type/
 * @internal
 * @psalm-type TypeOperatorShape = DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null|TypeOperator
 */
final class TypeOperator implements ResolvesToString, OperatorInterface
{
    public const ENCODE = Encode::Single;
    public const NAME = '$type';
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
