<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Accumulator;

use DateTimeInterface;
use MongoDB\BSON\Type;
use MongoDB\Builder\Type\AccumulatorInterface;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\ExpressionInterface;
use MongoDB\Builder\Type\OperatorInterface;
use MongoDB\Builder\Type\WindowInterface;
use stdClass;

/**
 * Returns the maximum value that results from applying an expression to each document.
 * Changed in MongoDB 5.0: Available in the $setWindowFields stage.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/max/
 * @internal
 */
final class MaxAccumulator implements AccumulatorInterface, WindowInterface, OperatorInterface
{
    public const ENCODE = Encode::Single;
    public const NAME = '$max';
    public const PROPERTIES = ['expression' => 'expression'];

    /** @var DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|null|stdClass|string $expression */
    public readonly DateTimeInterface|Type|ExpressionInterface|stdClass|array|bool|float|int|null|string $expression;

    /**
     * @param DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|null|stdClass|string $expression
     */
    public function __construct(
        DateTimeInterface|Type|ExpressionInterface|stdClass|array|bool|float|int|null|string $expression,
    ) {
        $this->expression = $expression;
    }

    /**
     * Accumulate the maximum for the current group state using the provided document.
     *
     * @param array $state Reference to the group state
     * @param array $doc The current document
     */
    public function accumulate(array &$state, array $doc): void
    {
        $expr = $this->expression;
        $val = null;
        if (is_string($expr) && str_starts_with($expr, '$')) {
            $field = substr($expr, 1);
            $val = $doc[$field] ?? null;
        } elseif (is_numeric($expr)) {
            $val = $expr;
        }
        if (!isset($state['value']) || ($val !== null && $val > $state['value'])) {
            $state['value'] = $val;
        }
    }
}
