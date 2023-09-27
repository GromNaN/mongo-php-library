<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

namespace MongoDB\Builder;

use MongoDB\BSON\Int64;
use MongoDB\BSON\PackedArray;
use MongoDB\Builder\Aggregation\AndAggregation;
use MongoDB\Builder\Aggregation\EqAggregation;
use MongoDB\Builder\Aggregation\FilterAggregation;
use MongoDB\Builder\Aggregation\GtAggregation;
use MongoDB\Builder\Aggregation\LtAggregation;
use MongoDB\Builder\Aggregation\NeAggregation;
use MongoDB\Builder\Expression\Expression;
use MongoDB\Builder\Expression\ResolvesToArray;
use MongoDB\Builder\Expression\ResolvesToBool;
use MongoDB\Builder\Expression\ResolvesToInt;
use MongoDB\Builder\Expression\ResolvesToString;
use MongoDB\Model\BSONArray;

final class Aggregation
{
    public static function and(mixed ...$expressions): AndAggregation
    {
        return new AndAggregation(...$expressions);
    }

    public static function eq(mixed $expression1, mixed $expression2): EqAggregation
    {
        return new EqAggregation($expression1, $expression2);
    }

    /**
     * @param BSONArray|PackedArray|ResolvesToArray|list<Expression|mixed> $input
     * @param ResolvesToString|string|null                                 $as
     * @param Int64|ResolvesToInt|int|null                                 $limit
     */
    public static function filter(
        PackedArray|ResolvesToArray|BSONArray|array $input,
        ResolvesToBool|bool $cond,
        ResolvesToString|null|string $as = null,
        Int64|ResolvesToInt|int|null $limit = null,
    ): FilterAggregation {
        return new FilterAggregation($input, $cond, $as, $limit);
    }

    public static function gt(mixed $expression1, mixed $expression2): GtAggregation
    {
        return new GtAggregation($expression1, $expression2);
    }

    public static function lt(mixed $expression1, mixed $expression2): LtAggregation
    {
        return new LtAggregation($expression1, $expression2);
    }

    public static function ne(mixed $expression1, mixed $expression2): NeAggregation
    {
        return new NeAggregation($expression1, $expression2);
    }

    /**
     * This class cannot be instantiated.
     */
    private function __construct()
    {
    }
}
