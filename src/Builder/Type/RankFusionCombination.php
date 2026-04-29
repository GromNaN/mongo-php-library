<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Type;

use MongoDB\BSON\Decimal128;
use MongoDB\BSON\Int64;
use stdClass;

use function is_array;

/**
 * Type class for the $combination argument of the $rankFusion operator.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/rankFusion/
 */
final class RankFusionCombination implements TypeInterface
{
    public const PROPERTIES = ['weights' => 'weights'];

    /** @var Optional|stdClass $weights Map from pipeline name to weight (non-negative number). If unspecified, default weight is 1 for each pipeline. */
    public readonly Optional|stdClass $weights;

    /** @param array<string, Decimal128|Int64|float|int>|Optional|stdClass $weights Map from pipeline name to weight (non-negative number). If unspecified, default weight is 1 for each pipeline. */
    public function __construct(Optional|stdClass|array $weights = Optional::Undefined)
    {
        $this->weights = is_array($weights) ? (object) $weights : $weights;
    }
}
