<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Type;

use DateTimeInterface;
use MongoDB\BSON\Decimal128;
use MongoDB\BSON\Int64;
use MongoDB\BSON\Type;
use stdClass;

use function is_array;

/**
 * Type class for the $combination argument of the $scoreFusion operator.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/scoreFusion/
 * @psalm-type ScoreFusionCombinationShape = array{weights?: array<string, Decimal128|Int64|float|int>, method?: string, expression?: DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null}|object{weights?: array<string, Decimal128|Int64|float|int>|stdClass, method?: string, expression?: DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null}&stdClass|ScoreFusionCombination|\MongoDB\BSON\Document|\MongoDB\BSON\Serializable
 */
final class ScoreFusionCombination implements TypeInterface
{
    public const PROPERTIES = ['weights' => 'weights', 'method' => 'method', 'expression' => 'expression'];

    /** @var Optional|stdClass $weights Map from pipeline name to weight (non-negative number). If unspecified, default weight is 1 for each pipeline. */
    public readonly Optional|stdClass $weights;

    /** @var Optional|string $method Specifies method for combining scores. Value can be avg or expression. Default is avg. */
    public readonly Optional|string $method;

    /** @var Optional|DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null $expression Custom expression used when combination.method is set to expression. */
    public readonly Optional|DateTimeInterface|Type|ExpressionInterface|stdClass|array|bool|float|int|string|null $expression;

    /**
     * @param array<string, Decimal128|Int64|float|int>|Optional|stdClass                                   $weights    Map from pipeline name to weight (non-negative number). If unspecified, default weight is 1 for each pipeline.
     * @param Optional|string                                                                               $method     Specifies method for combining scores. Value can be avg or expression. Default is avg.
     * @param Optional|DateTimeInterface|ExpressionInterface|Type|array|bool|float|int|stdClass|string|null $expression Custom expression used when combination.method is set to expression.
     */
    public function __construct(
        Optional|stdClass|array $weights = Optional::Undefined,
        Optional|string $method = Optional::Undefined,
        Optional|DateTimeInterface|Type|ExpressionInterface|stdClass|array|bool|float|int|string|null $expression = Optional::Undefined,
    ) {
        $this->weights = is_array($weights) ? (object) $weights : $weights;
        $this->method = $method;
        $this->expression = $expression;
    }
}
