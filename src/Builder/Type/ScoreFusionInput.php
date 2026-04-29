<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Type;

use MongoDB\BSON\PackedArray;
use MongoDB\Builder\Pipeline;
use MongoDB\Exception\InvalidArgumentException;
use MongoDB\Model\BSONArray;
use stdClass;

use function count;
use function is_array;
use function sprintf;

/**
 * Type class for the $input argument of the $scoreFusion operator.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/scoreFusion/
 */
final class ScoreFusionInput implements TypeInterface
{
    public const PROPERTIES = ['pipelines' => 'pipelines', 'normalization' => 'normalization'];

    /** @var stdClass<BSONArray|PackedArray|Pipeline|array> $pipelines Map from name to input pipeline. Each pipeline must be operating on the same collection. */
    public readonly stdClass $pipelines;

    /** @var string $normalization Normalizes the score to the range 0 to 1 before combining the results. Value can be none, sigmoid or minMaxScaler. */
    public readonly string $normalization;

    /**
     * @param non-empty-array<string, BSONArray|PackedArray|Pipeline|array>|stdClass $pipelines     Map from name to input pipeline. Each pipeline must be operating on the same collection.
     * @param string                                                                 $normalization Normalizes the score to the range 0 to 1 before combining the results. Value can be none, sigmoid or minMaxScaler.
     */
    public function __construct(stdClass|array $pipelines, string $normalization)
    {
        if (count((array) $pipelines) < 1) {
            throw new InvalidArgumentException(sprintf('Expected at least %d entries for $pipelines, got %d.', 1, count((array) $pipelines)));
        }

        $this->pipelines = is_array($pipelines) ? (object) $pipelines : $pipelines;
        $this->normalization = $normalization;
    }
}
