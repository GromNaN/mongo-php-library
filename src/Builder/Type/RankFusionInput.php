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
 * Type class for the $input argument of the $rankFusion operator.
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/rankFusion/
 * @psalm-type RankFusionInputShape = array{pipelines: non-empty-array<string, BSONArray|PackedArray|Pipeline|array>}|object{pipelines: non-empty-array<string, BSONArray|PackedArray|Pipeline|array>|stdClass}&stdClass|RankFusionInput|\MongoDB\BSON\Document|\MongoDB\BSON\Serializable
 */
final class RankFusionInput implements TypeInterface
{
    public const PROPERTIES = ['pipelines' => 'pipelines'];

    /** @var stdClass<BSONArray|PackedArray|Pipeline|array> $pipelines Map from name to ranked input pipeline. Each pipeline must operate on the same collection. */
    public readonly stdClass $pipelines;

    /** @param non-empty-array<string, BSONArray|PackedArray|Pipeline|array>|stdClass $pipelines Map from name to ranked input pipeline. Each pipeline must operate on the same collection. */
    public function __construct(stdClass|array $pipelines)
    {
        if (count((array) $pipelines) < 1) {
            throw new InvalidArgumentException(sprintf('Expected at least %d entries for $pipelines, got %d.', 1, count((array) $pipelines)));
        }

        $this->pipelines = is_array($pipelines) ? (object) $pipelines : $pipelines;
    }
}
