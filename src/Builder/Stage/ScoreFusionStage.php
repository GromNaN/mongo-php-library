<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Stage;

use MongoDB\BSON\Document;
use MongoDB\BSON\Serializable;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\OperatorInterface;
use MongoDB\Builder\Type\Optional;
use MongoDB\Builder\Type\ScoreFusionCombination;
use MongoDB\Builder\Type\ScoreFusionInput;
use MongoDB\Builder\Type\StageInterface;
use stdClass;

use function is_array;

/**
 * Combines multiple pipelines using relative score fusion to create hybrid search results.
 *
 * New in MongoDB 8.0
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/scoreFusion/
 * @internal
 * @psalm-import-type ScoreFusionInputShape from ScoreFusionInput
 * @psalm-import-type ScoreFusionCombinationShape from ScoreFusionCombination
 */
final class ScoreFusionStage implements StageInterface, OperatorInterface
{
    public const ENCODE = Encode::Object;
    public const NAME = '$scoreFusion';
    public const PROPERTIES = ['input' => 'input', 'scoreDetails' => 'scoreDetails', 'combination' => 'combination'];

    /** @var ScoreFusionInputShape $input An object that specifies the pipelines to combine with score fusion. */
    public readonly ScoreFusionInput|Document|Serializable|stdClass|array $input;

    /** @var bool $scoreDetails Set to true to include detailed scoring information. */
    public readonly bool $scoreDetails;

    /** @var Optional|ScoreFusionCombinationShape $combination An object that specifies how to combine the scores. */
    public readonly Optional|ScoreFusionCombination|Document|Serializable|stdClass|array $combination;

    /**
     * @param ScoreFusionInputShape $input An object that specifies the pipelines to combine with score fusion.
     * @param bool $scoreDetails Set to true to include detailed scoring information.
     * @param Optional|ScoreFusionCombinationShape $combination An object that specifies how to combine the scores.
     */
    public function __construct(
        ScoreFusionInput|Document|Serializable|stdClass|array $input,
        bool $scoreDetails = false,
        Optional|ScoreFusionCombination|Document|Serializable|stdClass|array $combination = Optional::Undefined,
    ) {
        if (is_array($input) || $input instanceof stdClass) {
            $input = new ScoreFusionInput(...(array) $input);
        }

        $this->input = $input;
        $this->scoreDetails = $scoreDetails;
        if (is_array($combination) || $combination instanceof stdClass) {
            $combination = new ScoreFusionCombination(...(array) $combination);
        }

        $this->combination = $combination;
    }
}
