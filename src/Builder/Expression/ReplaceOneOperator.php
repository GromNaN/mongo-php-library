<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Expression;

use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\OperatorInterface;

/**
 * Replaces the first instance of a matched string in a given input.
 *
 * New in MongoDB 4.4
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceOne/
 * @internal
 */
final class ReplaceOneOperator implements ResolvesToString, OperatorInterface
{
    public const ENCODE = Encode::Object;
    public const NAME = '$replaceOne';
    public const PROPERTIES = ['input' => 'input', 'find' => 'find', 'replacement' => 'replacement'];

    /** @var ResolvesToNull|ResolvesToString|string|null $input The string on which you wish to apply the find. Can be any valid expression that resolves to a string or a null. If input refers to a field that is missing, $replaceAll returns null. */
    public readonly ResolvesToNull|ResolvesToString|string|null $input;

    /** @var ResolvesToNull|ResolvesToString|string|null $find The string to search for within the given input. Can be any valid expression that resolves to a string or a null. If find refers to a field that is missing, $replaceAll returns null. */
    public readonly ResolvesToNull|ResolvesToString|string|null $find;

    /** @var ResolvesToNull|ResolvesToString|string|null $replacement The string to use to replace all matched instances of find in input. Can be any valid expression that resolves to a string or a null. */
    public readonly ResolvesToNull|ResolvesToString|string|null $replacement;

    /**
     * @param ResolvesToNull|ResolvesToString|string|null $input The string on which you wish to apply the find. Can be any valid expression that resolves to a string or a null. If input refers to a field that is missing, $replaceAll returns null.
     * @param ResolvesToNull|ResolvesToString|string|null $find The string to search for within the given input. Can be any valid expression that resolves to a string or a null. If find refers to a field that is missing, $replaceAll returns null.
     * @param ResolvesToNull|ResolvesToString|string|null $replacement The string to use to replace all matched instances of find in input. Can be any valid expression that resolves to a string or a null.
     */
    public function __construct(
        ResolvesToNull|ResolvesToString|string|null $input,
        ResolvesToNull|ResolvesToString|string|null $find,
        ResolvesToNull|ResolvesToString|string|null $replacement,
    ) {
        $this->input = $input;
        $this->find = $find;
        $this->replacement = $replacement;
    }
}
