<?php

/**
 * THIS FILE IS AUTO-GENERATED. ANY CHANGES WILL BE LOST!
 */

declare(strict_types=1);

namespace MongoDB\Builder\Expression;

use MongoDB\BSON\Regex;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\OperatorInterface;

/**
 * Replaces all instances of a search string in an input string with a replacement string.
 * $replaceAll is both case-sensitive and diacritic-sensitive, and ignores any collation present on a collection.
 *
 * New in MongoDB 4.4
 *
 * @see https://www.mongodb.com/docs/manual/reference/operator/aggregation/replaceAll/
 * @internal
 * @psalm-type ReplaceAllOperatorShape = array{input: ResolvesToNull|ResolvesToString|string|null, find: Regex|ResolvesToNull|ResolvesToRegex|ResolvesToString|string|null, replacement: ResolvesToNull|ResolvesToString|string|null}|object{input: ResolvesToNull|ResolvesToString|string|null, find: Regex|ResolvesToNull|ResolvesToRegex|ResolvesToString|string|null, replacement: ResolvesToNull|ResolvesToString|string|null}&stdClass|ReplaceAllOperator
 */
final class ReplaceAllOperator implements ResolvesToString, OperatorInterface
{
    public const ENCODE = Encode::Object;
    public const NAME = '$replaceAll';
    public const PROPERTIES = ['input' => 'input', 'find' => 'find', 'replacement' => 'replacement'];

    /** @var ResolvesToNull|ResolvesToString|string|null $input The string on which you wish to apply the find. Can be any valid expression that resolves to a string or a null. If input refers to a field that is missing, $replaceAll returns null. */
    public readonly ResolvesToNull|ResolvesToString|string|null $input;

    /** @var Regex|ResolvesToNull|ResolvesToRegex|ResolvesToString|string|null $find The string to search for within the given input. Can be any valid expression that resolves to a string or a null. If find refers to a field that is missing, $replaceAll returns null. */
    public readonly Regex|ResolvesToNull|ResolvesToRegex|ResolvesToString|string|null $find;

    /** @var ResolvesToNull|ResolvesToString|string|null $replacement The string to use to replace all matched instances of find in input. Can be any valid expression that resolves to a string or a null. */
    public readonly ResolvesToNull|ResolvesToString|string|null $replacement;

    /**
     * @param ResolvesToNull|ResolvesToString|string|null $input The string on which you wish to apply the find. Can be any valid expression that resolves to a string or a null. If input refers to a field that is missing, $replaceAll returns null.
     * @param Regex|ResolvesToNull|ResolvesToRegex|ResolvesToString|string|null $find The string to search for within the given input. Can be any valid expression that resolves to a string or a null. If find refers to a field that is missing, $replaceAll returns null.
     * @param ResolvesToNull|ResolvesToString|string|null $replacement The string to use to replace all matched instances of find in input. Can be any valid expression that resolves to a string or a null.
     */
    public function __construct(
        ResolvesToNull|ResolvesToString|string|null $input,
        Regex|ResolvesToNull|ResolvesToRegex|ResolvesToString|string|null $find,
        ResolvesToNull|ResolvesToString|string|null $replacement,
    ) {
        $this->input = $input;
        $this->find = $find;
        $this->replacement = $replacement;
    }
}
