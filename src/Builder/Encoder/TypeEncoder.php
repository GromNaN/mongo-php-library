<?php

declare(strict_types=1);

namespace MongoDB\Builder\Encoder;

use MongoDB\Builder\Type\Optional;
use MongoDB\Builder\Type\TypeInterface;
use MongoDB\Codec\EncodeIfSupported;
use MongoDB\Codec\Encoder;
use MongoDB\Exception\UnsupportedValueException;
use stdClass;

/**
 * @template-implements Encoder<stdClass, TypeInterface>
 * @internal
 */
final class TypeEncoder implements Encoder
{
    /** @template-use EncodeIfSupported<stdClass, TypeInterface> */
    use EncodeIfSupported;
    use RecursiveEncode;

    public function canEncode(mixed $value): bool
    {
        return $value instanceof TypeInterface;
    }

    public function encode(mixed $value): stdClass
    {
        if (! $this->canEncode($value)) {
            throw UnsupportedValueException::invalidEncodableValue($value);
        }

        $result = new stdClass();
        foreach ($value::PROPERTIES as $prop => $name) {
            $val = $value->$prop;

            // Skip optional arguments. If they have a default value, it is resolved by the server.
            if ($val === Optional::Undefined) {
                continue;
            }

            // The name is null for arguments with "mergeObject: true" in the YAML file,
            // the value properties are merged into the parent object.
            if ($name === null) {
                $val = $this->recursiveEncode($val);
                foreach ($val as $k => $v) {
                    $result->{$k} = $v;
                }
            } else {
                $result->{$name} = $this->recursiveEncode($val);
            }
        }

        return $result;
    }
}
