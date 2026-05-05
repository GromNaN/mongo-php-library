<?php

declare(strict_types=1);

namespace MongoDB\CodeGenerator;

use MongoDB\BSON\Javascript;
use MongoDB\Builder\Type\Encode;
use MongoDB\Builder\Type\OperatorInterface;
use MongoDB\Builder\Type\Optional;
use MongoDB\Builder\Type\QueryObject;
use MongoDB\Builder\Type\TypeInterface;
use MongoDB\CodeGenerator\Definition\ArgumentDefinition;
use MongoDB\CodeGenerator\Definition\GeneratorDefinition;
use MongoDB\CodeGenerator\Definition\OperatorDefinition;
use MongoDB\CodeGenerator\Definition\VariadicType;
use MongoDB\Exception\InvalidArgumentException;
use Nette\PhpGenerator\Literal;
use Nette\PhpGenerator\Method;
use Nette\PhpGenerator\Parameter;
use Nette\PhpGenerator\PhpNamespace;
use Nette\PhpGenerator\Property;
use RuntimeException;
use stdClass;
use Throwable;

use function array_filter;
use function array_map;
use function array_values;
use function assert;
use function count;
use function explode;
use function implode;
use function interface_exists;
use function ltrim;
use function max;
use function rtrim;
use function sprintf;
use function str_repeat;
use function str_starts_with;
use function strlen;
use function substr;

/**
 * Generates a value object class for stages and operators.
 */
class OperatorClassGenerator extends OperatorGenerator
{
    public function generate(GeneratorDefinition $definition): void
    {
        foreach ($this->getOperators($definition) as $operator) {
            try {
                $this->writeFile($this->createClass($definition, $operator));
                foreach ($operator->arguments as $argument) {
                    if ($argument->arguments) {
                        $this->writeFile($this->createArgumentTypeClass($definition, $operator, $argument));
                    }
                }
            } catch (Throwable $e) {
                throw new RuntimeException(sprintf('Failed to generate class for operator "%s"', $operator->name), 0, $e);
            }
        }
    }

    public function createClass(GeneratorDefinition $definition, OperatorDefinition $operator): PhpNamespace
    {
        $namespace = new PhpNamespace($definition->namespace);

        $interfaces = $this->getInterfaces($operator);
        foreach ($interfaces as $interface) {
            $namespace->addUse($interface);
        }

        $class = $namespace->addClass($this->getOperatorClassName($definition, $operator));
        $class->setFinal();
        $class->setImplements($interfaces);
        $namespace->addUse(OperatorInterface::class);
        $class->addImplement(OperatorInterface::class);

        // Expose operator metadata as constants
        // @todo move to encoder class
        $class->addComment($operator->description);
        $class->addComment('@see ' . $operator->link);
        $class->addComment('@internal');
        $namespace->addUse(Encode::class);
        $class->addConstant('ENCODE', new Literal('Encode::' . $operator->encode->name));
        $class->addConstant('NAME', $operator->wrapObject ? $operator->name : null);

        $encodeNames = [];
        $constructor = $class->addMethod('__construct');
        foreach ($operator->arguments as $argument) {
            $encodeNames[$argument->propertyName] = $argument->mergeObject ? null : $argument->name;

            $type = $this->getAcceptedTypes($argument);

            // If the argument has sub-fields, inject the generated type class into the union
            $typeClassName = $this->getArgumentTypeClassName($operator, $argument, $definition->namespace);
            if ($typeClassName !== null) {
                $namespace->addUse('\\' . $typeClassName);
                $type->use[] = '\\' . $typeClassName;
                [, $typeShortName] = $this->splitNamespaceAndClassName($typeClassName);

                $shapeTypeName = $typeShortName . 'Shape';
                $class->addComment('@psalm-import-type ' . $shapeTypeName . ' from ' . $typeShortName);

                if ($argument->optional) {
                    // Insert the type class right after Optional in native type
                    $optionalNative = '\\' . Optional::class . '|';
                    if (str_starts_with($type->native, $optionalNative)) {
                        $type->native = $optionalNative . '\\' . $typeClassName . '|' . substr($type->native, strlen($optionalNative));
                    }

                    // @var/@param: Optional + shape (shape already contains TypeClass and BSON types)
                    $type->doc = 'Optional|' . $shapeTypeName;
                } else {
                    $type->native = '\\' . $typeClassName . '|' . $type->native;
                    // @var/@param: just the shape (shape already contains TypeClass and BSON types)
                    $type->doc = $shapeTypeName;
                }
            }

            foreach ($type->use as $use) {
                $namespace->addUse($use);
            }

            $property = $class->addProperty($argument->propertyName);
            $property->setReadOnly();
            $constructorParam = $constructor->addParameter($argument->propertyName);
            $constructorParam->setType($type->native);

            if ($argument->variadic) {
                $constructor->setVariadic();
                $constructor->addComment('@param ' . $type->doc . ' ...$' . $argument->propertyName . rtrim(' ' . $argument->description));

                if ($argument->variadicMin > 0) {
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    if (\count(\${$argument->propertyName}) < {$argument->variadicMin}) {
                        throw new InvalidArgumentException(\sprintf('Expected at least %d values for \${$argument->propertyName}, got %d.', {$argument->variadicMin}, \count(\${$argument->propertyName})));
                    }

                    PHP);
                }

                if ($argument->variadic === VariadicType::Array) {
                    $property->setType('array');
                    $property->addComment('@var list<' . $type->doc . '> $' . $argument->propertyName . rtrim(' ' . $argument->description));
                    // Warn that named arguments are not supported
                    // @see https://psalm.dev/docs/running_psalm/issues/NamedArgumentNotAllowed/
                    $constructor->addComment('@no-named-arguments');
                    $namespace->addUseFunction('array_is_list');
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    if (! array_is_list(\${$argument->propertyName})) {
                        throw new InvalidArgumentException('Expected \${$argument->propertyName} arguments to be a list (array), named arguments are not supported');
                    }

                    PHP);
                } elseif ($argument->variadic === VariadicType::Object) {
                    $namespace->addUse(stdClass::class);
                    $property->setType(stdClass::class);
                    $property->addComment('@var stdClass<' . $type->doc . '> $' . $argument->propertyName . rtrim(' ' . $argument->description));
                    $namespace->addUseFunction('is_string');
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    foreach(\${$argument->propertyName} as \$key => \$value) {
                        if (! is_string(\$key)) {
                            throw new InvalidArgumentException('Expected \${$argument->propertyName} arguments to be a map (object), named arguments (<name>:<value>) or array unpacking ...[\'<name>\' => <value>] must be used');
                        }
                    }

                    \${$argument->propertyName} = (object) \${$argument->propertyName};
                    PHP);
                }

                $constructor->addBody('$this->' . $argument->propertyName . ' = $' . $argument->propertyName . ';');
            } else {
                // Non-variadic arguments: add validation/coercion bodies first, then property/param setup and setter
                if ($type->dollarPrefixedString) {
                    $namespace->addUseFunction('is_string');
                    $namespace->addUseFunction('str_starts_with');
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    if (is_string(\${$argument->propertyName}) && ! str_starts_with(\${$argument->propertyName}, '$')) {
                        throw new InvalidArgumentException('Argument \${$argument->propertyName} can be an expression, field paths and variable names must be prefixed by "$" or "$$".');
                    }

                    PHP);
                }

                // List type must be validated with array_is_list()
                if ($type->list) {
                    $namespace->addUseFunction('is_array');
                    $namespace->addUseFunction('array_is_list');
                    $namespace->addUse(InvalidArgumentException::class);
                    $constructor->addBody(<<<PHP
                    if (is_array(\${$argument->propertyName}) && ! array_is_list(\${$argument->propertyName})) {
                        throw new InvalidArgumentException('Expected \${$argument->propertyName} argument to be a list, got an associative array.');
                    }

                    PHP);
                }

                if ($type->query) {
                    $namespace->addUseFunction('is_array');
                    $namespace->addUse(QueryObject::class);
                    $constructor->addBody(<<<PHP
                    if (is_array(\${$argument->propertyName})) {
                        \${$argument->propertyName} = QueryObject::create(\${$argument->propertyName});
                    }

                    PHP);
                }

                if ($type->javascript) {
                    $namespace->addUseFunction('is_string');
                    $namespace->addUse(Javascript::class);
                    $constructor->addBody(<<<PHP
                    if (is_string(\${$argument->propertyName})) {
                        \${$argument->propertyName} = new Javascript(\${$argument->propertyName});
                    }

                    PHP);
                }

                if ($typeClassName !== null) {
                    $namespace->addUseFunction('is_array');
                    $namespace->addUse(stdClass::class);
                    $propName = $argument->propertyName;
                    $constructor->addBody(<<<PHP
                    if (is_array(\${$propName}) || \${$propName} instanceof stdClass) {
                        \${$propName} = new {$typeShortName}(...(array) \${$propName});
                    }

                    PHP);
                }

                // buildPropertyAndParam handles types/docs/defaults and adds the setter body last
                $info = $this->buildPropertyAndParam($namespace, $property, $constructorParam, $constructor, $argument, $type);
                $constructor->addComment('@param ' . $info['typeDoc'] . ' $' . $info['name'] . rtrim(' ' . $info['desc']));
            }
        }

        if ($encodeNames !== []) {
            $class->addConstant('PROPERTIES', $encodeNames);
        }

        return $namespace;
    }

    /**
     * Generates a dedicated type class for an operator argument with sub-fields.
     * The class is placed in a Type sub-namespace of the operator's namespace (e.g. MongoDB\Builder\Stage\Type).
     */
    private function createArgumentTypeClass(GeneratorDefinition $definition, OperatorDefinition $operator, ArgumentDefinition $argument): PhpNamespace
    {
        $typeClassName = $this->getArgumentTypeClassName($operator, $argument, $definition->namespace);
        [, $typeShortClassName] = $this->splitNamespaceAndClassName($typeClassName);

        $namespace = new PhpNamespace($definition->namespace . '\\Type');
        $class = $namespace->addClass($typeShortClassName);
        $class->setFinal();
        $class->addImplement(TypeInterface::class);
        $class->addComment(sprintf(
            'Type class for the $%s argument of the %s operator.',
            $argument->propertyName,
            $operator->name,
        ));
        $class->addComment('');
        $class->addComment('@see ' . $operator->link);
        // Build the shape type: array/object shapes + TypeClass + other accepted types (e.g. Document|Serializable)
        // Use FQCNs for parent-level types so no extra imports are needed in the type class file.
        $parentType = $this->getAcceptedTypes($argument);
        $shortToFqcn = [];
        foreach ($parentType->use as $fqcn) {
            $shortName = $this->splitNamespaceAndClassName(ltrim($fqcn, '\\'))[1];
            $shortToFqcn[$shortName] = ltrim($fqcn, '\\');
        }

        $nonArrayParts = array_values(array_filter(
            explode('|', $parentType->doc),
            fn ($p) => $p !== 'array' && $p !== 'stdClass' && $p !== 'Optional',
        ));
        // Use FQCNs for non-array parts (e.g. \MongoDB\BSON\Document); TypeClass uses short name (same namespace)
        $nonArrayPartsFqcn = array_map(fn ($p) => isset($shortToFqcn[$p]) ? '\\' . $shortToFqcn[$p] : $p, $nonArrayParts);

        $shapeDoc = $this->computeArgumentShapeDoc($argument);
        $shapeDoc .= '|' . $typeShortClassName . '|' . implode('|', $nonArrayPartsFqcn);
        $class->addComment('@psalm-type ' . $typeShortClassName . 'Shape = ' . $shapeDoc);

        $encodeNames = [];
        $constructor = $class->addMethod('__construct');
        $paramInfos = [];

        foreach ($argument->arguments as $subArg) {
            $encodeNames[$subArg->propertyName] = $subArg->mergeObject ? null : $subArg->name;
            $subType = $this->getAcceptedTypes($subArg);
            foreach ($subType->use as $use) {
                $namespace->addUse($use);
            }

            $property = $class->addProperty($subArg->propertyName);
            $property->setReadOnly();
            $constructorParam = $constructor->addParameter($subArg->propertyName);

            $paramInfos[] = $this->buildPropertyAndParam($namespace, $property, $constructorParam, $constructor, $subArg, $subType);
        }

        $this->addAlignedParamComments($constructor, $paramInfos);

        $class->addConstant('PROPERTIES', $encodeNames);

        return $namespace;
    }

    /**
     * Sets up a property, constructor parameter, PHPDoc, default value, validation body, and property
     * setter. Handles variadic:object (map) and variadic:array (list) sub-argument patterns, as well as regular
     * non-variadic arguments.
     *
     * Returns the @param annotation info (typeDoc, name, desc) for the caller to add, allowing alignment.
     *
     * @return array{typeDoc: string, name: string, desc: string}
     */
    private function buildPropertyAndParam(
        PhpNamespace $namespace,
        Property $property,
        Parameter $param,
        Method $constructor,
        ArgumentDefinition $arg,
        stdClass $type,
    ): array {
        $propName = $arg->propertyName;
        $paramTypeDoc = '';
        $paramDesc = rtrim((string) $arg->description);

        if ($arg->variadic === VariadicType::Object) {
            $namespace->addUse(stdClass::class);
            $namespace->addUseFunction('is_array');

            // Compute map VALUE doc type without Optional (optionality is at the parameter level)
            $baseArg = clone $arg;
            $baseArg->optional = false;
            $baseType = $this->getAcceptedTypes($baseArg);
            $prefix = ($arg->variadicMin ?? 0) > 0 ? 'non-empty-array' : 'array';
            $mapDocType = $prefix . '<string, ' . $baseType->doc . '>';

            if ($arg->optional) {
                $namespace->addUse(Optional::class);
                $property->setType(Optional::class . '|' . stdClass::class);
                $property->addComment('@var Optional|stdClass $' . $propName . rtrim(' ' . $arg->description));
                $param->setType(Optional::class . '|' . stdClass::class . '|array');
                $param->setDefaultValue(new Literal('Optional::Undefined'));
                $paramTypeDoc = $mapDocType . '|Optional|stdClass';
            } else {
                $property->setType(stdClass::class);
                $property->addComment('@var stdClass<' . $baseType->doc . '> $' . $propName . rtrim(' ' . $arg->description));
                $param->setType(stdClass::class . '|array');
                $paramTypeDoc = $mapDocType . '|stdClass';

                if (($arg->variadicMin ?? 0) > 0) {
                    $min = $arg->variadicMin;
                    $namespace->addUse(InvalidArgumentException::class);
                    $namespace->addUseFunction('count');
                    $namespace->addUseFunction('sprintf');
                    $constructor->addBody(<<<PHP
                    if (count((array) \${$propName}) < {$min}) {
                        throw new InvalidArgumentException(sprintf('Expected at least %d entries for \${$propName}, got %d.', {$min}, count((array) \${$propName})));
                    }

                    PHP);
                }
            }

            $constructor->addBody('$this->' . $propName . ' = is_array($' . $propName . ') ? (object) $' . $propName . ' : $' . $propName . ';');
        } elseif ($arg->variadic === VariadicType::Array) {
            $namespace->addUseFunction('is_array');
            $namespace->addUseFunction('array_is_list');
            $namespace->addUse(InvalidArgumentException::class);

            // Compute list element doc type without Optional
            $baseArg = clone $arg;
            $baseArg->optional = false;
            $baseType = $this->getAcceptedTypes($baseArg);
            $prefix = ($arg->variadicMin ?? 0) > 0 ? 'non-empty-list' : 'list';
            $listDocType = $prefix . '<' . $baseType->doc . '>';

            if ($arg->optional) {
                $namespace->addUse(Optional::class);
                $property->setType(Optional::class . '|array');
                $property->addComment('@var Optional|' . $listDocType . ' $' . $propName . rtrim(' ' . $arg->description));
                $param->setType(Optional::class . '|array');
                $param->setDefaultValue(new Literal('Optional::Undefined'));
                $paramTypeDoc = 'Optional|' . $listDocType;
            } else {
                $property->setType('array');
                $property->addComment('@var ' . $listDocType . ' $' . $propName . rtrim(' ' . $arg->description));
                $param->setType('array');
                $paramTypeDoc = $listDocType;
            }

            $constructor->addBody(<<<PHP
            if (is_array(\${$propName}) && ! array_is_list(\${$propName})) {
                throw new InvalidArgumentException('Expected \${$propName} argument to be a list, got an associative array.');
            }

            PHP);
            $constructor->addBody('$this->' . $propName . ' = $' . $propName . ';');
        } else {
            // Non-variadic
            $property->setType($type->native);
            $property->addComment('@var ' . $type->doc . ' $' . $propName . rtrim(' ' . $arg->description));
            $param->setType($type->native);
            $paramTypeDoc = $type->doc;

            if ($arg->optional) {
                $namespace->addUse(Optional::class);
                $param->setDefaultValue(new Literal('Optional::Undefined'));
            } elseif ($arg->default !== null) {
                $param->setDefaultValue($arg->default);
            }

            $constructor->addBody('$this->' . $propName . ' = $' . $propName . ';');
        }

        return ['typeDoc' => $paramTypeDoc, 'name' => $propName, 'desc' => $paramDesc];
    }

    /**
     * Adds aligned @param comments to the constructor, padding type and name columns to consistent widths.
     * When there is only one param, no padding is needed (single-space separation).
     *
     * @param array<array{typeDoc: string, name: string, desc: string}> $paramInfos
     */
    private function addAlignedParamComments(Method $constructor, array $paramInfos): void
    {
        if (count($paramInfos) === 0) {
            return;
        }

        $maxTypeLen = max(array_map(fn ($i) => strlen($i['typeDoc']), $paramInfos));
        $maxNameLen = max(array_map(fn ($i) => strlen($i['name']), $paramInfos));

        foreach ($paramInfos as $info) {
            $typeSpaces = str_repeat(' ', $maxTypeLen - strlen($info['typeDoc']) + 1);
            $nameSpaces = str_repeat(' ', $maxNameLen - strlen($info['name']) + 1);
            $constructor->addComment(rtrim('@param ' . $info['typeDoc'] . $typeSpaces . '$' . $info['name'] . $nameSpaces . $info['desc']));
        }
    }

    /**
     * Operator classes interfaces are defined by their return type as a MongoDB expression.
     *
     * @return list<class-string>
     */
    private function getInterfaces(OperatorDefinition $definition): array
    {
        $interfaces = [];

        foreach ($definition->type as $type) {
            $interfaces[] = $interface = $this->getType($type)->returnType;
            assert(interface_exists($interface), sprintf('"%s" is not an interface.', $interface));
        }

        return $interfaces;
    }
}
