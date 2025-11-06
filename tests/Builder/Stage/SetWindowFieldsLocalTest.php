<?php

declare(strict_types=1);

namespace MongoDB\Tests\Builder\Stage;

use MongoDB\Builder\Accumulator\OutputWindowAccumulator;
use MongoDB\Builder\Accumulator\SumAccumulator;
use MongoDB\Builder\Accumulator\MaxAccumulator;
use MongoDB\Builder\Stage\SetWindowFieldsStage;
use MongoDB\Builder\Type\Sort;
use MongoDB\Builder\Pipeline;
use PHPUnit\Framework\TestCase;

class SetWindowFieldsLocalTest extends TestCase
{
    public function testUseDocumentsWindowToObtainCumulativeAndMaximumQuantityForEachYear(): void
    {
        $docs = [
            ['orderDate' => new \DateTime('2022-01-01'), 'quantity' => 5],
            ['orderDate' => new \DateTime('2022-02-01'), 'quantity' => 7],
            ['orderDate' => new \DateTime('2023-01-01'), 'quantity' => 3],
            ['orderDate' => new \DateTime('2023-02-01'), 'quantity' => 8],
        ];
        $pipeline = new Pipeline(
            new SetWindowFieldsStage(
                sortBy: ['orderDate' => Sort::Asc],
                output: [
                    'cumulativeQuantityForYear' => new OutputWindowAccumulator(
                        new SumAccumulator('$quantity'),
                        ['documents' => ['unbounded', 'current']]
                    ),
                    'maximumQuantityForYear' => new OutputWindowAccumulator(
                        new MaxAccumulator('$quantity'),
                        ['documents' => ['unbounded', 'unbounded']]
                    ),
                ],
                partitionBy: new class {
                    // Mimic YearExpression for partitioning
                    public function __toString() { return 'year'; }
                },
            )
        );
        $result = $pipeline->processLocally($docs);
        $expected = [
            ['orderDate' => new \DateTime('2022-01-01'), 'quantity' => 5, 'cumulativeQuantityForYear' => 5, 'maximumQuantityForYear' => 7],
            ['orderDate' => new \DateTime('2022-02-01'), 'quantity' => 7, 'cumulativeQuantityForYear' => 12, 'maximumQuantityForYear' => 7],
            ['orderDate' => new \DateTime('2023-01-01'), 'quantity' => 3, 'cumulativeQuantityForYear' => 3, 'maximumQuantityForYear' => 8],
            ['orderDate' => new \DateTime('2023-02-01'), 'quantity' => 8, 'cumulativeQuantityForYear' => 11, 'maximumQuantityForYear' => 8],
        ];
        foreach ($result as $i => $doc) {
            $this->assertEquals($expected[$i]['orderDate'], $doc['orderDate']);
            $this->assertEquals($expected[$i]['quantity'], $doc['quantity']);
            $this->assertEquals($expected[$i]['cumulativeQuantityForYear'], $doc['cumulativeQuantityForYear']);
            $this->assertEquals($expected[$i]['maximumQuantityForYear'], $doc['maximumQuantityForYear']);
        }
    }
}

