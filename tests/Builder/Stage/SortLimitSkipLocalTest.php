<?php

declare(strict_types=1);

namespace MongoDB\Tests\Builder\Stage;

use MongoDB\Builder\Stage\SortStage;
use MongoDB\Builder\Stage\LimitStage;
use MongoDB\Builder\Stage\SkipStage;
use MongoDB\Builder\Pipeline;
use PHPUnit\Framework\TestCase;

class SortLimitSkipLocalTest extends TestCase
{
    public function testSortStageProcessLocallyAscDesc(): void
    {
        $docs = [
            ['x' => 2, 'y' => 'b'],
            ['x' => 1, 'y' => 'a'],
            ['x' => 3, 'y' => 'c'],
        ];
        $pipelineAsc = new Pipeline(
            new SortStage(...['x' => 1])
        );
        $resultAsc = $pipelineAsc->processLocally($docs);
        $this->assertSame([
            ['x' => 1, 'y' => 'a'],
            ['x' => 2, 'y' => 'b'],
            ['x' => 3, 'y' => 'c'],
        ], $resultAsc);

        $pipelineDesc = new Pipeline(
            new SortStage(...['x' => -1])
        );
        $resultDesc = $pipelineDesc->processLocally($docs);
        $this->assertSame([
            ['x' => 3, 'y' => 'c'],
            ['x' => 2, 'y' => 'b'],
            ['x' => 1, 'y' => 'a'],
        ], $resultDesc);
    }

    public function testLimitStageProcessLocally(): void
    {
        $docs = [
            ['x' => 1], ['x' => 2], ['x' => 3], ['x' => 4],
        ];
        $pipeline = new Pipeline(
            new LimitStage(2)
        );
        $result = $pipeline->processLocally($docs);
        $this->assertSame([
            ['x' => 1], ['x' => 2],
        ], $result);
    }

    public function testSkipStageProcessLocally(): void
    {
        $docs = [
            ['x' => 1], ['x' => 2], ['x' => 3], ['x' => 4],
        ];
        $pipeline = new Pipeline(
            new SkipStage(2)
        );
        $result = $pipeline->processLocally($docs);
        $this->assertSame([
            ['x' => 3], ['x' => 4],
        ], $result);
    }

    public function testSortLimitSkipCombined(): void
    {
        $docs = [
            ['x' => 5], ['x' => 2], ['x' => 8], ['x' => 1],
        ];
        $pipeline = new Pipeline(
            new SortStage(...['x' => 1]),
            new LimitStage(2),
            new SkipStage(1)
        );
        $result = $pipeline->processLocally($docs);
        $this->assertSame([
            ['x' => 2],
        ], $result);
    }
}
