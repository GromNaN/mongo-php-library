<?php

declare(strict_types=1);

namespace MongoDB\Tests\Builder\Stage;

use MongoDB\Builder\Stage\MatchStage;
use MongoDB\Builder\Stage\ProjectStage;
use MongoDB\Builder\Stage\GroupStage;
use PHPUnit\Framework\TestCase;

class LocalExecutionTest extends TestCase
{
    public function testMatchStageProcessLocally(): void
    {
        $docs = [
            ['author' => 'dave', 'score' => 80],
            ['author' => 'john', 'score' => 90],
        ];
        $stage = new MatchStage(['author' => 'dave']);
        $result = $stage->processLocally($docs);
        $this->assertSame([
            ['author' => 'dave', 'score' => 80],
        ], $result);
    }

    public function testProjectStageProcessLocally(): void
    {
        $docs = [
            ['author' => 'dave', 'score' => 80],
            ['author' => 'john', 'score' => 90],
        ];
        $stage = new ProjectStage(...['author' => 1]);
        $result = $stage->processLocally($docs);
        $this->assertSame([
            ['author' => 'dave'],
            ['author' => 'john'],
        ], $result);
    }

    public function testGroupStageProcessLocallySum(): void
    {
        $docs = [
            ['category' => 'A', 'value' => 10],
            ['category' => 'A', 'value' => 5],
            ['category' => 'B', 'value' => 7],
        ];
        $stage = new GroupStage('category', ...['total' => ['$sum' => 'value']]);
        $result = $stage->processLocally($docs);
        $this->assertSame([
            ['_id' => 'A', 'total' => 15],
            ['_id' => 'B', 'total' => 7],
        ], $result);
    }
}
