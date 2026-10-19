<?php

/**
 * Secondary checks without ext-mongodb: error handling, type maps,
 * unacknowledged writes, and index management.
 *
 *     php -n tests/Smoke/edge-cases.php
 */

declare(strict_types=1);

require dirname(__DIR__, 2) . '/vendor/autoload.php';

use MongoDB\BSON\Document;
use MongoDB\BSON\UTCDateTime;
use MongoDB\Client;
use MongoDB\Collection;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\Command;
use MongoDB\Driver\Exception\BulkWriteException;
use MongoDB\Driver\Exception\CommandException;
use MongoDB\Driver\Query;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\WriteConcern;
use MongoDB\Operation\FindOneAndReplace;

function check(string $label, bool $condition): void
{
    printf("  %s %s\n", $condition ? '[ok]  ' : '[FAIL]', $label);

    if (! $condition) {
        exit(1);
    }
}

$uri = getenv('MONGODB_URI') ?: 'mongodb://127.0.0.1:27017/';

$client = new Client($uri);
$collection = $client->selectCollection('decoupling_stage_0', 'edge');
$collection->drop();

// --- Type maps ------------------------------------------------------------

$collection->insertOne(['_id' => 1, 'date' => new UTCDateTime(1700000000000), 'nested' => ['a' => 1]]);

$raw = $collection->findOne(['_id' => 1], ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]);
check('typeMap "array" at the root returns a PHP array', is_array($raw));
check('typeMap "array" recurses into sub-documents', is_array($raw['nested']));
check('UTCDateTime survives the round trip', $raw['date'] instanceof UTCDateTime);

$std = $collection->findOne(['_id' => 1], ['typeMap' => ['root' => 'stdClass', 'document' => 'stdClass', 'array' => 'array']]);
check('typeMap "stdClass" returns a stdClass', $std instanceof stdClass);

$bson = $collection->findOne(['_id' => 1], ['typeMap' => ['root' => 'bson', 'document' => 'bson', 'array' => 'bson']]);
check('typeMap "bson" returns a Document', $bson instanceof Document);
check('Document::toPHP() converts back to PHP', $bson->toPHP(['root' => 'array'])['_id'] === 1);

// --- Driver level API -----------------------------------------------------

$manager = $client->getManager();

$bulk = new BulkWrite();
$bulk->insert(['_id' => 2, 'name' => 'direct']);
$bulk->update(['_id' => 2], ['$set' => ['name' => 'direct-updated']]);
$result = $manager->executeBulkWrite('decoupling_stage_0.edge', $bulk);
check('Manager::executeBulkWrite works', $result->getInsertedCount() === 1 && $result->getMatchedCount() === 1);

$cursor = $manager->executeQuery('decoupling_stage_0.edge', new Query(['_id' => 2]));
check('Manager::executeQuery works', $cursor->toArray()[0]->name === 'direct-updated');

$cursor = $manager->executeQuery(
    'decoupling_stage_0.edge',
    new Query(['_id' => 2]),
    ['readPreference' => new ReadPreference(ReadPreference::PRIMARY)],
);
check('Manager::executeQuery accepts a read preference', count($cursor->toArray()) === 1);

$cursor = $manager->executeReadCommand('decoupling_stage_0', new Command(['count' => 'edge']));
check('Manager::executeReadCommand works', $cursor->toArray()[0]->n === 2);

$cursor = $manager->executeCommand('decoupling_stage_0', new Command(['ping' => 1]));
check('Manager::executeCommand works', (int) $cursor->toArray()[0]->ok === 1);

$cursor = $manager->executeWriteCommand(
    'decoupling_stage_0',
    new Command(['delete' => 'edge', 'deletes' => [['q' => (object) ['_id' => 2], 'limit' => 1]]]),
);
check('Manager::executeWriteCommand works', $cursor->toArray()[0]->n === 1);

// --- Errors ---------------------------------------------------------------

$collection->insertOne(['_id' => 3, 'unique' => 'value']);
$collection->createIndex(['unique' => 1], ['unique' => true]);

try {
    $collection->insertOne(['_id' => 4, 'unique' => 'value']);
    check('duplicate key raises an exception', false);
} catch (BulkWriteException $e) {
    check('duplicate key raises a BulkWriteException', $e->getCode() === 11000);
    check('BulkWriteException carries a write result', $e->getWriteResult()->getWriteErrors() !== []);
}

try {
    $collection->aggregate([['$match' => ['a' => ['$badOperator' => 1]]]]);
    check('invalid aggregation raises an exception', false);
} catch (CommandException $e) {
    check('invalid aggregation raises a CommandException', $e->getCode() > 0);
    check('CommandException carries the result document', $e->getResultDocument()->ok == 0);
}

// --- Write concern --------------------------------------------------------

$unacknowledged = new WriteConcern(0);
$result = $collection->insertOne(['_id' => 5, 'unique' => 'other'], ['writeConcern' => $unacknowledged]);
check('unacknowledged write is reported as such', ! $result->isAcknowledged());
check('unacknowledged write still reaches the server', $collection->countDocuments(['_id' => 5]) === 1);

// --- Indexes --------------------------------------------------------------

// The unique index is no longer needed; it would reject the documents below.
$collection->dropIndex('unique_1');

$indexNames = static fn (Collection $collection): array => array_map(
    static fn ($index) => $index['name'],
    iterator_to_array($collection->listIndexes()),
);

$indexes = iterator_to_array($collection->listIndexes());
check('listIndexes returns the _id index', $indexes[0]['name'] === '_id_');

$name = $collection->createIndex(['n' => 1]);
check('createIndex returns the generated name', $name === 'n_1');
check('listIndexes includes the new index', in_array('n_1', $indexNames($collection), true));

$collection->dropIndex('n_1');
check('dropIndex removed the index', ! in_array('n_1', $indexNames($collection), true));

// --- replaceOne and findOneAndReplace -------------------------------------

$collection->insertOne(['_id' => 6, 'name' => 'before', 'extra' => true]);
$result = $collection->replaceOne(['_id' => 6], ['name' => 'after']);
check('replaceOne matched 1 document', $result->getMatchedCount() === 1);
check('replaceOne replaced the document', $collection->findOne(['_id' => 6])->getArrayCopy() === ['_id' => 6, 'name' => 'after']);

$document = $collection->findOneAndReplace(
    ['_id' => 6],
    ['name' => 'last'],
    ['returnDocument' => FindOneAndReplace::RETURN_DOCUMENT_AFTER],
);
check('findOneAndReplace returns the replacement', $document['name'] === 'last');

// --- Sessions -------------------------------------------------------------

$session = $client->startSession();
check('startSession returns a session', $session->getLogicalSessionId() !== null);
check('a new session is not in a transaction', ! $session->isInTransaction());
$result = $collection->insertOne(['_id' => 7], ['session' => $session]);
check('operations accept a session', $result->getInsertedCount() === 1);
$session->endSession();

$collection->drop();

echo "\nAll edge cases passed without ext-mongodb.\n";
