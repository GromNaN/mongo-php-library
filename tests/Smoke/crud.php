<?php

/**
 * End-to-end CRUD check without ext-mongodb.
 *
 * Every call below goes through the mongodb/mongodb library, using the bundled
 * PHP implementation of MongoDB\BSON and MongoDB\Driver. Run it with
 * ext-mongodb disabled to prove the library works without the extension:
 *
 *     php -n tests/Smoke/crud.php
 *
 * The script targets the MongoDB server given by MONGODB_URI, defaulting to
 * mongodb://127.0.0.1:27017/.
 */

declare(strict_types=1);

require dirname(__DIR__, 2) . '/vendor/autoload.php';

use MongoDB\BSON\ObjectId;
use MongoDB\Client;
use MongoDB\Driver\Manager;
use MongoDB\Operation\FindOneAndUpdate;

function check(string $label, bool $condition): void
{
    printf("  %s %s\n", $condition ? '[ok]  ' : '[FAIL]', $label);

    if (! $condition) {
        exit(1);
    }
}

$uri = getenv('MONGODB_URI') ?: 'mongodb://127.0.0.1:27017/';

printf("extension loaded: %s\n", extension_loaded('mongodb') ? 'yes' : 'no');
printf("driver class:     %s\n", (new ReflectionClass(Manager::class))->getFileName());
printf("connecting to:    %s\n\n", $uri);

$client = new Client($uri);
$collection = $client->selectCollection('decoupling_stage_0', 'crud');

$collection->drop();

// --- Insert ---------------------------------------------------------------

$result = $collection->insertOne(['_id' => 1, 'name' => 'alpha', 'n' => 1, 'tags' => ['x', 'y']]);
check('insertOne reports 1 inserted document', $result->getInsertedCount() === 1);
check('insertOne returns the given id', $result->getInsertedId() === 1);

$result = $collection->insertMany([
    ['_id' => 2, 'name' => 'beta', 'n' => 2, 'tags' => ['y']],
    ['_id' => 3, 'name' => 'gamma', 'n' => 3, 'tags' => []],
    ['_id' => 4, 'name' => 'delta', 'n' => 4, 'tags' => ['z']],
]);
check('insertMany reports 3 inserted documents', $result->getInsertedCount() === 3);

$generatedId = $collection->insertOne(['name' => 'epsilon', 'n' => 5])->getInsertedId();
check('insertOne generates an ObjectId', $generatedId instanceof ObjectId);

// --- Read -----------------------------------------------------------------

$document = $collection->findOne(['_id' => 1]);
check('findOne returns a document', $document !== null);
check('findOne decoded the document fields', $document['name'] === 'alpha' && $document['n'] === 1);
check('findOne decoded a nested array', $document['tags']->getArrayCopy() === ['x', 'y']);

$count = $collection->countDocuments(['n' => ['$gte' => 2]]);
check('countDocuments matches 4 documents', $count === 4);

// A batchSize smaller than the result set forces the cursor to issue getMore.
$cursor = $collection->find(['n' => ['$gte' => 1]], ['sort' => ['n' => 1], 'batchSize' => 2]);
$names = [];
foreach ($cursor as $doc) {
    $names[] = $doc['name'];
}

check('cursor iterated over every batch', $names === ['alpha', 'beta', 'gamma', 'delta', 'epsilon']);
check('cursor toArray returns the same documents', count($collection->find([], ['sort' => ['n' => 1]])->toArray()) === 5);

$projected = $collection->findOne(['_id' => 1], ['projection' => ['_id' => 0, 'name' => 1]]);
check('projection is applied', count($projected) === 1 && $projected['name'] === 'alpha');

$names = $collection->distinct('name', ['n' => ['$lte' => 3]]);
sort($names);
check('distinct returns the matching values', $names === ['alpha', 'beta', 'gamma']);

// --- Aggregate ------------------------------------------------------------

$aggregated = $collection->aggregate([
    ['$match' => ['n' => ['$gte' => 2]]],
    ['$group' => ['_id' => null, 'total' => ['$sum' => '$n']]],
])->toArray();

check('aggregate returns one bucket', count($aggregated) === 1);
check('aggregate computed the sum', $aggregated[0]['total'] === 14);

// --- Update ---------------------------------------------------------------

$result = $collection->updateOne(['_id' => 1], ['$set' => ['name' => 'ALPHA']]);
check('updateOne matched 1 document', $result->getMatchedCount() === 1);
check('updateOne modified 1 document', $result->getModifiedCount() === 1);

$result = $collection->updateMany(['n' => ['$gte' => 3]], ['$inc' => ['n' => 10]]);
check('updateMany matched 3 documents', $result->getMatchedCount() === 3);
check('updateMany modified 3 documents', $result->getModifiedCount() === 3);

$result = $collection->updateOne(['_id' => 99], ['$set' => ['name' => 'upserted']], ['upsert' => true]);
check('updateOne with upsert reports 1 upserted document', $result->getUpsertedCount() === 1);
check('updateOne with upsert reports the new id', $result->getUpsertedId() === 99);

$result = $collection->findOneAndUpdate(['_id' => 2], ['$set' => ['name' => 'BETA']], ['returnDocument' => FindOneAndUpdate::RETURN_DOCUMENT_AFTER]);
check('findOneAndUpdate returns the updated document', $result['name'] === 'BETA');

// --- Bulk write -----------------------------------------------------------

$result = $collection->bulkWrite([
    ['insertOne' => [['_id' => 100, 'name' => 'bulk-insert', 'n' => 100]]],
    ['updateOne' => [['_id' => 100], ['$set' => ['name' => 'bulk-update']]]],
    ['deleteOne' => [['_id' => 100]]],
]);

check('bulkWrite inserted 1 document', $result->getInsertedCount() === 1);
check('bulkWrite matched 1 document', $result->getMatchedCount() === 1);
check('bulkWrite deleted 1 document', $result->getDeletedCount() === 1);

// --- Delete ---------------------------------------------------------------

$result = $collection->deleteOne(['_id' => 99]);
check('deleteOne deleted 1 document', $result->getDeletedCount() === 1);

$result = $collection->deleteMany(['n' => ['$gte' => 13]]);
check('deleteMany deleted the matching documents', $result->getDeletedCount() === 3);

// --- Commands and databases ----------------------------------------------

$reply = $client->selectDatabase('admin')->command(['ping' => 1])->toArray();
check('command returns ok:1', (int) $reply[0]['ok'] === 1);

$collections = iterator_to_array($client->selectDatabase('decoupling_stage_0')->listCollectionNames());
check('listCollectionNames returns the collection', in_array('crud', $collections, true));

$stats = $client->selectDatabase('decoupling_stage_0')->command(['dbStats' => 1])->toArray();
check('dbStats reports the database', $stats[0]['db'] === 'decoupling_stage_0');

$server = $client->getManager()->selectServer();
printf("\nserver: %s:%d (%s)\n", $server->getHost(), $server->getPort(), $server->getType());
printf("maxWireVersion: %d\n", $server->getInfo()['maxWireVersion']);

$collection->drop();

echo "\nAll checks passed without ext-mongodb.\n";
