<?php

namespace MongoDB\Http;

use MongoDB\Http\Endpoint\Aggregate;
use MongoDB\Http\Endpoint\DeleteMany;
use MongoDB\Http\Endpoint\DeleteOne;
use MongoDB\Http\Endpoint\Find;
use MongoDB\Http\Endpoint\FindOne;
use MongoDB\Http\Endpoint\InsertMany;
use MongoDB\Http\Endpoint\InsertOne;
use MongoDB\Http\Endpoint\UpdateMany;
use MongoDB\Http\Endpoint\UpdateOne;
use Traversable;

class Collection
{
    public function __construct(
        public readonly Database $database,
        public readonly string $name
    ) {
    }

    public function findOne($filter = [], array $options = [])
    {
        $endpoint = new FindOne($this->database->name, $this->name, $filter, $options);

        return $endpoint->execute($this->database->client);
    }

    public function find($filter = [], array $options = [])
    {
        $endpoint = new Find($this->database->name, $this->name, $filter, $options);

        return $endpoint->execute($this->database->client);
    }

    public function insertOne($document, array $options = [])
    {
        $endpoint = new InsertOne($this->database->name, $this->name, $document, $options);

        return $endpoint->execute($this->database->client);
    }

    public function insertMany($documents, array $options = [])
    {
        $endpoint = new InsertMany($this->database->name, $this->name, $documents, $options);

        return $endpoint->execute($this->database->client);
    }

    public function updateOne($filter, $update, array $options = [])
    {
        $endpoint = new UpdateOne($this->database->name, $this->name, $filter, $update, $options);

        return $endpoint->execute($this->database->client);
    }

    public function updateMany($filter, $update, array $options = [])
    {
        $endpoint = new UpdateMany($this->database->name, $this->name, $filter, $update, $options);

        return $endpoint->execute($this->database->client);
    }

    public function deleteOne($filter, array $options = [])
    {
        $endpoint = new DeleteOne($this->database->name, $this->name, $filter, $options);

        return $endpoint->execute($this->database->client);
    }

    public function deleteMany($filter, array $options = [])
    {
        $endpoint = new DeleteMany($this->database->name, $this->name, $filter, $options);

        return $endpoint->execute($this->database->client);
    }

    public function aggregate(array $pipeline, array $options = []): Traversable
    {
        $endpoint = new Aggregate($this->database->name, $this->name, $pipeline, $options);

        return $endpoint->execute($this->database->client);
    }
}
