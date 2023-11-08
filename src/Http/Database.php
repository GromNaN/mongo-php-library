<?php

namespace MongoDB\Http;

class Database
{
    public function __construct(
        public readonly Client $client,
        public readonly string $name
    ) {
    }

    public function getCollection(string $collectionName): Collection
    {
        return new Collection($this, $collectionName);
    }

    public function __get(string $name): Collection
    {
        return $this->getCollection($name);
    }
}
