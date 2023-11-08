<?php

namespace MongoDB\Http;

use Psr\Http\Client\ClientInterface;
use Symfony\Component\HttpClient\HttpClient;

class Client
{
    public function __construct(
        private string $uri,
        private array $options = [],
        private ClientInterface|null $httpClient = null
    ) {
        $this->httpClient ??= HttpClient::create();
    }

    public function getDatabase(string $name): Database
    {
        return new Database($this, $name);
    }

    public function getCollection(string $databaseName, string $collectionName): Collection
    {
        return $this->getDatabase($databaseName)->getCollection($collectionName);
    }

    public function __get(string $name): Database
    {
        return $this->getDatabase($name);
    }

    public function getHttpClient(): ClientInterface
    {
        return $this->httpClient;
    }

    public function getDataSource(): string
    {
    }
}
