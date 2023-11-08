<?php

namespace MongoDB\Http\Endpoint;

use MongoDB\Http\Client;

class Endpoint
{
    public function __construct(
        private readonly string $path,
        private readonly array $parameters,
    ) {
    }

    public function execute(Client $client)
    {
        //$request = new

        $client->getHttpClient()->sendRequest();
    }
}
