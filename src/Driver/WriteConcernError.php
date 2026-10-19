<?php
/*
 * Copyright 2015-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

declare(strict_types=1);

namespace MongoDB\Driver;

final class WriteConcernError
{
    private ?object $details;

    private function __construct(
        public readonly int $code,
        public readonly string $message,
        public readonly ?object $info = null,
        ?array $details = null,
    ) {
        $this->details = $details !== null ? (object) $details : null;
    }

    public function __debugInfo(): array
    {
        return [
            'message' => $this->message,
            'code'    => $this->code,
            'info'    => $this->info,
        ];
    }

    public function getCode(): int
    {
        return $this->code;
    }

    public function getDetails(): ?object
    {
        return $this->details;
    }

    public function getInfo(): ?object
    {
        return $this->info;
    }

    public function getMessage(): string
    {
        return $this->message;
    }

    /** @internal */
    public static function create(int $code, string $message, ?object $info = null, ?array $details = null): self
    {
        return new self($code, $message, $info, $details);
    }
}
