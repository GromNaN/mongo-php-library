<?php

namespace MongoDB\Tests\UnifiedSpecTests;

use ArrayIterator;
use MongoDB\Driver\Monitoring\CommandFailedEvent;
use MongoDB\Driver\Monitoring\CommandStartedEvent;
use MongoDB\Driver\Monitoring\CommandSubscriber;
use MongoDB\Driver\Monitoring\CommandSucceededEvent;
use MongoDB\Tests\UnifiedSpecTests\Constraint\Matches;
use MultipleIterator;
use PHPUnit\Framework\Assert;
use stdClass;

use function array_reverse;
use function count;
use function current;
use function is_object;
use function key;
use function MongoDB\Driver\Monitoring\addSubscriber;
use function MongoDB\Driver\Monitoring\removeSubscriber;
use function PHPUnit\Framework\assertArrayHasKey;
use function PHPUnit\Framework\assertCount;
use function PHPUnit\Framework\assertGreaterThanOrEqual;
use function PHPUnit\Framework\assertInstanceOf;
use function PHPUnit\Framework\assertIsBool;
use function PHPUnit\Framework\assertIsObject;
use function PHPUnit\Framework\assertIsString;
use function PHPUnit\Framework\assertNotEmpty;
use function PHPUnit\Framework\assertObjectHasAttribute;
use function PHPUnit\Framework\assertSame;
use function PHPUnit\Framework\assertThat;
use function sprintf;

/**
 * EventObserver handles "observeEvents" for client entities and assertions for
 * "expectEvents" and special operations (e.g. assertSameLsidOnLastTwoCommands).
 */
final class EventObserver implements CommandSubscriber
{
    /**
     * These commands are always considered sensitive (i.e. command and reply
     * documents should be redacted).
     *
     * @see https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst#security
     */
    private static array $sensitiveCommands = [
        'authenticate' => 1,
        'saslStart' => 1,
        'saslContinue' => 1,
        'getnonce' => 1,
        'createUser' => 1,
        'updateUser' => 1,
        'copydbgetnonce' => 1,
        'copydbsaslstart' => 1,
        'copydb' => 1,
    ];

    /**
     * These commands are only considered sensitive when the command or reply
     * document includes a speculativeAuthenticate field.
     *
     * @see https://github.com/mongodb/specifications/blob/master/source/command-monitoring/command-monitoring.rst#security
     */
    private static array $sensitiveCommandsWithSpeculativeAuthenticate = [
        'ismaster' => 1,
        'isMaster' => 1,
        'hello' => 1,
    ];

    private static array $supportedEvents = [
        'commandStartedEvent' => CommandStartedEvent::class,
        'commandSucceededEvent' => CommandSucceededEvent::class,
        'commandFailedEvent' => CommandFailedEvent::class,
    ];

    /**
     * These events are defined in the specification but unsupported by PHPLIB
     * (e.g. CMAP events).
     */
    private static array $unsupportedEvents = [
        'poolCreatedEvent' => 1,
        'poolReadyEvent' => 1,
        'poolClearedEvent' => 1,
        'poolClosedEvent' => 1,
        'connectionCreatedEvent' => 1,
        'connectionReadyEvent' => 1,
        'connectionClosedEvent' => 1,
        'connectionCheckOutStartedEvent' => 1,
        'connectionCheckOutFailedEvent' => 1,
        'connectionCheckedOutEvent' => 1,
        'connectionCheckedInEvent' => 1,
    ];

    private array $actualEvents = [];

    /**
     * The configureFailPoint command (used by failPoint and targetedFailPoint
     * operations) is always ignored.
     */
    private array $ignoreCommands = ['configureFailPoint' => 1];

    private array $observeEvents = [];

    public function __construct(array $observeEvents, array $ignoreCommands, private bool $observeSensitiveCommands, private string $clientId, private Context $context)
    {
        assertNotEmpty($observeEvents);

        foreach ($observeEvents as $event) {
            assertIsString($event);

            /* Unlike Context::assertExpectedEventsForClients, which runs within
             * a test, EventObserver is constructed via createEntities (before
             * all tests). Ignoring events here allows tests within the file
             * that don't assert these events to still execute. */
            if (isset(self::$unsupportedEvents[$event])) {
                continue;
            }

            assertArrayHasKey($event, self::$supportedEvents);
            $this->observeEvents[self::$supportedEvents[$event]] = 1;
        }

        foreach ($ignoreCommands as $command) {
            assertIsString($command);
            $this->ignoreCommands[$command] = 1;
        }
    }

    /** @see https://php.net/manual/en/mongodb-driver-monitoring-commandsubscriber.commandfailed.php */
    public function commandFailed(CommandFailedEvent $event): void
    {
        $this->handleEvent($event);
    }

    /** @see https://php.net/manual/en/mongodb-driver-monitoring-commandsubscriber.commandstarted.php */
    public function commandStarted(CommandStartedEvent $event): void
    {
        $this->handleEvent($event);
    }

    /** @see https://php.net/manual/en/mongodb-driver-monitoring-commandsubscriber.commandsucceeded.php */
    public function commandSucceeded(CommandSucceededEvent $event): void
    {
        $this->handleEvent($event);
    }

    public function start(): void
    {
        addSubscriber($this);
    }

    public function stop(): void
    {
        removeSubscriber($this);
    }

    public function getLsidsOnLastTwoCommands(): array
    {
        $lsids = [];

        foreach (array_reverse($this->actualEvents) as $event) {
            if (! $event instanceof CommandStartedEvent) {
                continue;
            }

            $command = $event->getCommand();
            assertObjectHasAttribute('lsid', $command);
            $lsids[] = $command->lsid;

            if (count($lsids) === 2) {
                return $lsids;
            }
        }

        Assert::fail('Not enough CommandStartedEvents observed');
    }

    public function assert(array $expectedEvents, bool $ignoreExtraEvents): void
    {
        if ($ignoreExtraEvents) {
            assertGreaterThanOrEqual(count($expectedEvents), count($this->actualEvents));
        } else {
            assertCount(count($expectedEvents), $this->actualEvents);
        }

        $mi = new MultipleIterator(MultipleIterator::MIT_NEED_ANY);
        $mi->attachIterator(new ArrayIterator($expectedEvents));
        $mi->attachIterator(new ArrayIterator($this->actualEvents));

        foreach ($mi as $keys => $events) {
            [$expectedEvent, $actualEvent] = $events;

            if ($ignoreExtraEvents && $expectedEvent === null) {
                break;
            }

            assertIsObject($expectedEvent);
            $expectedEvent = (array) $expectedEvent;
            assertCount(1, $expectedEvent);

            $type = key($expectedEvent);
            assertArrayHasKey($type, self::$supportedEvents);
            $data = current($expectedEvent);
            assertIsObject($data);

            // Message is used for actual event assertions (not test structure)
            $message = sprintf('%s event[%d]', $this->clientId, $keys[0]);

            assertInstanceOf(self::$supportedEvents[$type], $actualEvent, $message . ': type matches');
            $this->assertEvent($actualEvent, $data, $message);
        }
    }

    private function assertEvent($actual, stdClass $expected, string $message): void
    {
        assertIsObject($actual);

        match ($actual::class) {
            CommandStartedEvent::class => $this->assertCommandStartedEvent($actual, $expected, $message),
            CommandSucceededEvent::class => $this->assertCommandSucceededEvent($actual, $expected, $message),
            CommandFailedEvent::class => $this->assertCommandFailedEvent($actual, $expected, $message),
            default => Assert::fail($message . ': Unsupported event type: ' . $actual::class)
        };
    }

    private function assertCommandStartedEvent(CommandStartedEvent $actual, stdClass $expected, string $message): void
    {
        Util::assertHasOnlyKeys($expected, ['command', 'commandName', 'databaseName', 'hasServiceId', 'hasServerConnectionId']);

        if (isset($expected->command)) {
            assertIsObject($expected->command);
            $constraint = new Matches($expected->command, $this->context->getEntityMap());
            assertThat($actual->getCommand(), $constraint, $message . ': command matches');
        }

        if (isset($expected->commandName)) {
            assertIsString($expected->commandName);
            assertSame($actual->getCommandName(), $expected->commandName, $message . ': commandName matches');
        }

        if (isset($expected->databaseName)) {
            assertIsString($expected->databaseName);
            assertSame($actual->getDatabaseName(), $expected->databaseName, $message . ': databaseName matches');
        }

        if (isset($expected->hasServiceId)) {
            assertIsBool($expected->hasServiceId);
            assertSame($actual->getServiceId() !== null, $expected->hasServiceId, $message . ': hasServiceId matches');
        }

        if (isset($expected->hasServerConnectionId)) {
            assertIsBool($expected->hasServerConnectionId);
            assertSame($actual->getServerConnectionId() !== null, $expected->hasServerConnectionId, $message . ': hasServerConnectionId matches');
        }
    }

    private function assertCommandSucceededEvent(CommandSucceededEvent $actual, stdClass $expected, string $message): void
    {
        Util::assertHasOnlyKeys($expected, ['reply', 'commandName', 'databaseName', 'hasServiceId', 'hasServerConnectionId']);

        if (isset($expected->reply)) {
            assertIsObject($expected->reply);
            $constraint = new Matches($expected->reply, $this->context->getEntityMap());
            assertThat($actual->getReply(), $constraint, $message . ': reply matches');
        }

        if (isset($expected->commandName)) {
            assertIsString($expected->commandName);
            assertSame($actual->getCommandName(), $expected->commandName, $message . ': commandName matches');
        }

        if (isset($expected->databaseName)) {
            assertIsString($expected->databaseName);
            assertSame($actual->getDatabaseName(), $expected->databaseName, $message . ': databaseName matches');
        }

        if (isset($expected->hasServiceId)) {
            assertIsBool($expected->hasServiceId);
            assertSame($actual->getServiceId() !== null, $expected->hasServiceId, $message . ': hasServiceId matches');
        }

        if (isset($expected->hasServerConnectionId)) {
            assertIsBool($expected->hasServerConnectionId);
            assertSame($actual->getServerConnectionId() !== null, $expected->hasServerConnectionId, $message . ': hasServerConnectionId matches');
        }
    }

    private function assertCommandFailedEvent(CommandFailedEvent $actual, stdClass $expected, string $message): void
    {
        Util::assertHasOnlyKeys($expected, ['commandName', 'databaseName', 'hasServiceId', 'hasServerConnectionId']);

        if (isset($expected->commandName)) {
            assertIsString($expected->commandName);
            assertSame($actual->getCommandName(), $expected->commandName, $message . ': commandName matches');
        }

        if (isset($expected->databaseName)) {
            assertIsString($expected->databaseName);
            assertSame($actual->getDatabaseName(), $expected->databaseName, $message . ': databaseName matches');
        }

        if (isset($expected->hasServiceId)) {
            assertIsBool($expected->hasServiceId);
            assertSame($actual->getServiceId() !== null, $expected->hasServiceId, $message . ': hasServiceId matches');
        }

        if (isset($expected->hasServerConnectionId)) {
            assertIsBool($expected->hasServerConnectionId);
            assertSame($actual->getServerConnectionId() !== null, $expected->hasServerConnectionId, $message . ': hasServerConnectionId matches');
        }
    }

    private function handleEvent(CommandStartedEvent|CommandSucceededEvent|CommandFailedEvent $event): void
    {
        if (! $this->context->isActiveClient($this->clientId)) {
            return;
        }

        if (! is_object($event)) {
            return;
        }

        if (! isset($this->observeEvents[$event::class])) {
            return;
        }

        if (isset($this->ignoreCommands[$event->getCommandName()])) {
            return;
        }

        if (! $this->observeSensitiveCommands && $this->isSensitiveCommand($event)) {
            return;
        }

        $this->actualEvents[] = $event;
    }

    private function isSensitiveCommand(CommandStartedEvent|CommandSucceededEvent|CommandFailedEvent $event): bool
    {
        if (isset(self::$sensitiveCommands[$event->getCommandName()])) {
            return true;
        }

        /* If the command or reply included a speculativeAuthenticate field,
         * libmongoc will already have redacted it (CDRIVER-4000). Therefore, we
         * can infer that the command was sensitive if its command or reply is
         * empty. */
        if (isset(self::$sensitiveCommandsWithSpeculativeAuthenticate[$event->getCommandName()])) {
            $commandOrReply = $event instanceof CommandStartedEvent ? $event->getCommand() : $event->getReply();

            return (array) $commandOrReply === [];
        }

        return false;
    }
}
