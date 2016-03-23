<?php
/**
 * @file
 */

namespace CultuurNet\SilexAMQP\Console;

use Knp\Command\Command;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use RuntimeException;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;

class ConsumeCommand extends Command
{
    /**
     * @var string
     */
    private $amqpConnectionServiceName;

    /**
     * @var string
     */
    private $heartBeatServiceName;

    /**
     * @param string $name
     * @param $amqpConnectionServiceName
     */
    public function __construct($name, $amqpConnectionServiceName)
    {
        parent::__construct($name);

        $this->amqpConnectionServiceName = $amqpConnectionServiceName;
    }

    public function withHeartBeat($heartBeatServiceName)
    {
        $this->heartBeatServiceName = $heartBeatServiceName;
    }

    private function handleSignal(OutputInterface $output, $signal)
    {
        $output->writeln('Signal received, halting.');
        exit;
    }

    private function registerSignalHandlers(OutputInterface $output)
    {
        $handler = function ($signal) use ($output) {
            $this->handleSignal($output, $signal);
        };

        foreach ([SIGINT, SIGTERM, SIGQUIT] as $signal) {
            pcntl_signal($signal, $handler);
        }
    }

    protected function execute(InputInterface $input, OutputInterface $output)
    {
        $this->registerSignalHandlers($output);

        $output->writeln('Connecting...');
        $connection = $this->getAMQPConnection();
        $output->writeln('Connected. Listening for incoming messages...');

        $heartBeat = $this->getHeartBeat();

        $channel = $connection->channel(1);

        while (count($channel->callbacks) > 0) {
            if ($heartBeat) {
                $heartBeat($this->getSilexApplication());
            }

            pcntl_signal_dispatch();

            try {
                $channel->wait(null, true, 4);
            } catch (AMQPTimeoutException $e) {
                // Ignore this one.
            }
        }
    }

    /**
     * @return AMQPStreamConnection
     */
    protected function getAMQPConnection()
    {
        $app = $this->getSilexApplication();

        $connection = $app[$this->amqpConnectionServiceName];

        if (!$connection instanceof AMQPStreamConnection) {
            throw new RuntimeException(
                'The AMQP connection service is not of the expected type AMQPStreamConnection'
            );
        }

        return $connection;
    }

    /**
     * @return callable|null
     */
    protected function getHeartBeat()
    {
        $app = $this->getSilexApplication();

        $heartBeat = null;
        
        if ($this->heartBeatServiceName) {
            $heartBeat = $app[$this->heartBeatServiceName];

            if (!is_callable($heartBeat)) {
                throw new RuntimeException(
                    'The heartbeat service should be callable'
                );
            }
        }

        return $heartBeat;
    }
}
