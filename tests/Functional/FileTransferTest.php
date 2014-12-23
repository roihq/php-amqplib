<?php

namespace PhpAmqpLib\Tests\Functional;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;

class FileTransferTest extends \PHPUnit_Framework_TestCase
{

    protected $exchange_name = 'test_exchange';

    protected $queue_name = null;

    /**
     * @var AMQPConnection
     */
    protected $conn;

    /**
     * @var AMQPChannel
     */
    protected $ch;

    protected $msg_body;



    public function setUp()
    {
        parent::setUp();
    }



    public function testSendFile()
    {
        $this->conn = new AMQPConnection(HOST, PORT, USER, PASS, VHOST);
        $this->ch = $this->conn->channel();

        $this->ch->exchange_declare($this->exchange_name, 'direct', false, false, false);
        list($this->queue_name, ,) = $this->ch->queue_declare();
        $this->ch->queue_bind($this->queue_name, $this->exchange_name, $this->queue_name);
        $this->flush();

        $this->msg_body = file_get_contents(__DIR__ . '/fixtures/data_1mb.bin');
        $this->flush();
        //$this->msg_body = 'yes';

        $msg = new AMQPMessage($this->msg_body, array('delivery_mode' => 1));
        $this->flush();
        $this->ch->basic_publish($msg, $this->exchange_name, $this->queue_name);
        $this->flush();


        $this->ch->basic_consume(
            $this->queue_name,
            '',
            false,
            false,
            false,
            false,
            array($this, 'process_msg')
        );
        $this->flush();
        while (count($this->ch->callbacks)) {
            $this->ch->wait();
            $this->flush();
        }
        $this->flush();

        if ($this->ch) {
            $this->ch->exchange_delete($this->exchange_name);
            $this->ch->close();
            $this->flush();
        }

        if ($this->conn) {
            $this->conn->close();
            $this->flush();
        }
    }



    public function process_msg($msg)
    {
        $this->flush();

        echo '+++++++++++++++++++++++';
        //var_dump($msg);
        $delivery_info = $msg->delivery_info;

        $delivery_info['channel']->basic_ack($delivery_info['delivery_tag']);
        $this->flush();
        $delivery_info['channel']->basic_cancel($delivery_info['consumer_tag']);
        $this->flush();

        $this->assertEquals($this->msg_body, $msg->body);
        $this->flush();
        
    }

    public function flush()
    {
        //@ob_flush();
        //flush();
    }


    /*
    public function tearDown()
    {
        
    }
    */
}
