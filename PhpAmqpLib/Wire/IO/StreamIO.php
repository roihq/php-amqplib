<?php

namespace PhpAmqpLib\Wire\IO;

use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Helper\MiscHelper;
use PhpAmqpLib\Wire\AMQPWriter;

class StreamIO extends AbstractIO
{

    /**
     * @var string
     */
    protected $host;

    /**
     * @var int
     */
    protected $port;

    /**
     * @var int
     */
    protected $connection_timeout;

    /**
     * @var int
     */
    protected $read_write_timeout;

    /**
     * @var resource
     */
    protected $context;

    /**
     * @var bool
     */
    protected $keepalive;

    /**
     * @var int
     */
    protected $heartbeat;

    

    /**
     * @var float
     */
    protected $last_read;

    /**
     * @var float
     */
    protected $last_write;

    /**
     * @var array
     */
    protected $last_error;



    /**
     * @var resource
     */
    private $sock;

    /**
     * @var bool
     */
    private $canDispatchPcntlSignal;


    public function __construct($host, $port, $connection_timeout, $read_write_timeout, $context = null, $keepalive = false, $heartbeat = 0)
    {
        $this->host = $host;
        $this->port = $port;
        $this->connection_timeout = $connection_timeout;
        $this->read_write_timeout = $read_write_timeout;
        $this->context = $context;
        $this->keepalive = $keepalive;
        $this->heartbeat = $heartbeat;
        $this->canDispatchPcntlSignal = extension_loaded('pcntl') && function_exists('pcntl_signal_dispatch')
            && (defined('AMQP_WITHOUT_SIGNALS') ? !AMQP_WITHOUT_SIGNALS : true);
    }



    /**
     * Setup the stream connection
     *
     * @throws \PhpAmqpLib\Exception\AMQPRuntimeException
     * @throws \Exception
     */
    public function connect()
    {
        $remote = sprintf(
            '%s://%s:%s', 
            (is_null($this->context)) ? 'tcp' : 'ssl',
            $this->host,
            $this->port
        );
        
        $errstr = $errno = null;

        set_error_handler(array($this, 'error_handler'));
        
        $this->sock = stream_socket_client(
            $remote,
            $errno,
            $errstr,
            $this->connection_timeout,
            STREAM_CLIENT_CONNECT | STREAM_CLIENT_ASYNC_CONNECT,
            $this->context ?: stream_context_create()
        );
        
        restore_error_handler();

        if (false === $this->sock) {
            throw new AMQPRuntimeException(
                sprintf(
                    'Error Connecting to server(%s): %s ', 
                    $errno,
                    $errstr
                )
            );
        }

        if (false === stream_socket_get_name($this->sock, true)) {
            throw new AMQPRuntimeException(
                sprintf(
                    'Connection refused(%s): %s ', 
                    $remote
                )
            );
        }
        var_dump(stream_socket_get_name($this->sock, true));



        list($sec, $uSec) = MiscHelper::splitSecondsMicroseconds($this->read_write_timeout);
        if (!stream_set_timeout($this->sock, $sec, $uSec)) {
            throw new AMQPIOException("Timeout could not be set");
        }

        // bugs.php.net/41631 5.4.33
        // bugs.php.net/65137 5.4.34
        // 5.4.36

        // php cannot capture signals while streams are blocking
        if ($this->canDispatchPcntlSignal) {
            stream_set_blocking($this->sock, 0);
            stream_set_write_buffer($this->sock, 0);
            if (function_exists('stream_set_read_buffer')) {
                stream_set_read_buffer($this->sock, 0);
            }
        } else {
            stream_set_blocking($this->sock, 1);
        }

        if ($this->keepalive) {
            $this->enable_keepalive();
        }
    }

    /**
     * Reconnect the socket
     */
    public function reconnect()
    {
        $this->close();
        $this->connect();
    }



    public function read($len)
    {
        $read = 0;
        $data = '';

        while ($read < $len) {
            echo '+r';
            $this->flush();

            $this->check_heartbeat();

            

            if (feof($this->sock)) {
                echo '--------broken-read-1';
                $this->flush();
                break;
            }

            if (!is_resource($this->sock)) {
                echo '--------broken-pipe-read';
                $this->flush();
                break;
            }


            set_error_handler(array($this, 'error_handler'));
            $buffer = fread($this->sock, ($len - $read));
            restore_error_handler();

            if (false === $buffer) {
                echo '--------broken-read-data';
                $this->flush();
                break;
            }

            if ($buffer === '' && feof($this->sock)) {
                echo '--------broken-read-2';
                $this->flush();
                break;
            }

            




            
            

            if ($buffer === '') {
                if ($this->canDispatchPcntlSignal) {
                    // prevent cpu from being consumed while waiting
                    
                    echo '+b';
                    $this->flush();
                    $this->select(null, null);
                    //sleep(1);
                    pcntl_signal_dispatch();
                }
                continue;
            }

            $read += mb_strlen($buffer, 'ASCII');
            $data .= $buffer;
        }

        if (mb_strlen($data, 'ASCII') != $len) {
            throw new AMQPRuntimeException("Error reading data. Received " .
                mb_strlen($data, 'ASCII') . " instead of expected $len bytes");
        }

        $this->last_read = microtime(true);
        return $data;
    }



    public function write($data)
    {
        $written = 0;
        $len = mb_strlen($data, 'ASCII');
        
        while ($written < $len) {
            echo '+w';
            $this->flush();
            

                   


            if (!is_resource($this->sock)) {
                echo '--------broken-pipe';
                $this->flush();
                throw new AMQPRuntimeException("Broken pipe or closed connection");
            }

            set_error_handler(array($this, 'error_handler'));
            $buffer = fwrite($this->sock, $data);
            restore_error_handler();


            if (false === $buffer) {
                echo '--------broken-data';
                $meta = stream_get_meta_data($this->sock);
                var_dump($this->last_error);
                var_dump($meta);
                $this->flush();
                throw new AMQPRuntimeException("Error sending data");
            }

            if ($buffer === 0 && feof($this->sock)) {
                echo '--------broken-write';
                $meta = stream_get_meta_data($this->sock);
                var_dump($this->last_error);
                var_dump($meta);
                $this->flush();
                throw new AMQPRuntimeException("Broken pipe or closed connection");
            }

            if ($this->timed_out()) {
                echo '--------timed-out';
                $meta = stream_get_meta_data($this->sock);
                var_dump($this->last_error);
                var_dump($meta);
                $this->flush();
                throw new AMQPTimeoutException("Error sending data. Socket connection timed out");
            }

            $written += mb_strlen($buffer, 'ASCII');
            $data = mb_substr($data, $buffer, mb_strlen($data, 'ASCII') - $buffer, 'ASCII');
        }

        $this->last_write = microtime(true);
        return;
    }

    public function error_handler($errno, $errstr, $errfile, $errline, $errcontext = null)
    {
        $this->last_error = compact('errno', 'errstr', 'errfile', 'errline', 'errcontext');

        // fwrite warning that stream isn't ready
        if (strstr($errstr, 'Resource temporarily unavailable')) {
             echo 'TTTTTTTTTTTTTTTTTTTTTTTTTTTTT';
             return;
        }

        // stream_select warning that it has been interrupted by a signal
        if (strstr($errstr, 'Interrupted system call')) {
             echo 'SSSSSSSSSSSSSSSSSSSSSSSSSSSSS';
             return;
        }

        echo 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX';
        var_dump($this->last_error);
        
        return;
    }

    public function flush()
    {
        @ob_flush();
        flush();
    }




    public function check_heartbeat()
    {
        // ignore unless heartbeat interval is set
        if ($this->heartbeat !== 0 && $this->last_read && $this->last_write) {
            $t = microtime(true);
            $t_read  = round($t - $this->last_read);
            $t_write = round($t - $this->last_write);
            
            // server has gone away
            if (($this->heartbeat * 2) < $t_read) {
                $this->reconnect();
            }

            // time for client to send a heartbeat
            if (($this->heartbeat / 2) < $t_write) {
                $this->write_heartbeat();
            }
        }
    }



    public function write_heartbeat()
    {
        $pkt = new AMQPWriter();
        $pkt->write_octet(8);
        $pkt->write_short(0);
        $pkt->write_long(0);
        $pkt->write_octet(0xCE);
        $val = $pkt->getvalue();
        $this->write($pkt->getvalue());
    }



    public function close()
    {
        if (is_resource($this->sock)) {
            fclose($this->sock);
        }
        $this->sock = null;
    }



    public function get_socket()
    {
        return $this->sock;
    }



    public function select($sec, $usec)
    {
        $read = array($this->sock);
        $write = null;
        $except = null;

        $result = false;
        set_error_handler(array($this, 'error_handler'));
        $result = stream_select($read, $write, $except, $sec, $usec);
        restore_error_handler();

        return $result;
    }



    protected function timed_out()
    {
        // get status of socket to determine whether or not it has timed out
        $info = stream_get_meta_data($this->sock);
        return $info['timed_out'];
    }

    protected function enable_keepalive()
    {
        if (!function_exists('socket_import_stream')) {
            throw new AMQPIOException("Can not enable keepalive: function socket_import_stream does not exist");
        }

        if (!defined('SOL_SOCKET') || !defined('SO_KEEPALIVE')) {
            throw new AMQPIOException("Can not enable keepalive: SOL_SOCKET or SO_KEEPALIVE is not defined");
        }

        $socket = socket_import_stream($this->sock);
        socket_set_option($socket, SOL_SOCKET, SO_KEEPALIVE, 1);
    }

}
