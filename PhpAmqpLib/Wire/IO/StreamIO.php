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

        $this->sock = stream_socket_client(
            $remote,
            $errno,
            $errstr,
            $this->connection_timeout,
            STREAM_CLIENT_CONNECT | STREAM_CLIENT_ASYNC_CONNECT,
            $this->context ?: stream_context_create()
        );

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
                    $errno,
                    $errstr
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



    public function read($n)
    {
        $res = '';
        $read = 0;

        while ($read < $n && !feof($this->sock) && (false !== ($buf = fread($this->sock, $n - $read)))) {
            






            echo '+r';
            @ob_flush();
            flush();
            $this->check_heartbeat();

            if ($buf === '') {
                if ($this->canDispatchPcntlSignal) {
                    // prevent cpu from being consumed while waiting
                    //$this->select(null, null);
                    echo '+b';
                    @ob_flush();
                    flush();
                    sleep(1);
                    pcntl_signal_dispatch();
                }
                continue;
            }

            $read += mb_strlen($buf, 'ASCII');
            $res .= $buf;

            $this->last_read = microtime(true);
        }

        if (mb_strlen($res, 'ASCII') != $n) {
            throw new AMQPRuntimeException("Error reading data. Received " .
                mb_strlen($res, 'ASCII') . " instead of expected $n bytes");
        }

        return $res;
    }



    public function write($data)
    {
        //echo 'SOCKET_EWOULDBLOCK='.SOCKET_EWOULDBLOCK;
        //echo 'SOCKET_EAGAIN='.SOCKET_EAGAIN;

        //fwrite(): send of 8192 bytes failed with errno=11 Resource temporarily unavailable
        //fread(): unable to read from socket [35]: Resource temporarily unavailable
        //clearstatcache
        //
        //EAGAIN = 'Resource temporarily unavailable';
        //EWOULDBLOCK
        //EINTR
        //MAX_RETRIES
        //
        //SOCKET_EAGAIN
        //
        //SOCKET_ENOBUFS

        $len = mb_strlen($data, 'ASCII');
        
        while (true) {
            echo '+w';
            ob_flush();
            flush();
            

            /*
            if (socket_last_error($socket) == SOCKET_EAGAIN or            

                    socket_last_error($socket) == SOCKET_EWOULDBLOCK or

                    socket_last_error($socket) == SOCKET_EINPROGRESS) 

                {
                }*/
        
        


            if (!is_resource($this->sock)) {
                echo '--------broken-pipe';
                //throw new AMQPRuntimeException("Broken pipe or closed connection");
            }

            

            set_error_handler(array($this, 'error'));
            $written = fwrite($this->sock, $data);
            restore_error_handler();


            if (false === $written) {
                $meta = stream_get_meta_data($this->sock);
                var_dump($this->last_error);
                var_dump($meta);
                echo '--------broken-data';
                //throw new AMQPRuntimeException("Error sending data");
            }

            if ($written === 0 && feof($this->sock)) {
                $meta = stream_get_meta_data($this->sock);
                var_dump($this->last_error);
                var_dump($meta);
                echo '--------broken-write';
                //throw new AMQPRuntimeException("Broken pipe or closed connection");
            }





            if ($this->timed_out()) {
                $meta = stream_get_meta_data($this->sock);
                var_dump($this->last_error);
                var_dump($meta);
                echo '--------timed-out';
                //throw new AMQPTimeoutException("Error sending data. Socket connection timed out");
            }
            /*
            $len = $len - $written;
            if ($len > 0) {
                $data = mb_substr($data, $written, mb_strlen($data, 'ASCII') - $written, 'ASCII');
                continue;
            } else {
                $this->last_write = microtime(true);
                break;
            }
            */
           
            if ($written === mb_strlen($data, 'ASCII')) {
                $this->last_write = microtime(true);
                echo '-full.'.$written.'-';
                @ob_flush();
                flush();
                break;
            } else {
                $data = mb_substr($data, $written, mb_strlen($data, 'ASCII') - $written, 'ASCII');
                echo '-part.'.$written.'-';
                @ob_flush();
                flush();
                continue;
            }


        }
    }

    public function error($errno, $errstr, $errfile, $errline, $errcontext = null)
    {
        //8     E_NOTICE


        echo 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX';
        var_dump(error_get_last());
        $e = socket_last_error();
        var_dump($e);
        var_dump(socket_strerror($e));
        $this->last_error = compact('errno', 'errstr', 'errfile', 'errline', 'errcontext');
        if ($this->last_error['errno'] === SOCKET_EAGAIN) {
            '++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++';
        }
        echo 'readdy='.$this->last_error['errno'];


        var_dump($this->last_error);
        //return true;
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
        set_error_handler(array($this, 'error'));
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
