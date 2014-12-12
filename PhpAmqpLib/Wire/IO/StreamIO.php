<?php
namespace PhpAmqpLib\Wire\IO;

use PhpAmqpLib\Exception\AMQPIOException;
use PhpAmqpLib\Exception\AMQPRuntimeException;
use PhpAmqpLib\Exception\AMQPTimeoutException;
use PhpAmqpLib\Helper\MiscHelper;
use PhpAmqpLib\Wire\AMQPWriter;

class StreamIO extends AbstractIO
{
    /** @var  string */
    protected $host;

    /** @var int */
    protected $port;

    /** @var int */
    protected $connection_timeout;

    /** @var int */
    protected $read_write_timeout;

    /** @var resource */
    protected $context;

    /** @var bool */
    protected $keepalive;

    /** @var int */
    protected $heartbeat;

    /** @var float */
    protected $last_read;

    /** @var float */
    protected $last_write;

    /** @var resource */
    private $sock;

    /** @var bool */
    private $canDispatchPcntlSignal;

    /**
     * @param string $host
     * @param int $port
     * @param int $connection_timeout
     * @param int $read_write_timeout
     * @param null $context
     * @param bool $keepalive
     * @param int $heartbeat
     */
    public function __construct(
        $host,
        $port,
        $connection_timeout,
        $read_write_timeout,
        $context = null,
        $keepalive = false,
        $heartbeat = 0
    ) {
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
     * Sets up the stream connection
     *
     * @throws \PhpAmqpLib\Exception\AMQPRuntimeException
     * @throws \Exception
     */
    public function connect()
    {
        $errstr = $errno = null;

        //ini_set('magic_quotes_gpc', '0');
        //ini_set('magic_quotes_runtime', '0');
        //ini_set('magic_quotes_sybase', '0');
        if (get_magic_quotes_runtime()) {
            echo '----get_magic_quotes_runtime_true----';
        } else {
            echo '----get_magic_quotes_runtime_false----';
        }
        echo ini_get("magic_quotes_gpc");
        echo ini_get("magic_quotes_runtime");
        echo ini_get("magic_quotes_sybase");
        echo '-|-';

        if ($this->context) {
            $remote = sprintf('ssl://%s:%s', $this->host, $this->port);
            $this->sock = @stream_socket_client(
                $remote,
                $errno,
                $errstr,
                $this->connection_timeout,
                STREAM_CLIENT_CONNECT |
                STREAM_CLIENT_ASYNC_CONNECT,
                $this->context
            );
        } else {
            $remote = sprintf('tcp://%s:%s', $this->host, $this->port);
            $this->sock = @stream_socket_client(
                $remote,
                $errno,
                $errstr,
                $this->connection_timeout,
                STREAM_CLIENT_CONNECT |
                STREAM_CLIENT_ASYNC_CONNECT
            );
        }

        if (!$this->sock) {
            throw new AMQPRuntimeException(sprintf(
                'Error Connecting to server (%s): %s',
                $errno,
                $errstr
            ), $errno);
        }

        list($sec, $uSec) = MiscHelper::splitSecondsMicroseconds($this->read_write_timeout);
        if (!stream_set_timeout($this->sock, $sec, $uSec)) {
            throw new AMQPIOException('Timeout could not be set');
        }

        // php cannot capture signals while streams are blocking
        if ($this->canDispatchPcntlSignal) {
            echo '_dispatch_';
            $blocking = stream_set_blocking($this->sock, 0);
            echo ($blocking === true) ? '_blocking_true' : '_blocking_false';
            $write = stream_set_write_buffer($this->sock, 0);
            echo ($write === 0) ? '_write_buffer_true' : '_write_buffer_false';
            if (function_exists('stream_set_read_buffer')) {
                $read = stream_set_read_buffer($this->sock, 0);
                echo ($read === 0) ? '_read_buffer_true' : '_read_buffer_false';
            }
        } else {
            stream_set_blocking($this->sock, 1);
        }


        if ($this->keepalive) {
            $this->enable_keepalive();
        }
    }

    /**
     * Reconnects the socket
     */
    public function reconnect()
    {
        $this->close();
        $this->connect();
    }


    protected $is_reading = null;
    protected $is_writing = null;
    protected $is_blocking = null;

    /**
     * @param $n
     * @throws \PhpAmqpLib\Exception\AMQPIOException
     * @return mixed|string
     */
    public function read($n)
    {
        $this->is_reading = true;

        $res = '';
        $read = 0;

        while ($read < $n && !feof($this->sock) && (false !== ($buf = fread($this->sock, $n - $read)))) {
            
            $meta = stream_get_meta_data($this->sock);
            $unread = $meta['unread_bytes'];

            echo '****'.(($this->is_reading) ? 'true' : 'false').'****'.(($this->is_writing) ? 'true' : 'false').'****'.(($this->is_blocking) ? 'true' : 'false').'******';
            echo '.(r'.$n.'.'.$read.'.'.$unread.'.'.(mb_strlen($buf, 'ASCII')).'.)';

            //echo '.(r'.$n.'.'.$read.'.'.$res.'.'.$buff.'.)';
            ob_flush();
            flush();
            //pcntl_signal_dispatch();
            

            $this->check_heartbeat();
            //if ($buf === '') {
            //    echo '_done_';
           // }

            if ($buf === '') {// && $res !== ''
                if ($this->canDispatchPcntlSignal) {
                    // prevent cpu from being consumed while waiting
                    //echo 'wait ';
                    //echo $n.' ';
                    //echo $res.' ';
                    //echo $read.' ';
                    //echo (($read < $n) ? ' true' : ' false');
                    //echo ((!feof($this->sock)) ? ' true' : ' false');
                    //var_dump($buf);
                    //ob_flush();
                    //flush();
                    echo '====BLOCKING====';
                    ob_flush();
                    flush();
                    $this->is_blocking = true;
                    $this->select(null, null);
                    $this->is_blocking = false;
                    pcntl_signal_dispatch();
                }
                continue;
            } else {
                //var_dump($buf);
                //$this->select(0, 0);
            }

            $read += mb_strlen($buf, 'ASCII');
            $res .= $buf;

            $this->last_read = microtime(true);
        }
       

        if (mb_strlen($res, 'ASCII') != $n) {
            throw new AMQPIOException(sprintf(
                'Error reading data. Received %s instead of expected %s bytes',
                mb_strlen($res, 'ASCII'),
                $n
            ));
        }
        ob_flush();
        flush();
        $this->is_reading = false;
        return $res;
    }

    /**
     * @param $data
     * @return mixed|void
     * @throws \PhpAmqpLib\Exception\AMQPRuntimeException
     * @throws \PhpAmqpLib\Exception\AMQPTimeoutException
     */
    public function write($data)
    {
        $this->is_writing = true;
        // net.inet.tcp.sendspace
        // fwrite operations on non-blocking streams can be interrupted by the arrival of new packets
        // not atomic
        // call stream_select only write when stream can accept a write operation without blocking
        // is_resource($stream) && !feof($stream)
        // account for blocking and retries
        // EAGAIN/EWOULDBLOCK/EINTR


        $len = mb_strlen($data, 'ASCII');
        
        while (true) {
            echo '++'.$len.'++';
            if (is_null($this->sock)) {
                echo '**********************************************1';
                throw new AMQPRuntimeException('Broken pipe or closed connection, socket is null.');
                die;
            }

            if ($this->timed_out()) {
                echo '**********************************************4';
                throw new AMQPTimeoutException('Error sending data. Socket connection timed out');
                die;
            }

            /*
            $read = null;
            $write = array($this->sock);
            $except = null;
            $result = stream_select($read, $write, $except, null, null);
            var_dump($result); // returns 1
            */
           
            $meta = stream_get_meta_data($this->sock);
            var_dump($meta);
            ob_flush();
            flush();

            if (false === ($written = fwrite($this->sock, $data, $len))) {
                echo '**********************************************2';
                throw new AMQPRuntimeException('Error sending data');
            }
            ob_flush();
            flush();

            if ($written === 0) {
                echo '****'.(($this->is_reading) ? 'true' : 'false').'****'.(($this->is_writing) ? 'true' : 'false').'****'.(($this->is_blocking) ? 'true' : 'false').'******';
                echo '********************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************************3';
                $meta = stream_get_meta_data($this->sock);
                var_dump($meta);
                ob_flush();
                flush();
                /*
                $read = null;
                $write = array($this->sock);
                $except = null;
                $result = stream_select($read, $write, $except, null, $usec);
                var_dump($result); // returns 1
                */
                /*
                3array(7) {
                  ["stream_type"]=>
                  string(14) "tcp_socket/ssl"
                  ["mode"]=>
                  string(2) "r+"
                  ["unread_bytes"]=>
                  int(0)
                  ["seekable"]=>
                  bool(false)
                  ["timed_out"]=>
                  bool(false)
                  ["blocked"]=>
                  bool(false)
                  ["eof"]=>
                  bool(false)
                }
                int(1)

                 */


                //die;
                
                
                
                //throw new AMQPRuntimeException('Broken pipe or closed connection, zero data written.');
                //die;
                //$this->select(2, null);
                //exit(0);
                //continue;

            }

            
            $args = array(
                'written1' => $written,
                'written2' => (mb_strlen($written, 'ASCII')),
                'len' => $len,
                'rem' => null,
                'drem' => null
            );
            
            $len = $len - $written;
            $args['rem'] = $len;
            

            if ($len > 0) {
                echo 'din';
                echo '--'.$len.'--';
                $data = mb_substr($data, (0 - $len), null, 'ASCII');
                $args['drem'] = mb_strlen($data, 'ASCII');
                var_dump($args);
                ob_flush();
                flush();
                continue;

            } else {
                echo 'dout';
                $this->last_write = microtime(true);
                var_dump($args);
                ob_flush();
                flush();
                break;
            }
        }
        $this->is_writing = false;
    }

    /**
     * Heartbeat logic: check connection health here
     */
    protected function check_heartbeat()
    {
        // ignore unless heartbeat interval is set
        if ($this->heartbeat !== 0 && $this->last_read && $this->last_write) {
            echo '--heart--';
            $t = microtime(true);
            $t_read = round($t - $this->last_read);
            $t_write = round($t - $this->last_write);

            // server has gone away
            if (($this->heartbeat * 2) < $t_read) {
                echo '--heart1--';
                $this->reconnect();
            }

            // time for client to send a heartbeat
            if (($this->heartbeat / 2) < $t_write) {
                echo '--heart2--';
                $this->write_heartbeat();
            }
        }
    }

    /**
     * Sends a heartbeat message
     */
    protected function write_heartbeat()
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

    /**
     * @return resource
     */
    public function get_socket()
    {
        return $this->sock;
    }

    /**
     * @return resource
     */
    public function getSocket()
    {
        return $this->get_socket();
    }

    /**
     * @param $sec
     * @param $usec
     * @return int|mixed
     */
    public function select($sec, $usec)
    {
        $read = array($this->sock);
        $write = null;
        $except = null;

        $result = false;
        set_error_handler(function() { return true; }, E_WARNING);
        $result = stream_select($read, $write, $except, $sec, $usec);
        restore_error_handler();

        return $result;
    }

    /**
     * @return mixed
     */
    protected function timed_out()
    {
        // get status of socket to determine whether or not it has timed out
        $info = stream_get_meta_data($this->sock);

        return $info['timed_out'];
    }

    /**
     * @throws \PhpAmqpLib\Exception\AMQPIOException
     */
    protected function enable_keepalive()
    {
        if (!function_exists('socket_import_stream')) {
            throw new AMQPIOException('Can not enable keepalive: function socket_import_stream does not exist');
        }

        if (!defined('SOL_SOCKET') || !defined('SO_KEEPALIVE')) {
            throw new AMQPIOException('Can not enable keepalive: SOL_SOCKET or SO_KEEPALIVE is not defined');
        }

        $socket = socket_import_stream($this->sock);
        socket_set_option($socket, SOL_SOCKET, SO_KEEPALIVE, 1);
    }
}
