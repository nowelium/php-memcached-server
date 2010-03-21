<?php

class MemcachedServer {
    protected $command;
    public function __construct(MemcachedCommand $command){
        $this->command = $command;
    }
    public function start(){
        $this->run();
    }
    public function run(){
        $socket = @socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        if(false === $socket){
            $code = socket_last_error();
            $msg = socket_strerror($code);
            throw new Exception(sprintf('socket_create was error(%s):%s', $code, $msg));
        }
        socket_set_option($socket, SOL_SOCKET, SO_REUSEADDR, 1);

        $binded = @socket_bind($socket, 0, 11222);
        if(false === $binded){
            $code = socket_last_error();
            $msg = socket_strerror($code);
            throw new Exception(sprintf('socket_bind was error(%s):%s', $code, $msg));
        }
        $listend = @socket_listen($socket);
        if(false === $listend){
            $code = socket_last_error();
            $msg = socket_strerror($code);
            throw new Exception(sprintf('socket_listen was error(%s):%s', $code, $msg));
        }
        //socket_set_nonblock($socket);
       
        echo 'server start on tcp://0.0.0.0:11222', PHP_EOL;
        $read = array($socket);
        $write = null;
        $except = null;
        while(true){
            $accept = @socket_accept($socket);
            if(false === $accept){
                continue;
            }
            $handler = new MemcachedAcceptHandler($accept, $this->command);
            $handler->init();
            $handler->execute();
            $handler->destroy();
        }
    }
}

interface StreamReadWrite {
    const DELIMITER = "\r\n";
    public function readLine();
    public function writeLine($str);
}

class MemcachedAcceptHandler implements StreamReadWrite {
    protected $socket;
    protected $command;
    protected $connected = true;
    public function __construct($socket, MemcachedCommand $command){
        $this->socket = $socket;
        $this->command = $command;
    }
    public function __destruct(){
        if(null !== $this->socket){
            @socket_close($this->socket);
            unset($this->socket);
        }
    }
    public function init(){
        echo 'new connection', PHP_EOL;
        //socket_set_nonblock($this->socket);
    }
    public function execute(){
        while($this->connected){
            $read = array($this->socket);
            $write = array();
            $except = array();
            $select = @socket_select($read, $write, $except, 1);
            if(false === $select){
                throw new RuntimeException('socket_select');
            }
            if($select < 1){
                continue;
            }

            $line = $this->readLine();
            if(null === $line){
                continue;
            }
            // get hoge => get
            $mode = substr($line, 0, 3);
            // get hoge foo => array(hoge, foo)
            $args = explode(' ', substr($line, 4));
            $this->command->call($this, $mode, $args);
        }
    }
    public function destroy(){
        echo 'close connection', PHP_EOL;
        @socket_shutdown($this->socket);
    }
    public function readLine(){
        $line = '';
        while(true){
            $buf = @socket_read($this->socket, 1);
            if(false === $buf || '' === $buf){
                // FIXME!!!
                $this->connected = false;
                return null;
            }
            $line .= $buf;
            if(self::DELIMITER == substr($line, -2)){
                $line = substr($line, 0, -2);
                break;
            }
        }
        if(empty($line)){
            return null;
        }
        if(preg_match('/^\s+$/', $line)){
            return null;
        }
        return $line;
    }
    public function writeLine($str){
        return @socket_write($this->socket, $str . self::DELIMITER);
    }
}

interface MemcachedCommand {
    public function call(StreamReadWrite $reader, $mode, array $args);
}

abstract class AbstractMemcachedCommand implements MemcachedCommand {
    protected $reflector;
    public function __construct(){
        $this->reflector = new ReflectionObject($this);
    }
    protected static function concat(array $a, array $b){
        array_splice($a, count($a), 0, $b);
        return $a;
    }
    public final function call(StreamReadWrite $rw, $mode, array $args){
        if(!$this->reflector->hasMethod($mode)){
            return $this->error($rw);
        }
        echo 'command => ', $mode, ' ', join(' ', $args), PHP_EOL;
        return call_user_func_array(array($this, $mode), self::concat(array($rw), $args));
    }
    protected function error($rw){
        $rw->writeLine('ERROR');
    }
    protected abstract function get(StreamReadWrite $rw, $keys);
    protected abstract function set(StreamReadWrite $rw, $key, $flag, $expire, $length);
    protected abstract function delete(StreamReadWrite $rw, $key, $expire = 0);
}
class StorageMemcacheCommand extends AbstractMemcachedCommand {
    protected $cache = array();
    protected function get(StreamReadWrite $rw, $keys){
        $args = func_get_args();
        array_shift($args);
        foreach($args as $key){
            if(!isset($this->cache[$key])){
                continue;
            }
            $value = $this->cache[$key];
            if($value->expire < time()){
                continue;
            }
            $rw->writeLine(sprintf('VALUE %s %d %d', $key, $value->flag, $value->length));
            $rw->writeLine($value->value);
        }
        $rw->writeLine('END');
    }
    protected function set(StreamReadWrite $rw, $key, $flag, $expire, $length){
        $value = new stdClass;
        $value->flag = $flag;
        $value->expire = time() + $expire;
        $value->length = $length;
        $value->value = $rw->readLine();
        $this->cache[$key] = $value;
        $rw->writeLine('STORED');
    }
    protected function delete(StreamReadWrite $rw, $key, $expire = 0){
        if(isset($this->cache[$key])){
            $this->cache[$key]->expire = $expire;
        }
    }
}

$server = new MemcachedServer(new StorageMemcacheCommand);
$server->start();

