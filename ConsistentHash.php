<?php

interface HashFunction {
    public function hash($key);
}
interface Circle {
    public function put($key, $value);
}
interface Node {
    public function put($key, $value);
    public function get($key);
    public function has($key);
    public function keys();
    public function getName();
}

class HashMD5Integer implements HashFunction {
    public function hash($key){
        $values = unpack('h*', md5($key));
        return $values[1];
    }
}
class HashSha1Integer implements HashFunction {
    public function hash($key){
        $values = unpack('h*', sha1($key));
        return $values[1];
    }
}
class HashCRC32Integer implements HashFunction {
    public function hash($key){
        $values = unpack('h*', sprintf('%u', crc32($key)));
        return $values[1];
    }
}
class TreeMap implements Circle {
    private $values;
    public function __construct(array $values = array()){
        $this->values = new ArrayObject($values);
    }
    public function put($key, $value){
        $this->values->offsetSet($key, $value);
    }
    public function get($key){
        return $this->values->offsetGet($key);
    }
    public function remove($key){
        $this->values->offsetUnset($key);
    }
    public function has($key){
        return $this->values->offsetExists($key);
    }
    public function isEmpty(){
        return $this->values->count() < 1;
    }
    public function firstKey(){
        $map = clone $this->values;
        $map->ksort();
        $arrayKeys = array_keys($map->getArrayCopy());
        return $arrayKeys[0];
    }
    public function tailMap($key){
        $map = clone $this->values;
        $map->ksort();
        $array = $map->getArrayCopy();
        $arrayKeys = array_keys($array);

        $results = array();
        foreach($arrayKeys as $arrayKey){
            if($key <= $arrayKey){
                $results[$arrayKey] = $array[$arrayKey];
            }
        }
        return new self($results);
    }
}

class ConsistentHash {
    private $hashFunction;
    private $numberOfReplicas;
    private $circle;
    private $nodes = array();
    public function __construct(HashFunction $hashFunction, $numberOfReplicas){
        $this->hashFunction = $hashFunction;
        $this->numberOfReplicas = $numberOfReplicas;
        $this->circle = new TreeMap;
    }
    public function getNodes(){
        return $this->nodes;
    }
    public function add(Node $node){
        for($i = 0; $i < $this->numberOfReplicas; ++$i){
            $nodeKey = $this->hashFunction->hash($node->getName() . $i);
            $this->circle->put($nodeKey, $node);
        }
        $this->nodes[] = $node;
    }
    public function get($key){
        if($this->circle->isEmpty()){
            return null;
        }
        $hash = $this->hashFunction->hash($key);
        if(!$this->circle->has($hash)){
            $tailMap = $this->circle->tailMap($hash);
            if($tailMap->isEmpty()){
                $hash = $this->circle->firstKey();
            } else {
                $hash = $tailMap->firstKey();
            }
        }
        return $this->circle->get($hash);
    }
    public function remove(Node $node){
        for($i = 0; $i < $this->numberOfReplicas; ++$i){
            $nodeKey = $this->hashFunction->hash($node->getName() . $i);
            $this->circle->remove($nodeKey);
        }
    }
}

class ConsistentHashNode implements Node {
    private $hash;
    private $keys = array();
    public function __construct(ConsistentHash $hash){
        $this->hash = $hash;
    }
    public function put($key, $value){
        $this->hash->get($key)->put($key, $value);
        $this->keys[] = $key;
    }
    public function get($key){
        return $this->hash->get($key)->get($key);
    }
    public function has($key){
        return $this->hash->get($key)->has($key);
    }
    public function keys(){
        return $this->keys;
    }
    public function getName(){
        return __CLASS__;
    }
}

class IdentNode implements Node {
    private $name;
    private $values = array();
    public function __construct($name){
        $this->name = $name;
    }
    public function put($key, $value){
        $this->values[$key] = $value;
    }
    public function get($key){
        return $this->values[$key];
    }
    public function has($key){
        return isset($this->values);
    }
    public function keys(){
        return array_keys($this->values);
    }
    public function getName(){
        return $this->name;
    }
}

/*
$hash = new ConsistentHash(new HashMD5Integer, 8);
$hash->add(new IdentNode('hoge1'));
$hash->add(new IdentNode('hoge2'));
$hash->add(new IdentNode('hoge3'));
$map = new ConsistentHashNode($hash);
for($i = 0; $i < 10; ++$i){
    $map->put('key' . $i, 'value' . $i);
}
foreach($hash->getNodes() as $node){
    echo $node->getName(), ':', join(',', $node->keys()), PHP_EOL;
}

echo '------------------------', PHP_EOL;
$hashAlgos = array(
    new HashMD5Integer,
    new HashSha1Integer,
    new HashCRC32Integer
);
foreach($hashAlgos as $algo){
    echo get_class($algo), PHP_EOL;
    $hash = new ConsistentHash($algo, 10);
    for($i = 0; $i < 10; $i++){
       $hash->add(new IdentNode('hoge' . $i));
    }

    $map = new ConsistentHashNode($hash);
    for($i = 0; $i < 100; $i++){
        $map->put('*' . $i, $i);
    }

    $allKeys = $map->keys();
    foreach($hash->getNodes() as $node){
        $keys = $node->keys();
        echo 'node(', (count($keys) / count($allKeys)) * 100, '%):', $node->getName(), ', keys:', join(',', $keys), PHP_EOL;
    }
}

echo '------------------------', PHP_EOL;

$hash = new ConsistentHash(new HashMD5Integer, 32);
$hash->add(new IdentNode('hoge1'));
$hash->add(new IdentNode('hoge2'));
$hash->add(new IdentNode('hoge3'));

$map = new ConsistentHashNode($hash);
for($i = 0; $i < 10; ++$i){
    $map->put('key' . $i, 'value' . $i);
}
$hash->add(new IdentNode('hoge4'));
for($i = 10; $i < 20; ++$i){
    $map->put('key' . $i, 'value' . $i);
}
$hash->add(new IdentNode('hoge5'));
for($i = 30; $i < 40; ++$i){
    $map->put('key' . $i, 'value' . $i);
}

$allKeys = $map->keys();
foreach($hash->getNodes() as $node){
    $keys = $node->keys();
    echo 'node(', (count($keys) / count($allKeys)) * 100, '%):', $node->getName(), ', keys:', join(',', $keys), PHP_EOL;
}
 */

$hash = new ConsistentHash(new HashMD5Integer, 32);
$hash->add(new IdentNode('hoge1'));
$hash->add(new IdentNode('hoge2'));
$hash->add(new IdentNode('hoge3'));
$hash->add(new IdentNode('hoge4'));
$map = new ConsistentHashNode($hash);
for($i = 0; $i < 10; ++$i){
    $map->put(chr(65 + $i), $i);
}

foreach($hash->getNodes() as $node){
    echo $node->getName(), ':', join(',', $node->keys()), PHP_EOL;
}

