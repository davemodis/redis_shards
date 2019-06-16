<?php

include 'RedisShardsClient.php';
include 'RedisShardsException.php';

$redis = new \davemodis\redis_shards\RedisShardsClient;

$redis->hostname = ['5.9.50.209','144.76.107.247'];
$redis->port = [6379,6379];
$redis->password = ['wlVnx58aN2Bx8wY3qeNDzy1VMYn0moVK','H6flp52cIZs1madZaDSZZK9jtlFd9hba'];
$redis->database = [0,0];

$redis->open();
var_dump($redis->keys('test*'));