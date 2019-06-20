# Redis Shards Client
Redis client based on Yii2 Redis Connector. Extends functional to use few redis servers as one (sharding) and support MGET, MSET, KEYS, Pipeline. 
From original Yii class removed events.

### How sharding works
The server (shard) is selected depending on the key. If the key contains digits at the end, they are used to select the shard, otherwise the key is converted into a number with the hash function CRC32. Then we take the remainder of dividing this number on the count of servers and get the shard index.
It can be useful if you use keys like `user:[id]`. For example, if you have 3 shards and you need to know on which of them stores user 123 you can find it with `123 % 3 = 0`. 0 is index of shard.

### Features
**MGET, MSET, KEYS and Pipeline** works transporently: each shard will get only its keys.
MGET, MSET, KEYS separates all keys on packs for their shards, and then send requests. One shard - one request.

You can delay reading response from Redis. [Learn more about Pipeline on Redis.io](https://redis.io/topics/pipelining).
In sharding mode pipeline works the same as usual. 
When you start Pipeline mode `$redis->pipelineStart();`, client doesn't read response from Redis. When you end Pipeline mode `$redis->pipelineEnd();` client read all responses of each requests made in Pipeline mode.
If you don't need to read responses (for economy reason) you can close sockets with `$redis->close();`

## Installation
### Composer installation
`composer require davemodis/redis_shards`

### Manually installation
1. Clone this repo;
2. Include code in your project.
```
include 'RedisShardsClient.php';
include 'RedisShardsException.php';
```

## Configuration
### Simple configuration
Create object and configure it.
```
$redis = new \davemodis\redis_shards\RedisShardsClient();
$redis->hostname = '127.0.0.1';
```

### Sharding configuration
To use sharding feature specify more than one Redis server.
In this case you must specify ports, passwords and databases for each host. You will get error if miss one of them.

```
$redis->hostname = ['127.0.0.1','127.0.0.2'];
$redis->port = [6379,6379];
$redis->password = [null,'StrOngPassw0rd!!1'];
$redis->database = [0,0];
```

### Yii2 configuration
```
'components' => [
    'redis' => [
        'class'         => 'davemodis\redis_shards\RedisShardsClient',
        'hostname'      => ['127.0.0.1','127.0.0.2'],
        'port'          => [6379,6379],
        'password'      => [null,'StrOngPassw0rd!!1'],
        'database'      => [0,0],
    ],
]
```
> NOTICE: You can't use this class in Yii2 to Cache or Session.
> Add one more redis component in Yii2 config with 
> yii\redis\Connection class to use in Cache and Session.

```
'components' => [
    'redis' => [
        'class'         => 'davemodis\redis_shards\RedisShardsClient',
        'hostname'      => ['127.0.0.1','127.0.0.2'],
        'port'          => [6379,6379],
        'password'      => [null,'StrOngPassw0rd!!1'],
        'database'      => [0,0],
    ],
    'redis_yiicache' => [
        'class'         => 'yii\redis\Connection',
        'hostname'      => '127.0.0.2',
        'port'          => 6379,
        'password'      => 'StrOngPassw0rd!!1',
        'database'      => 1,
    ],
    'cache' => [
        'class' => 'yii\redis\Cache',
        'redis' => 'redis_yiicache',
    ],
    'session' => [
        'class' => 'yii\redis\Session',
        'redis' => 'redis_yiicache',
        'timeout' => 86400*30,
    ],
]
```
