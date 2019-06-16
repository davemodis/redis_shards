# Redis Shards Clent
Redis client based on Yii2 Redis Connector.
Extends functional to use many redis servers as one (sharding) and support MGET, MSET, KEYS, Pipeline. 
From original Yii class removed events.

### How sharding works
The server (shard) is selected depending on the key. If the key contains digits at the end, they are used to select the shard, otherwise the key is converted into a number by the hash function CRC32. Then we take the remainder of dividing this number by the number of servers and get the shard number.

It can be useful if you use keys like `user:[id]`. For example, if you have 3 shards and you need to know on which of it stored user 123 you find it with `123 % 3 = 0`. 0 is index of shard.

MGET, MSET, KEYS and Pipeline works transporently in this mode: each shard will get only its keys.

MGET, MSET, KEYS separate all key on parts equal count of shards, and send its.

### Composer installation
`composer require davemodis/redis_shards`

### Manually installation
1. Clone this repo;
2. Include code in your project.
```
include 'RedisShardsClient.php';
include 'RedisShardsException.php';
```

### Simple configuration
Create object and configure it.
```
$redis = new \davemodis\redis_shards\RedisShardsClient
$redis->hostname = '127.0.0.1';
```

### Sharding configuration
To use sharding feature specify more than one Redis server.
In this case you must specify ports, passwords and databases for each host. You will get error if miss one of it.

```
$redis->hostname = ['127.0.0.1','127.0.0.2'];
$redis->port = [6379,6379];
$redis->password = [null,'StrOngPassw0rd!!1'];
$redis->database = [0,0];
```


### Yii2 configuration

