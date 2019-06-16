<?php
/**
 * RedisShards - Redis client based on Yii2 RedisConnection Component
 * @link http://github.com/davemodis/redis_shards/
 * @license http://www.yiiframework.com/license/
 */

namespace davemodis\redis_shards;

use davemodis\redis_shards\RedisShardsException;

class RedisShardsClient 
{
    /**
     * @var string the hostname or ip address to use for connecting to the redis server. Defaults to ['localhost'].
     */
    public $hostname = ['localhost'];
    /**
     * @var integer the port to use for connecting to the redis server. Default port is 6379.
     */
    public $port = [6379];
    /**
     * @var string the password for establishing DB connection. Defaults to null meaning no AUTH command is sent.
     * See http://redis.io/commands/auth
     */
    public $password = [null];
    /**
     * @var integer the redis database to use. This is an integer value starting from 0. Defaults to 0.
     * Since version 2.0.6 you can disable the SELECT command sent after connection by setting this property to `null`.
     */
    public $database = [0];
    /**
     * @var float timeout to use for connection to redis. If not set the timeout set in php.ini will be used: `ini_get("default_socket_timeout")`.
     */
    public $connectionTimeout = null;
    /**
     * @var float timeout to use for redis socket when reading and writing data. If not set the php default value will be used.
     */
    public $dataTimeout = null;
    /**
     * @var function Callback function to log messages with one argument - message.
     */
    public $logCallback = null;
    /**
     * @var integer Bitmask field which may be set to any combination of connection flags passed to [stream_socket_client()](http://php.net/manual/en/function.stream-socket-client.php).
     * Currently the select of connection flags is limited to `STREAM_CLIENT_CONNECT` (default), `STREAM_CLIENT_ASYNC_CONNECT` and `STREAM_CLIENT_PERSISTENT`.
     *
     * > Warning: `STREAM_CLIENT_PERSISTENT` will make PHP reuse connections to the same server. If you are using multiple
     * > connection objects to refer to different redis [[$database|databases]] on the same [[port]], redis commands may
     * > get executed on the wrong database. `STREAM_CLIENT_PERSISTENT` is only safe to use if you use only one database.
     * >
     * > You may still use persistent connections in this case when disambiguating ports as described
     * > in [a comment on the PHP manual](http://php.net/manual/en/function.stream-socket-client.php#105393)
     * > e.g. on the connection used for session storage, specify the port as:
     * >
     * > ```php
     * > 'port' => '6379/session'
     * > ```
     *
     * @see http://php.net/manual/en/function.stream-socket-client.php
     * @since 2.0.5
     */
    public $socketClientFlags = STREAM_CLIENT_CONNECT;
    /**
     * @var integer The number of times a command execution should be retried when a connection failure occurs.
     * This is used in [[executeCommand()]] when a [[RedisShardsException]] is thrown.
     * Defaults to 0 meaning no retries on failure.
     * @since 2.0.7
     */
    public $retries = 0;
    /**
     * @var array List of available redis commands.
     * @see http://redis.io/commands
     */
    public $redisCommands = [
        'APPEND', // Append a value to a key
        'AUTH', // Authenticate to the server
        'BGREWRITEAOF', // Asynchronously rewrite the append-only file
        'BGSAVE', // Asynchronously save the dataset to disk
        'BITCOUNT', // Count set bits in a string
        'BITFIELD', // Perform arbitrary bitfield integer operations on strings
        'BITOP', // Perform bitwise operations between strings
        'BITPOS', // Find first bit set or clear in a string
        'BLPOP', // Remove and get the first element in a list, or block until one is available
        'BRPOP', // Remove and get the last element in a list, or block until one is available
        'BRPOPLPUSH', // Pop a value from a list, push it to another list and return it; or block until one is available
        'CLIENT KILL', // Kill the connection of a client
        'CLIENT LIST', // Get the list of client connections
        'CLIENT GETNAME', // Get the current connection name
        'CLIENT PAUSE', // Stop processing commands from clients for some time
        'CLIENT REPLY', // Instruct the server whether to reply to commands
        'CLIENT SETNAME', // Set the current connection name
        'CLUSTER ADDSLOTS', // Assign new hash slots to receiving node
        'CLUSTER COUNTKEYSINSLOT', // Return the number of local keys in the specified hash slot
        'CLUSTER DELSLOTS', // Set hash slots as unbound in receiving node
        'CLUSTER FAILOVER', // Forces a slave to perform a manual failover of its master.
        'CLUSTER FORGET', // Remove a node from the nodes table
        'CLUSTER GETKEYSINSLOT', // Return local key names in the specified hash slot
        'CLUSTER INFO', // Provides info about Redis Cluster node state
        'CLUSTER KEYSLOT', // Returns the hash slot of the specified key
        'CLUSTER MEET', // Force a node cluster to handshake with another node
        'CLUSTER NODES', // Get Cluster config for the node
        'CLUSTER REPLICATE', // Reconfigure a node as a slave of the specified master node
        'CLUSTER RESET', // Reset a Redis Cluster node
        'CLUSTER SAVECONFIG', // Forces the node to save cluster state on disk
        'CLUSTER SETSLOT', // Bind a hash slot to a specific node
        'CLUSTER SLAVES', // List slave nodes of the specified master node
        'CLUSTER SLOTS', // Get array of Cluster slot to node mappings
        'COMMAND', // Get array of Redis command details
        'COMMAND COUNT', // Get total number of Redis commands
        'COMMAND GETKEYS', // Extract keys given a full Redis command
        'COMMAND INFO', // Get array of specific Redis command details
        'CONFIG GET', // Get the value of a configuration parameter
        'CONFIG REWRITE', // Rewrite the configuration file with the in memory configuration
        'CONFIG SET', // Set a configuration parameter to the given value
        'CONFIG RESETSTAT', // Reset the stats returned by INFO
        'DBSIZE', // Return the number of keys in the selected database
        'DEBUG OBJECT', // Get debugging information about a key
        'DEBUG SEGFAULT', // Make the server crash
        'DECR', // Decrement the integer value of a key by one
        'DECRBY', // Decrement the integer value of a key by the given number
        'DEL', // Delete a key
        'DISCARD', // Discard all commands issued after MULTI
        'DUMP', // Return a serialized version of the value stored at the specified key.
        'ECHO', // Echo the given string
        'EVAL', // Execute a Lua script server side
        'EVALSHA', // Execute a Lua script server side
        'EXEC', // Execute all commands issued after MULTI
        'EXISTS', // Determine if a key exists
        'EXPIRE', // Set a key's time to live in seconds
        'EXPIREAT', // Set the expiration for a key as a UNIX timestamp
        'FLUSHALL', // Remove all keys from all databases
        'FLUSHDB', // Remove all keys from the current database
        'GEOADD', // Add one or more geospatial items in the geospatial index represented using a sorted set
        'GEOHASH', // Returns members of a geospatial index as standard geohash strings
        'GEOPOS', // Returns longitude and latitude of members of a geospatial index
        'GEODIST', // Returns the distance between two members of a geospatial index
        'GEORADIUS', // Query a sorted set representing a geospatial index to fetch members matching a given maximum distance from a point
        'GEORADIUSBYMEMBER', // Query a sorted set representing a geospatial index to fetch members matching a given maximum distance from a member
        'GET', // Get the value of a key
        'GETBIT', // Returns the bit value at offset in the string value stored at key
        'GETRANGE', // Get a substring of the string stored at a key
        'GETSET', // Set the string value of a key and return its old value
        'HDEL', // Delete one or more hash fields
        'HEXISTS', // Determine if a hash field exists
        'HGET', // Get the value of a hash field
        'HGETALL', // Get all the fields and values in a hash
        'HINCRBY', // Increment the integer value of a hash field by the given number
        'HINCRBYFLOAT', // Increment the float value of a hash field by the given amount
        'HKEYS', // Get all the fields in a hash
        'HLEN', // Get the number of fields in a hash
        'HMGET', // Get the values of all the given hash fields
        'HMSET', // Set multiple hash fields to multiple values
        'HSET', // Set the string value of a hash field
        'HSETNX', // Set the value of a hash field, only if the field does not exist
        'HSTRLEN', // Get the length of the value of a hash field
        'HVALS', // Get all the values in a hash
        'INCR', // Increment the integer value of a key by one
        'INCRBY', // Increment the integer value of a key by the given amount
        'INCRBYFLOAT', // Increment the float value of a key by the given amount
        'INFO', // Get information and statistics about the server
        'KEYS', // Find all keys matching the given pattern
        'LASTSAVE', // Get the UNIX time stamp of the last successful save to disk
        'LINDEX', // Get an element from a list by its index
        'LINSERT', // Insert an element before or after another element in a list
        'LLEN', // Get the length of a list
        'LPOP', // Remove and get the first element in a list
        'LPUSH', // Prepend one or multiple values to a list
        'LPUSHX', // Prepend a value to a list, only if the list exists
        'LRANGE', // Get a range of elements from a list
        'LREM', // Remove elements from a list
        'LSET', // Set the value of an element in a list by its index
        'LTRIM', // Trim a list to the specified range
        'MGET', // Get the values of all the given keys
        'MSET', // Set the values for all the given keys
        'MIGRATE', // Atomically transfer a key from a Redis instance to another one.
        'MONITOR', // Listen for all requests received by the server in real time
        'MOVE', // Move a key to another database
        'MSET', // Set multiple keys to multiple values
        'MSETNX', // Set multiple keys to multiple values, only if none of the keys exist
        'MULTI', // Mark the start of a transaction block
        'OBJECT', // Inspect the internals of Redis objects
        'PERSIST', // Remove the expiration from a key
        'PEXPIRE', // Set a key's time to live in milliseconds
        'PEXPIREAT', // Set the expiration for a key as a UNIX timestamp specified in milliseconds
        'PFADD', // Adds the specified elements to the specified HyperLogLog.
        'PFCOUNT', // Return the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).
        'PFMERGE', // Merge N different HyperLogLogs into a single one.
        'PING', // Ping the server
        'PSETEX', // Set the value and expiration in milliseconds of a key
        'PSUBSCRIBE', // Listen for messages published to channels matching the given patterns
        'PUBSUB', // Inspect the state of the Pub/Sub subsystem
        'PTTL', // Get the time to live for a key in milliseconds
        'PUBLISH', // Post a message to a channel
        'PUNSUBSCRIBE', // Stop listening for messages posted to channels matching the given patterns
        'QUIT', // Close the connection
        'RANDOMKEY', // Return a random key from the keyspace
        'READONLY', // Enables read queries for a connection to a cluster slave node
        'READWRITE', // Disables read queries for a connection to a cluster slave node
        'RENAME', // Rename a key
        'RENAMENX', // Rename a key, only if the new key does not exist
        'RESTORE', // Create a key using the provided serialized value, previously obtained using DUMP.
        'ROLE', // Return the role of the instance in the context of replication
        'RPOP', // Remove and get the last element in a list
        'RPOPLPUSH', // Remove the last element in a list, prepend it to another list and return it
        'RPUSH', // Append one or multiple values to a list
        'RPUSHX', // Append a value to a list, only if the list exists
        'SADD', // Add one or more members to a set
        'SAVE', // Synchronously save the dataset to disk
        'SCARD', // Get the number of members in a set
        'SCRIPT DEBUG', // Set the debug mode for executed scripts.
        'SCRIPT EXISTS', // Check existence of scripts in the script cache.
        'SCRIPT FLUSH', // Remove all the scripts from the script cache.
        'SCRIPT KILL', // Kill the script currently in execution.
        'SCRIPT LOAD', // Load the specified Lua script into the script cache.
        'SDIFF', // Subtract multiple sets
        'SDIFFSTORE', // Subtract multiple sets and store the resulting set in a key
        'SELECT', // Change the selected database for the current connection
        'SET', // Set the string value of a key
        'SETBIT', // Sets or clears the bit at offset in the string value stored at key
        'SETEX', // Set the value and expiration of a key
        'SETNX', // Set the value of a key, only if the key does not exist
        'SETRANGE', // Overwrite part of a string at key starting at the specified offset
        'SHUTDOWN', // Synchronously save the dataset to disk and then shut down the server
        'SINTER', // Intersect multiple sets
        'SINTERSTORE', // Intersect multiple sets and store the resulting set in a key
        'SISMEMBER', // Determine if a given value is a member of a set
        'SLAVEOF', // Make the server a slave of another instance, or promote it as master
        'SLOWLOG', // Manages the Redis slow queries log
        'SMEMBERS', // Get all the members in a set
        'SMOVE', // Move a member from one set to another
        'SORT', // Sort the elements in a list, set or sorted set
        'SPOP', // Remove and return one or multiple random members from a set
        'SRANDMEMBER', // Get one or multiple random members from a set
        'SREM', // Remove one or more members from a set
        'STRLEN', // Get the length of the value stored in a key
        'SUBSCRIBE', // Listen for messages published to the given channels
        'SUNION', // Add multiple sets
        'SUNIONSTORE', // Add multiple sets and store the resulting set in a key
        'SWAPDB', // Swaps two Redis databases
        'SYNC', // Internal command used for replication
        'TIME', // Return the current server time
        'TOUCH', // Alters the last access time of a key(s). Returns the number of existing keys specified.
        'TTL', // Get the time to live for a key
        'TYPE', // Determine the type stored at key
        'UNSUBSCRIBE', // Stop listening for messages posted to the given channels
        'UNLINK', // Delete a key asynchronously in another thread. Otherwise it is just as DEL, but non blocking.
        'UNWATCH', // Forget about all watched keys
        'WAIT', // Wait for the synchronous replication of all the write commands sent in the context of the current connection
        'WATCH', // Watch the given keys to determine execution of the MULTI/EXEC block
        'ZADD', // Add one or more members to a sorted set, or update its score if it already exists
        'ZCARD', // Get the number of members in a sorted set
        'ZCOUNT', // Count the members in a sorted set with scores within the given values
        'ZINCRBY', // Increment the score of a member in a sorted set
        'ZINTERSTORE', // Intersect multiple sorted sets and store the resulting sorted set in a new key
        'ZLEXCOUNT', // Count the number of members in a sorted set between a given lexicographical range
        'ZRANGE', // Return a range of members in a sorted set, by index
        'ZRANGEBYLEX', // Return a range of members in a sorted set, by lexicographical range
        'ZREVRANGEBYLEX', // Return a range of members in a sorted set, by lexicographical range, ordered from higher to lower strings.
        'ZRANGEBYSCORE', // Return a range of members in a sorted set, by score
        'ZRANK', // Determine the index of a member in a sorted set
        'ZREM', // Remove one or more members from a sorted set
        'ZREMRANGEBYLEX', // Remove all members in a sorted set between the given lexicographical range
        'ZREMRANGEBYRANK', // Remove all members in a sorted set within the given indexes
        'ZREMRANGEBYSCORE', // Remove all members in a sorted set within the given scores
        'ZREVRANGE', // Return a range of members in a sorted set, by index, with scores ordered from high to low
        'ZREVRANGEBYSCORE', // Return a range of members in a sorted set, by score, with scores ordered from high to low
        'ZREVRANK', // Determine the index of a member in a sorted set, with scores ordered from high to low
        'ZSCORE', // Get the score associated with the given member in a sorted set
        'ZUNIONSTORE', // Add multiple sorted sets and store the resulting sorted set in a new key
        'SCAN', // Incrementally iterate the keys space
        'SSCAN', // Incrementally iterate Set elements
        'HSCAN', // Incrementally iterate hash fields and associated values
        'ZSCAN', // Incrementally iterate sorted sets elements and associated scores
    ];

    /**
     * @var resource current redis socket connection
     */
    private $_socket = false;

    /**
     * @var array of resources redis socket connections
     */
    private $_storage = [];

    /**
     * @var flag for pipeline mode
     */
    private $_pipeline = false;

    /**
     * @var order of pipeline requests to shards
     */
    private $_pipeline_order = [];


    /**
     * Closes the connection when this component is being serialized.
     * @return array
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }

    /**
     * Returns a value indicating whether the DB connection is established.
     * @return bool whether the DB connection is established
     */
    public function getIsActive($index = -1)
    {
        if($index === -1)
            return $this->_socket !== false;

        return $this->_storage[$index] !== false;
    }

    /**
     * Establishes a DB connection.
     * It does nothing if a DB connection has already been established.
     * @throws RedisShardsException if connection fails
     */
    public function open( $index = 0 )
    {
        

        if( !is_array($this->hostname) )
            $this->hostname = [$this->hostname];
        if( !is_array($this->port) )
            $this->port = [$this->port];
        if( !is_array($this->password) )
            $this->password = [$this->password];
        if( !is_array($this->database) )
            $this->database = [$this->database];

        if( count($this->hostname) !== count($this->port) )
            throw new RedisShardsException("Count of ports in config not equal to count of hosts");

        if( count($this->hostname) !== count($this->password) )
            throw new RedisShardsException("Count of passwords in config not equal to count of hosts");

        if( count($this->hostname) !== count($this->database) )
            throw new RedisShardsException("Count of databases in config not equal to count of hosts");

        if( count($this->_storage) < count($this->hostname) )
        {
            $this->_storage = [];
            foreach ($this->hostname as $v) {
                $this->_storage[] = false;
            }
        }

        if( !isset($this->_storage[$index]) )
            throw new RedisShardsException("Failed to open connection. No host with index $index in config");

        if($this->_storage[$index] === false)
        {
            $connection = $this->hostname[$index] . ':' . $this->port[$index] . ', database=' . $this->database[$index];
            $this->logger('Opening redis DB connection: ' . $connection, __METHOD__);
            $this->_storage[$index] = @stream_socket_client(
                'tcp://' . $this->hostname[$index] . ':' . $this->port[$index],
                $errorNumber,
                $errorDescription,
                $this->connectionTimeout ? $this->connectionTimeout : ini_get('default_socket_timeout'),
                $this->socketClientFlags
            );
            if($this->_storage[$index])
                $this->_socket = &$this->_storage[$index];
            else
                $this->_socket = false;

            if ($this->_socket) {
                if ($this->dataTimeout !== null) {
                    stream_set_timeout($this->_socket, $timeout = (int) $this->dataTimeout, (int) (($this->dataTimeout - $timeout) * 1000000));
                }
                if ($this->password[$index]) {
                    $this->executeCommand('AUTH', [$this->password[$index]], $index);
                    if($this->_pipeline) {
                        $this->parseResponse('PIPELINE');
                    }
                }
                if ($this->database[$index]) {
                    $this->executeCommand('SELECT', [$this->database[$index]], $index);
                    if($this->_pipeline) {
                        $this->parseResponse('PIPELINE');
                    }
                }
            } else {
                $this->logger("Failed to open redis DB connection ($connection): $errorNumber - $errorDescription", __CLASS__);
                $message = "Failed to open redis DB connection ($connection): $errorNumber - $errorDescription";
                throw new RedisShardsException($message);
            }
        }
        else
            $this->_socket = &$this->_storage[$index];

    }

    /**
     * Closes the currently active DB connection.
     * It does nothing if the connection is already closed.
     */
    public function close()
    {
        $this->_pipeline = false;
        $this->_pipeline_order = [];

        foreach ($this->_storage as $i => &$conn) {
            $this->_socket = &$conn;
            if ($this->_socket !== false) {
                $connection = $this->hostname[$i] . ':' . $this->port[$i] . ', database=' . $this->database[$i];
                $this->logger('Closing DB connection: ' . $connection, __METHOD__);
                try {
                    $this->executeCommand('QUIT');
                } catch (RedisShardsException $e) {
                    // ignore errors when quitting a closed connection
                }
                fclose($this->_socket);
                $this->_socket = false;
                $this->_storage[$i] = false;
            }
        }
    }

    /**
     * Returns the name of the DB driver for the current [[dsn]].
     * @return string name of the DB driver
     */
    public function getDriverName()
    {
        return 'redis';
    }

    /**
     * @return LuaScriptBuilder
     */
    public function getLuaScriptBuilder()
    {
        return new LuaScriptBuilder();
    }

    /**
     * Allows issuing all supported commands via magic methods.
     *
     * ```php
     * $redis->hmset('test_collection', 'key1', 'val1', 'key2', 'val2')
     * ```
     *
     * @param string $name name of the missing method to execute
     * @param array $params method call arguments
     * @return mixed
     */
    public function __call($name, $params)
    {
        $redisCommand = strtoupper($this->camel2words($name, false));
        if (in_array($redisCommand, $this->redisCommands)) {
            return $this->executeCommand($redisCommand, $params);
        }
    }

    /**
     * Executes a redis command.
     * For a list of available commands and their parameters see http://redis.io/commands.
     *
     * The params array should contain the params separated by white space, e.g. to execute
     * `SET mykey somevalue NX` call the following:
     *
     * ```php
     * $redis->executeCommand('SET', ['mykey', 'somevalue', 'NX']);
     * ```
     *
     * @param string $name the name of the command
     * @param array $params list of parameters for the command
     * @return array|bool|null|string Dependent on the executed command this method
     * will return different data types:
     *
     * - `true` for commands that return "status reply" with the message `'OK'` or `'PONG'`.
     * - `string` for commands that return "status reply" that does not have the message `OK` (since version 2.0.1).
     * - `string` for commands that return "integer reply"
     *   as the value is in the range of a signed 64 bit integer.
     * - `string` or `null` for commands that return "bulk reply".
     * - `array` for commands that return "Multi-bulk replies".
     *
     * See [redis protocol description](http://redis.io/topics/protocol)
     * for details on the mentioned reply types.
     * @throws RedisShardsException for commands that return [error reply](http://redis.io/topics/protocol#error-reply).
     */
    public function executeCommand($name, $params = [], $index = -1)
    {

        if( count($this->hostname) > 1 )
        {

            if($name === 'MGET' && $index < 0)
                return $this->_mget($params);


            if($name === 'MSET' && $index < 0)
                return $this->_mset($params);


            if($name === 'KEYS' && $index < 0)
            {
                $arr = [];
                for($i = 0; $i < count($this->hostname); ++$i)
                {
                    $arr = array_merge($arr,$this->executeCommand($name,$params,$i));
                }
                return $arr;
            }

        }

        if(!array_search($name, ['0','AUTH','SELECT']))
        {
            $key = count($params) ? $params[0] : substr(strstr($name, ' '), 1);

            if(strstr($key,' ')) {
                $key = strstr($key, ' ', true);
            }

            $index = $index < 0 ? $this->getServerByKey( $key ) : $index;
            $this->open( $index );

            if($this->_pipeline) {
                $this->_pipeline_order[] = $index;
            }
        }
        else
            $key = '';

        $params = array_merge(explode(' ', $name), $params);
        $command = '*' . count($params) . "\r\n";
        foreach ($params as $arg) {
            $command .= '$' . mb_strlen($arg, '8bit') . "\r\n" . $arg . "\r\n";
        }        

        $this->logger("Querying Redis Server[$index]: {$name} $key", __METHOD__);

        if ($this->retries > 0) {
            $tries = $this->retries;
            while ($tries-- > 0) {
                try {
                    return $this->sendCommandInternal($command, $params);
                } catch (RedisShardsException $e) {
                    $this->logger($e, __METHOD__, true);
                    
                    // backup retries, fail on commands that fail inside here
                    $retries = $this->retries;
                    $this->retries = 0;
                    $this->close();
                    $this->open();
                    $this->retries = $retries;
                }
            }
        }
        return $this->sendCommandInternal($command, $params);
    }

    /**
     * Sends RAW command string to the server.
     * @throws RedisShardsException on connection error.
     */
    private function sendCommandInternal($command, $params)
    {
        $written = @fwrite($this->_socket, $command);
        if ($written === false) {
            throw new RedisShardsException("Failed to write to socket.\nRedis command was: " . $command);
        }
        if ($written !== ($len = mb_strlen($command, '8bit'))) {
            throw new RedisShardsException("Failed to write to socket. $written of $len bytes written.\nRedis command was: " . $command);
        }

        if($params[0] === 'AUTH')
            $params[1] = '[hidden]';

        return $this->_pipeline ? true : $this->parseResponse(implode(' ', $params));
    }

    /**
     * @param string $command
     * @return mixed
     * @throws RedisShardsException on error
     */
    private function parseResponse($command)
    {
        if ( ($line = fgets($this->_socket)) === false) {
            throw new RedisShardsException("Failed to read from socket.\nRedis command was: " . $command);
        }

        // return $line;
        
        $type = $line[0];
        $line = mb_substr($line, 1, -2, '8bit');
        switch ($type) {
            case '+': // Status reply
                if ($line === 'OK' || $line === 'PONG') {
                    return true;
                } else {
                    return $line;
                }
            case '-': // Error reply
                throw new RedisShardsException("Redis error: " . $line . "\nRedis command was: " . $command);
            case ':': // Integer reply
                // no cast to int as it is in the range of a signed 64 bit integer
                return $line;
            case '$': // Bulk replies
                if ($line == '-1') {
                    return null;
                }
                $length = (int)$line + 2;
                $data = '';
                while ($length > 0) {
                    if (($block = fread($this->_socket, $length)) === false) {
                        throw new RedisShardsException("Failed to read from socket.\nRedis command was: " . $command);
                    }
                    $data .= $block;
                    $length -= mb_strlen($block, '8bit');
                }

                return mb_substr($data, 0, -2, '8bit');
            case '*': // Multi-bulk replies
                $count = (int) $line;

                if( $count === -1 )
                    return null;

                $data = [];
                for ($i = 0; $i < $count; $i++) {
                    $data[] = $this->parseResponse($command);
                }

                return $data;
            default:
                if($this->_pipeline)
                    return null;
                throw new RedisShardsException('Received illegal data from redis: ' . $line . "\nRedis command was: " . $command);
        }
    }

    /**
     * Select shard by key name.
     * Shard selects by modulo servers count from last digits or crc32 hash of key. 
     * @param string $key
     * @return integer Index of shard
     */
    public function getServerByKey($key)
    {
        if(count($this->hostname) === 1) {
            return 0;
        }

        $int = '';
        for($i = strlen($key)-1; $i >= 0; --$i) {
            if(is_numeric($key[$i])) {
                $int = $key[$i].$int;
            }
            else {
                break;
            }
        }

        if(!is_numeric($int)) {
            $int = crc32($key);
            if($int < 0) 
                $int = $int * -1;
        }
        else {
            $int = (int)$int;
        }

        return $int % count($this->hostname);
    }

    /**
     * Implement multi-get request to shards and combine response in keys order.
     * @param array $keys Keys to multi-get
     * @return array Array of values in order of keys
     */
    private function _mget($keys)
    {
        $REDIS_CNT = count($this->hostname);
        $srv = [];
        $data = [];

        if(count($keys) === 1 && is_array($keys[0])) {
            $keys = $keys[0];
        }

        // определяем нужный сервер по последним цифрам ключа
        // или по числовому представленю строки
        foreach ($keys as $k) {
            $data[$k] = null;
            $srv[] = $this->getServerByKey($k);
        }

        $srvs = array_pad([], $REDIS_CNT, []);

        foreach ($srv as $k => $s) {
            $srvs[$s][] = $keys[$k];
        }
      
        foreach ($srvs as $sn => $v) {
            if(!empty($v)) {
                $add = $this->executeCommand('MGET',$v,$sn);
                
                if(is_array( $add )) {
                    for( $i = 0; $i < count($v); ++$i )
                        $data[$v[$i]] = $add[$i];
                }
            }
        }

        $result = [];
        foreach ($keys as $k) {
            $result[] = $data[$k];
        }

        return $result;
    }

    /**
     * Implement multi-set request to shards.
     * @param array $keys Array of keys and values to set
     * @return bool Return true on finish
     */
    private function _mset($keys)
    {
        $REDIS_CNT = count($this->hostname);
        $srv = [];
        $val = [];
        $key = [];

        if(count($keys) === 1 && is_array($keys[0])) {
            $keys = $keys[0];
        }

        for ( $i=0; $i < count($keys); $i+=2 ) {
            $key[] = $keys[$i];
            $val[$keys[$i]] = $keys[$i + 1];
        }

        // определяем нужный сервер по последним цифрам ключа
        // или по числовому представленю строки
        foreach ($key as $k) {
            $srv[] = $this->getServerByKey($k);
        }

        $srvs = array_pad(array(), $REDIS_CNT, array());

        foreach ($srv as $k => $s) {
            $srvs[$s][] = $key[$k];
        }
      

        foreach ($srvs as $sn => $v) {
            if(!empty($v)) {
                $values = [];

                foreach ($v as $k) {
                    $values[] = $k;
                    $values[] = $val[$k];
                }

                $this->executeCommand('MSET',$values,$sn);
            }
        }

        return true;
    }

    /**
     * Enable pipeline mode.
     * In pipeline mode all requests executed without reading of response.
     */
    public function pipelineStart()
    {
        $this->_pipeline = true;
    }

    /**
     * Disable pipeline mode and read responses for all requests.
     * @return array
     */
    public function pipelineEnd()
    {
        $result = [];

        foreach ($this->_pipeline_order as $i) {
            $this->_socket = &$this->_storage[$i];

            $r = $this->parseResponse('PIPELINE');
            $result[] = $r;
        }
        
        $this->_pipeline = false;
        $this->_pipeline_order = [];

        return $result;
    }

    /**
     * Sends log messages to callback function or Yii::trace if exists
     * @param string $msg Log message
     * @param string $method Method of log message from
     * @param bool $is_error Send log message to Yii::error
     */
    private function logger($msg, $method, $is_error = false)
    {
        if($this->logCallback !== null && is_callable($this->logCallback)) {
            $this->logCallback($msg.' IN '.$method);
        }


        if(!$is_error && method_exists('Yii','trace')) {
            \Yii::trace($msg, $method);
        }

        if($is_error && method_exists('Yii','error')) {
            \Yii::error($msg, $method);
        }
    }

    /**
     * Converts a CamelCase name into space-separated words.
     * @param string $name The string to be converted
     * @return string The resulting words
     */
    private function camel2words($name)
    {
        return mb_strtolower(trim(str_replace([
            '-',
            '_',
            '.',
        ], ' ', preg_replace('/(?<!\p{Lu})(\p{Lu})|(\p{Lu})(?=\p{Ll})/u', ' \0', $name))));
    }
}
