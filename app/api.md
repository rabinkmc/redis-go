## Redis Clone

### Basic Operations

* **PING**: `PING [message]` [X]
* **ECHO**: `ECHO message` [X]
* **SET**: `SET key value [NX|XX] [GET] [EX seconds|PX milliseconds]` [X]
* **GET**: `GET key` [X]
* **INCR**: `INCR key` [X]
* **TYPE**: `TYPE key` [X]
* **KEYS**: `KEYS pattern` [X]

### List Commands

* **LPUSH**: `LPUSH key value [value ...]` [X]
* **RPUSH**: `RPUSH key value [value ...]` [X]
* **LPOP**: `LPOP key [count]` [X]
* **BLPOP**: `BLPOP key [key ...] timeout` [X]
* **LLEN**: `LLEN key` [X]
* **LRANGE**: `LRANGE key start end` [X]

### Sorted Sets (ZSET)

* **ZADD**: `ZADD key score member` [X]
* **ZREM**: `ZREM key member` [X]
* **ZRANK**: `ZRANK key member` [X]
* **ZRANGE**: `ZRANGE key start end` [X]
* **ZCARD**: `ZCARD key` [X]
* **ZSCORE**: `ZSCORE key member` [X]

### Streams

* **XADD**: `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|ID field value [field value ...]` [X]
* **XRANGE**: `XRANGE key start end [COUNT count]` [X]
* **XREAD**: `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]` [-]

### Pub/Sub

* **SUBSCRIBE**: `SUBSCRIBE channel [channel ...]` [-]
* **UNSUBSCRIBE**: `UNSUBSCRIBE [channel [channel ...]]` [-]
* **PUBLISH**: `PUBLISH channel message` [-]

### Transactions

* **MULTI**: `MULTI` (Starts transaction) [-]
* **EXEC**: `EXEC` (Executes transaction) [-]
* **DISCARD**: `DISCARD` (Aborts transaction) [-]

### Geospatial

* **GEOADD**: `GEOADD key longitude latitude member` [X]
* **GEOPOS**: `GEOPOS key member` [X]
* **GEODIST**: `GEODIST key member1 member2 [unit]` [X]
* **GEOSEARCH**: `GEOSEARCH key long lat within [unit]` [X]

### Server/Replication

* **INFO**: `INFO [section]` [-]
* **CONFIG**: `CONFIG GET parameter` / `CONFIG SET parameter value` [-]
* **WAIT**: `WAIT numreplicas timeout` [-]
* **REPLCONF**: `REPLCONF <parameter> <value>` (Internal use, usually not manual) [-]
* **PSYNC**: `PSYNC replicationid offset` (Internal use) [-]

### Authorization

* **AUTH**: `AUTH [username] password` [-]
* **WHOAMI**: `ACL WHOAMI` [-]
* **GETUSER**: `ACL GETUSER username` [-]
* **SETUSER**: `SETUSER username [rule [rule ...]]` [-]
