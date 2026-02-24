## Redis Clone

### Basic Operations

* **PING**: `PING [message]`
* **ECHO**: `ECHO message`
* **SET**: `SET key value [NX|XX] [GET] [EX seconds|PX milliseconds]`
* **GET**: `GET key`
* **INCR**: `INCR key`
* **TYPE**: `TYPE key`
* **KEYS**: `KEYS pattern`

### List Commands

* **LPUSH**: `LPUSH key value [value ...]`
* **RPUSH**: `RPUSH key value [value ...]`
* **LPOP**: `LPOP key [count]`
* **BLPOP**: `BLPOP key [key ...] timeout`
* **LLEN**: `LLEN key`
* **LRANGE**: `LRANGE key start stop`

### Sorted Sets (ZSET)

* **ZADD**: `ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]`
* **ZREM**: `ZREM key member [member ...]`
* **ZRANK**: `ZRANK key member [WITHSCORE]`
* **ZRANGE**: `ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`
* **ZCARD**: `ZCARD key`
* **ZSCORE**: `ZSCORE key member`

### Streams

* **XADD**: `XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold [LIMIT count]] *|ID field value [field value ...]`
* **XRANGE**: `XRANGE key start end [COUNT count]`
* **XREAD**: `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]`

### Pub/Sub

* **SUBSCRIBE**: `SUBSCRIBE channel [channel ...]`
* **UNSUBSCRIBE**: `UNSUBSCRIBE [channel [channel ...]]`
* **PUBLISH**: `PUBLISH channel message`

### Transactions

* **MULTI**: `MULTI` (Starts transaction)
* **EXEC**: `EXEC` (Executes transaction)
* **DISCARD**: `DISCARD` (Aborts transaction)

### Geospatial

* **GEOADD**: `GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]`
* **GEOPOS**: `GEOPOS key member [member ...]`
* **GEODIST**: `GEODIST key member1 member2 [unit]`
* **GEOSEARCH**: `GEOSEARCH key FROMMEMBER member | FROMLONLAT lon lat BYRADIUS radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]`

### Server/Replication

* **INFO**: `INFO [section]`
* **CONFIG**: `CONFIG GET parameter` / `CONFIG SET parameter value`
* **WAIT**: `WAIT numreplicas timeout`
* **REPLCONF**: `REPLCONF <parameter> <value>` (Internal use, usually not manual)
* **PSYNC**: `PSYNC replicationid offset` (Internal use)

### Authorization

* **AUTH**: `AUTH [username] password`
* **WHOAMI**: `ACL WHOAMI`
* **GETUSER**: `ACL GETUSER username`
* **SETUSER**: `SETUSER username [rule [rule ...]]` 
