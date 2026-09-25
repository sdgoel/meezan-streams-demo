# Redis Enterprise Streams + JSON + Hashes

A Java 21 / Spring Boot service demonstrating:

- concurrent consumption from multiple Redis Streams with consumer groups;
- process-first semantics, explicit `XACK`, and optional `XDEL` after a successful acknowledgement;
- Redis String create/read/update/delete, optional TTLs, and atomic counters;
- native RedisJSON create/read/update/delete and numeric increments;
- Redis Hash field writes, reads, multi-field reads, and deletes;
- TLS, username/password, explicit connect and command timeouts, health checks, and integration tests.

## Prerequisites

- Java 21 and Maven 3.9+, or Docker;
- Redis Enterprise with the JSON capability enabled. The included local environment uses Redis Stack, which provides compatible Redis Streams, Hash, and JSON commands.

## Run locally

With Docker only:

```bash
docker compose up --build
```

Or, with Java 21 and Maven installed:

```bash
docker compose up -d redis
mvn spring-boot:run
```

The API listens on `http://localhost:8080`. Health is at `/actuator/health`.

## Connect to Redis Enterprise

Set environment variables; never commit credentials:

```bash
export REDIS_HOST=your-endpoint.example.com
export REDIS_PORT=12000
export REDIS_USERNAME=default
export REDIS_PASSWORD='replace-me'
export REDIS_SSL=true
export REDIS_STREAMS=t24_customer_events,t24_currency_events,t24_cusrtomer_events
export REDIS_STREAM_GROUP=t24-projector
export REDIS_STREAM_CONSUMER=t24-projector-1
export T24_CUSTOMER_C_VALUES=c176,c178
export T24_CURRENCY_C_VALUES=c1,c2
export T24_CUSRTOMER_C_VALUES=c176,c178
mvn spring-boot:run
```

The consumer name must be unique per running application instance. A unique value is generated when the variable is omitted. Stream keys use lowercase colon-separated names. Consumer groups are created idempotently with `XGROUP CREATE ... 0-0 MKSTREAM`, so existing messages are consumed on first deployment.

### Stream delivery semantics

1. Redis delivers a record to the configured consumer group.
2. `StreamRecordHandler` passes the record to `StreamProcessorRouter`.
3. The router selects one `StreamSpecificProcessor` from the concrete stream key.
4. On success, the record is acknowledged (`XACK`).
5. If `REDIS_STREAM_DELETE_AFTER_ACK=true`, it is then removed (`XDEL`).
6. On processing failure it is not acknowledged or deleted, and remains in the group's pending-entry list.

`XDEL` removes the record globally, including for other groups. Set `REDIS_STREAM_DELETE_AFTER_ACK=false` when more than one consumer group needs the same stream or when the stream is an audit log. Configure Redis stream trimming/retention separately for that case.

The three streams use independently configurable XML-to-Hash projections:

| Stream | Selected fields | Output key example |
|---|---|---|
| `t24_customer_events` | `T24_CUSTOMER_C_VALUES` | `t24_customer_100011_c176_c178` |
| `t24_currency_events` | `T24_CURRENCY_C_VALUES` | `t24_currency_USD_c1_c2` |
| `t24_cusrtomer_events` | `T24_CUSRTOMER_C_VALUES` | `t24_cusrtomer_100011_c176_c178` |

`t24_cusrtomer_events` is intentionally configured exactly as supplied; rename `T24_CUSRTOMER_STREAM` and its hash prefix if that spelling is accidental.

For each message, `XmlHashStreamProcessor` reads `doc`, validates its `<row id='...'>`, extracts the configured XML elements, and writes one Redis Hash. A normal single element is stored as a plain string. Repeated elements such as `c178 m='12'` and `c178 m='13'` are stored in the `c178` hash field as a JSON array so neither their `m` attributes nor values are lost. A configured element that is absent from the XML is stored as an empty string.

Example output:

```text
key: t24_customer_100011_c176_c178

row_id = 100011
c176   = ""
c178   = [{"m":"12","value":"ELAHI BUKHSH"},{"m":"13","value":"UMER JAHAN"}]
```

The Hash write is idempotent: redelivery writes the same key and fields. Only after the Hash write succeeds does the common handler acknowledge and delete the Stream entry. Invalid XML, a missing `doc`, or conflicting `row_id` leaves the message pending and logs the error. XML DTDs and external entities are disabled.

## API examples

### Publish to either configured stream

```bash
curl -i -X POST http://localhost:8080/api/v1/streams/events:orders/messages \
  -H 'Content-Type: application/json' \
  -d '{"fields":{"event":"order-created","orderId":"1001"}}'

curl -i -X POST http://localhost:8080/api/v1/streams/events:payments/messages \
  -H 'Content-Type: application/json' \
  -d '{"fields":{"event":"payment-received","orderId":"1001"}}'
```

### Redis String CRUD and counters

```bash
# Create or replace a String (SET)
curl -i -X PUT http://localhost:8080/api/v1/strings/config/greeting \
  -H 'Content-Type: application/json' -d '{"value":"hello"}'

# Set a String with a 60-second TTL
curl -i -X PUT 'http://localhost:8080/api/v1/strings/session/abc123?ttlSeconds=60' \
  -H 'Content-Type: application/json' -d '{"value":"active"}'

# Read a String (GET)
curl http://localhost:8080/api/v1/strings/config/greeting

# Atomic increment or decrement (INCRBY)
curl -X POST 'http://localhost:8080/api/v1/strings/counter/visits/increment?amount=5'
curl -X POST 'http://localhost:8080/api/v1/strings/counter/visits/increment?amount=-1'

# Delete a String (DEL)
curl -i -X DELETE http://localhost:8080/api/v1/strings/config/greeting
```

### Redis Hash CRUD

```bash
# Create/update several fields (HSET)
curl -i -X PUT http://localhost:8080/api/v1/hashes/customer/42 \
  -H 'Content-Type: application/json' \
  -d '{"fields":{"name":"Ada","tier":"gold"}}'

# Read one field (HGET)
curl http://localhost:8080/api/v1/hashes/customer/42/fields/name

# Read selected fields only (HMGET; avoids an unbounded HGETALL)
curl 'http://localhost:8080/api/v1/hashes/customer/42?fields=name,tier'

# Update one field
curl -i -X PUT http://localhost:8080/api/v1/hashes/customer/42/fields/tier \
  -H 'Content-Type: application/json' -d '{"value":"platinum"}'

# Delete one field, then the whole hash
curl -i -X DELETE http://localhost:8080/api/v1/hashes/customer/42/fields/tier
curl -i -X DELETE http://localhost:8080/api/v1/hashes/customer/42
```

### RedisJSON CRUD

RedisJSON paths use JSONPath syntax and should be URL-encoded when they contain special characters.

```bash
# Set the document (JSON.SET)
curl -i -X PUT 'http://localhost:8080/api/v1/json/profile/42' \
  -H 'Content-Type: application/json' \
  -d '{"name":"Ada","visits":1,"address":{"city":"Dubai"}}'

# Get the document or one path (JSON.GET)
curl 'http://localhost:8080/api/v1/json/profile/42'
curl 'http://localhost:8080/api/v1/json/profile/42?path=$.address.city'

# Replace one path
curl -i -X PUT 'http://localhost:8080/api/v1/json/profile/42?path=$.address.city' \
  -H 'Content-Type: application/json' -d '"Abu Dhabi"'

# Atomic numeric increment
curl -X POST 'http://localhost:8080/api/v1/json/profile/42/increment?path=$.visits&amount=2'

# Delete one path or the entire document
curl -i -X DELETE 'http://localhost:8080/api/v1/json/profile/42?path=$.address'
curl -i -X DELETE 'http://localhost:8080/api/v1/json/profile/42'
```

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `REDIS_HOST` | `localhost` | Redis endpoint |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_USERNAME` | `default` | ACL username |
| `REDIS_PASSWORD` | empty | ACL password |
| `REDIS_SSL` | `false` | Enable TLS for Redis Enterprise |
| `REDIS_CONNECT_TIMEOUT` | `2s` | TCP connection timeout |
| `REDIS_COMMAND_TIMEOUT` | `5s` | Command timeout |
| `REDIS_STREAMS` | the three T24 streams above | Comma-separated streams |
| `REDIS_STREAM_GROUP` | `demo-service` | Consumer group |
| `REDIS_STREAM_CONSUMER` | generated | Unique instance consumer name |
| `REDIS_STREAM_POLL_TIMEOUT` | `2s` | Blocking stream read timeout |
| `REDIS_STREAM_DELETE_AFTER_ACK` | `true` | Delete records after successful processing |
| `T24_CUSTOMER_C_VALUES` | `c176,c178` | XML elements projected for `t24_customer_events` |
| `T24_CURRENCY_C_VALUES` | `c1,c2` | XML elements projected for `t24_currency_events` |
| `T24_CUSRTOMER_C_VALUES` | `c176,c178` | XML elements projected for `t24_cusrtomer_events` |

## Test and package

Integration tests start an isolated Redis Stack container and verify String CRUD/counters, Hash CRUD, JSON path operations, and consumption/acknowledgement/deletion on two streams.

```bash
mvn clean verify
docker build -t redis-enterprise-streams:local .
```

## Production notes

- Use a dedicated Redis Enterprise ACL user. Grant only `~demo:*`, the configured stream key patterns, and the commands `PING`, `SET`, `GET`, `INCRBY`, `HSET`, `HGET`, `HMGET`, `HDEL`, `HLEN`, `DEL`, `XADD`, `XGROUP`, `XREADGROUP`, `XACK`, `XDEL`, `JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.NUMINCRBY`, and `EVAL`. Do not grant administrative or dangerous commands.
- Keep TLS enabled for remote Redis Enterprise connections.
- Restrict the Redis endpoint to application networks; do not expose it directly to the public internet.
- Do not share a consumer name between live instances.
- Make processors idempotent and monitor the consumer group's pending-entry list.
- This sample leaves failed records pending by design. In production, add a retry/reclaim policy and dead-letter stream that matches your business rules; automatically claiming an event without a bounded retry policy can cause poison-message loops.
