# RabbitMQ Pub/Sub Notification Service

A runnable Node.js implementation of the experiment in the assignment: a RabbitMQ topic exchange distributes ride-hailing events to independent notification consumers.

## Requirements
- Docker and Docker Compose
- Node.js 18+ and npm (for running the app outside Docker)

## Start
```bash
docker compose up --build
```
RabbitMQ management UI: http://localhost:15672 (guest / guest)
Publisher API: http://localhost:3000

## Test
Create two independent subscribers (each process owns a durable queue):
```bash
curl -X POST http://localhost:3000/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"subscriberId":"passenger-101","bindings":["ride.*.passenger-101","ride.*"]}'

curl -X POST http://localhost:3000/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"subscriberId":"driver-202","bindings":["ride.*.driver-202"]}'
```
Publish a notification:
```bash
curl -X POST http://localhost:3000/notifications \
  -H 'Content-Type: application/json' \
  -d '{"eventType":"ride.driver_assigned","rideId":"ride-9001","recipientId":"passenger-101","message":"A driver has been assigned","data":{"driverName":"Sam"}}'
```
The routing key is `ride.<eventType>.<recipientId>`. Example: `ride.ride.driver_assigned.passenger-101`; for simpler general event subscriptions use binding `ride.#`.

Each subscriber queue gets its own copy of matching events. Messages are acknowledged only after the notification handler succeeds. Failures are retried by RabbitMQ and then routed to a dead-letter queue after three attempts. Publisher confirms reduce the risk of silently losing accepted messages. This demo logs delivery; replace the handler with push, SMS, email, or WebSocket delivery.

## API
`POST /subscriptions` body: `{"subscriberId":"...","bindings":["ride.#"]}`. Creates a durable queue named `notifications.<subscriberId>` and binds it to `ride.events` topic exchange. Returns 201.

`POST /notifications` body: `{"eventType":"ride.driver_assigned","rideId":"ride-9001","recipientId":"passenger-101","message":"...","data":{}}`. Returns 202 and `eventId` when broker confirms publication; returns 503 if RabbitMQ is unavailable.

Health: `GET /health`.

## Notes for production
Use authenticated RabbitMQ users and TLS, validate and authorize subscriber IDs, persist subscription state, use an outbox if publishing from a database transaction, monitor queue depth and dead letters, and make downstream delivery idempotent using `eventId`. A durable queue and persistent message protect against broker restart, but not against every infrastructure failure; deploy a replicated RabbitMQ quorum queue where required. Avoid sensitive data in broad topic events.
