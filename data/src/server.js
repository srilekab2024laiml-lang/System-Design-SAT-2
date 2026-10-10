const express = require('express');
const amqp = require('amqplib');
const { randomUUID } = require('crypto');

const app = express();
app.use(express.json({ limit: '64kb' }));
const AMQP_URL = process.env.AMQP_URL || 'amqp://localhost';
const PORT = Number(process.env.PORT || 3000);
const EXCHANGE = 'ride.events';
const DLX = 'ride.events.dlx';
const MAX_RETRIES = 3;

let connection;
let publisher;
const consumers = new Map();

function validString(value, max = 200) {
  return typeof value === 'string' && value.trim().length > 0 && value.length <= max;
}
function safeName(s) { return s.replace(/[^a-zA-Z0-9_.-]/g, '_'); }

async function setup() {
  connection = await amqp.connect(AMQP_URL);
  connection.on('error', err => console.error('AMQP connection error:', err.message));
  connection.on('close', () => console.error('AMQP connection closed'));
  publisher = await connection.createConfirmChannel();
  await publisher.assertExchange(EXCHANGE, 'topic', { durable: true });
  await publisher.assertExchange(DLX, 'topic', { durable: true });
}

async function subscribe(subscriberId, bindings) {
  const queue = `notifications.${safeName(subscriberId)}`;
  const channel = await connection.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  await channel.assertExchange(DLX, 'topic', { durable: true });
  await channel.assertQueue(queue, {
    durable: true,
    deadLetterExchange: DLX,
    deadLetterRoutingKey: queue
  });
  await channel.assertQueue(`${queue}.dead`, { durable: true });
  await channel.bindQueue(`${queue}.dead`, DLX, queue);
  for (const key of bindings) await channel.bindQueue(queue, EXCHANGE, key);
  await channel.prefetch(10);

  channel.consume(queue, async msg => {
    if (!msg) return;
    try {
      const event = JSON.parse(msg.content.toString());
      await deliverNotification(subscriberId, event);
      channel.ack(msg);
    } catch (err) {
      const retries = Number(msg.properties.headers?.['x-retry-count'] || 0);
      console.error(`Notification handler failed for ${subscriberId}:`, err.message);
      if (retries < MAX_RETRIES) {
        const retryHeaders = { ...(msg.properties.headers || {}), 'x-retry-count': retries + 1 };
        channel.sendToQueue(queue, msg.content, {
          persistent: true,
          contentType: msg.properties.contentType || 'application/json',
          messageId: msg.properties.messageId,
          headers: retryHeaders
        });
        channel.ack(msg);
      } else {
        channel.nack(msg, false, false);
      }
    }
  }, { noAck: false });
  consumers.set(subscriberId, channel);
  return queue;
}

async function deliverNotification(subscriberId, event) {
  // Replace this with a push/SMS/email/WebSocket provider call.
  console.log(JSON.stringify({ deliveredTo: subscriberId, event }));
}

app.get('/health', (_req, res) => res.json({ status: connection ? 'ok' : 'starting' }));

app.post('/subscriptions', async (req, res) => {
  const { subscriberId, bindings } = req.body || {};
  if (!validString(subscriberId, 100) || !Array.isArray(bindings) || bindings.length < 1 || bindings.length > 20 || !bindings.every(b => validString(b, 200))) {
    return res.status(400).json({ error: 'subscriberId and 1-20 string bindings are required' });
  }
  try {
    const queue = await subscribe(subscriberId, [...new Set(bindings)]);
    return res.status(201).json({ subscriberId, queue, bindings: [...new Set(bindings)] });
  } catch (err) {
    console.error('Subscription failed:', err);
    return res.status(503).json({ error: 'Broker unavailable or subscription failed' });
  }
});

app.post('/notifications', async (req, res) => {
  const { eventType, rideId, recipientId, message, data = {} } = req.body || {};
  if (![eventType, rideId, recipientId, message].every(v => validString(v, 200)) || typeof data !== 'object' || data === null || Array.isArray(data)) {
    return res.status(400).json({ error: 'eventType, rideId, recipientId, message and object data are required' });
  }
  const eventId = randomUUID();
  const routingKey = `ride.${safeName(eventType)}.${safeName(recipientId)}`;
  const event = { eventId, eventType, rideId, recipientId, message, data, createdAt: new Date().toISOString() };
  try {
    await new Promise((resolve, reject) => {
      publisher.publish(EXCHANGE, routingKey, Buffer.from(JSON.stringify(event)), {
        persistent: true,
        contentType: 'application/json',
        messageId: eventId,
        timestamp: Date.now(),
        type: eventType
      }, err => err ? reject(err) : resolve());
    });
    return res.status(202).json({ accepted: true, eventId, routingKey });
  } catch (err) {
    console.error('Publish failed:', err);
    return res.status(503).json({ error: 'Notification broker unavailable' });
  }
});

async function main() {
  await setup();
  app.listen(PORT, () => console.log(`Notification API listening on ${PORT}`));
}
main().catch(err => { console.error('Startup failed:', err); process.exit(1); });
