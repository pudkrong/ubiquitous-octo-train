import { ServiceBusClient, ServiceBusError } from '@azure/service-bus';
import { DefaultAzureCredential } from '@azure/identity';
import { setTimeout } from 'timers/promises';
import { createServer } from 'node:net';
import pLimit from 'p-limit';
import { EventEmitter } from 'node:events';
import { ppid } from 'process';

const namespace = 'acc-d-weu-sb-ns';
const requestQueueName = 'acc-d-weu-sb-request';
const fullyQualifiedNamespace = `${namespace}.servicebus.windows.net`;
const credential = new DefaultAzureCredential();
const concurrent = 5;

const limit = pLimit(concurrent);
const emitter = new EventEmitter({
  captureRejections: true,
});
const client = new ServiceBusClient(fullyQualifiedNamespace, credential);
const receiver = client.createReceiver(requestQueueName);

const reply = async (requestMessage, response) => {
  try {
    const { replyTo, messageId } = requestMessage;

    const sender = client.createSender(replyTo);
    await sender.sendMessages({
      body: {
        data: response,
        status: 'ok',
      },
      correlationId: messageId,
    });
  } catch (err) {
    if (err instanceof ServiceBusError) {
      return console.warn(`ServiceBusError:`, err.message);
    }
  }
};

const onRequest = async (request) => {
  const { body } = request;

  await setTimeout(Math.random() * 500);
  await reply(request, { value: body.value });
};

emitter.on('request', async (msg) => {
  limit(() => onRequest(msg));
});

const processor = async () => {
  return await receiver.subscribe({
    processMessage: async (msg) => {
      emitter.emit('request', msg);
    },
    processError: async (err) => {
      console.error(err);
    },
  });
};

async function main() {
  // const ac = new AbortController();
  // process.once('SIGINT', () => ac.abort());
  const sub = await processor();
  console.log('ready to process');
  //   ac.signal.addEventListener('abort', async () => sub.close());

  await new Promise(() => {});
}

main()
  .then(() => console.log('Done'))
  .catch(console.error)
  .finally(async () => {
    console.log('closing client');
    await client.close();
  });
