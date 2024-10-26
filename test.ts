import { ServiceBusClient } from '@azure/service-bus';
import { DefaultAzureCredential } from '@azure/identity';
import { randomUUID } from 'crypto';
import { receiveMessageOnPort, resourceLimits } from 'worker_threads';
import pLimit from 'p-limit';
import { createServer } from 'node:net';

const namespace = 'acc-d-weu-sb-ns';
const requestQueueName = 'acc-d-weu-sb-request';
const replyQueueName = 'acc-d-weu-sb-reply';
const fullyQualifiedNamespace = `${namespace}.servicebus.windows.net`;
const credential = new DefaultAzureCredential();

const timeout = 5;
const client = new ServiceBusClient(fullyQualifiedNamespace, credential);
const sender = client.createSender(requestQueueName);

const request = async (payload) => {
  const sessionId = randomUUID();
  // Send request through request queue
  console.log(`sending request: ${sessionId}`, payload);
  await sender.sendMessages({
    body: payload,
    sessionId,
    timeToLive: 60 * 1000,
  });
};

const dequeue = async () => {
  const receiver = await client.acceptNextSession(requestQueueName, {
    receiveMode: 'receiveAndDelete',
  });
  const response = await receiver.receiveMessages(1, {
    maxWaitTimeInMs: 100,
  });
  if (response.length === 0) return null;
  console.log(`session id: ${receiver.sessionId} > `, response[0].body);
  return response[0].body;
};

async function main() {
  // Sending
  await Promise.all(
    Array.from({ length: 10 }, (_, i) => i).map((i) => request({ value: i })),
  );

  Array.from({ length: 10 }, (_, i) => i).map((i) => dequeue());

  await new Promise(() => {}).then(() => {});
}

main()
  .then(() => console.log('Done'))
  .catch(console.error)
  .finally(async () => {
    console.log('closing client');
    await sender.close();
    await client.close();
  });
