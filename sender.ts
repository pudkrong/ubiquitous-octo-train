import {
  ServiceBusClient,
  ServiceBusAdministrationClient,
} from "@azure/service-bus";
import { DefaultAzureCredential } from "@azure/identity";
import { randomUUID } from "crypto";
import pLimit from "p-limit";

const namespace = "acc-d-weu-sb-ns";
const requestQueueName = "acc-d-weu-sb-request";
const fullyQualifiedNamespace = `${namespace}.servicebus.windows.net`;
const credential = new DefaultAzureCredential();

const timeout = 10;
const client = new ServiceBusClient(fullyQualifiedNamespace, credential);
const adminClient = new ServiceBusAdministrationClient(
  fullyQualifiedNamespace,
  credential
);
const sender = client.createSender(requestQueueName);

const cleanQueues = async () => {
  const r = adminClient.listQueues();
  for await (const q of r) {
    if (q.name.startsWith("reply-")) {
      console.log(`deleting queue: ${q.name}`);
      await adminClient.deleteQueue(q.name);
    }
  }
};

const request = async (payload: any, timeoutInMs: number = 1000) => {
  const messageId = randomUUID();
  const replyTo = `reply-${messageId}`;
  await adminClient.createQueue(replyTo, {
    requiresSession: false,
  });
  const receiver = client.createReceiver(replyTo);

  // Send request through request queue
  // console.log(`sending request: ${messageId}`, payload);
  await sender.sendMessages({
    body: payload,
    replyTo,
    messageId,
    timeToLive: timeoutInMs * 2,
  });

  // Wait for response
  try {
    console.time(messageId);
    const response = await receiver.receiveMessages(1, {
      maxWaitTimeInMs: timeoutInMs,
    });
    if (response.length === 0) {
      console.log(`timeout to receive response: ${messageId}`);
      return { success: false, error: new Error("Timeout") };
    }

    console.log(`received response: ${messageId}`, response[0].body);
    console.timeEnd(messageId);
    return { success: true, data: response[0].body };
  } finally {
    await receiver.close();
    await adminClient.deleteQueue(replyTo);
  }
};

async function main() {
  // await cleanQueues();

  const limit = pLimit(10);
  await Promise.all(
    Array.from({ length: 1000 }, (_, i) => i).map(
      (i) => limit(() => request({ value: i }, 2000))
      // request({ value: i }, 5000),
    )
  );
}

main()
  .then(() => console.log("Done"))
  .catch(console.error)
  .finally(async () => {
    console.log("closing client");
    await sender.close();
    await client.close();
  });
