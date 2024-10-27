import { DefaultAzureCredential } from "@azure/identity";
import {
  ServiceBusAdministrationClient,
  ServiceBusClient,
  ServiceBusReceivedMessage,
  ServiceBusReceiver,
  ServiceBusSender,
} from "@azure/service-bus";
import { time } from "console";
import { randomUUID } from "crypto";
import { hostname } from "os";

export type RequestOptions = {
  namespace: string;
  requestQueue: string;
  queueDNS?: string;
  timeoutInMs: 5000;
};

export class Request {
  private logger = console;
  private adminClient: ServiceBusAdministrationClient;
  private client: ServiceBusClient;
  private sender: ServiceBusSender;
  private receiver: ServiceBusReceiver;
  private replyQueue: string;
  private pendingRequests: Map<
    string,
    {
      resolve: (response: any) => void;
      reject: (error: Error) => void;
      timeoutId: NodeJS.Timeout;
    }
  > = new Map();

  constructor(private options: RequestOptions) {
    const fullyQualifiedNamespace = `${this.options.namespace}.${
      this.options.queueDNS ?? "servicebus.windows.net"
    }`;
    const credential = new DefaultAzureCredential();

    this.adminClient = new ServiceBusAdministrationClient(
      fullyQualifiedNamespace,
      credential
    );
    this.client = new ServiceBusClient(fullyQualifiedNamespace, credential);
    this.sender = this.client.createSender(this.options.requestQueue);
    this.replyQueue = `reply-${hostname().toLowerCase()}`;
  }

  private async assertReceiverQueue(): Promise<void> {
    if (this.receiver === undefined) {
      if ((await this.adminClient.queueExists(this.replyQueue)) === false) {
        this.logger.debug(`Creating reply queue: ${this.replyQueue}`);
        await this.adminClient.createQueue(this.replyQueue);
      }

      this.receiver = this.client.createReceiver(this.replyQueue);
    }
  }

  async start(): Promise<void> {
    await this.assertReceiverQueue();

    this.logger.info(`Start receiving response from ${this.replyQueue}`);
    const sub = this.receiver.subscribe({
      processMessage: this.handleResponse.bind(this),
      processError: this.handleError.bind(this),
    });
  }

  async request(payload: any, timeoutInMs?: number): Promise<any> {
    const requestId = randomUUID();
    timeoutInMs = timeoutInMs ?? this.options.timeoutInMs;

    return new Promise(async (resolve, reject) => {
      const timeoutId = setTimeout(() => {
        this.pendingRequests.delete(requestId);
        reject(
          new Error(`Request: ${requestId} timeout after ${timeoutInMs} ms`)
        );
      }, timeoutInMs);

      this.pendingRequests.set(requestId, { resolve, reject, timeoutId });

      this.logger.debug(
        `Sending request ${requestId} to ${this.options.requestQueue}`
      );
      await this.sender.sendMessages({
        body: payload,
        replyTo: this.replyQueue,
        messageId: requestId,
      });
    });
  }

  private async handleError({ error }: { error: Error }) {
    this.logger.warn(`Receiver Error:`, error);
  }

  private async handleResponse(
    message: ServiceBusReceivedMessage
  ): Promise<void> {
    const { correlationId } = message;
    if (correlationId === undefined) return;

    const requestId = String(correlationId);
    const pendingRequest = this.pendingRequests.get(requestId);
    if (pendingRequest === undefined) return;

    const { resolve, reject, timeoutId } = pendingRequest;
    clearTimeout(timeoutId);

    const { success, data, error, stack } = message.body;
    if (success) {
      resolve(data);
    } else {
      const requestError = new Error(error);
      requestError.stack = stack;
      reject(requestError);
    }

    this.pendingRequests.delete(requestId);
  }

  async close() {
    this.logger.debug(`Closing all resources`);
    await Promise.all([this.sender.close(), this.receiver?.close()]);

    await this.client.close();
    if (this.receiver) {
      this.logger.debug(`Deleting ${this.replyQueue}`);
      await this.adminClient.deleteQueue(this.replyQueue);
    }
  }
}
