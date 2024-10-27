import { DefaultAzureCredential } from "@azure/identity";
import {
  ServiceBusClient,
  ServiceBusReceivedMessage,
  ServiceBusReceiver,
  ServiceBusSender,
} from "@azure/service-bus";
import { EventEmitter } from "node:events";

export type ReplyOptions = {
  namespace: string;
  requestQueue: string;
  queueDNS?: string;
  handler: ({ payload }) => Promise<any>;
};

class RequestHandlerError extends Error {
  constructor(error: Error, public requestId: string, public replyTo: string) {
    super(error.message);

    this.stack = error.stack;
    Object.setPrototypeOf(this, RequestHandlerError.prototype);
  }
}

export class Reply {
  private logger = console;
  private client: ServiceBusClient;
  private receiver: ServiceBusReceiver;
  private replyToQueues: Map<string, ServiceBusSender> = new Map();
  private emitter = new EventEmitter({
    captureRejections: true,
  });

  constructor(private options: ReplyOptions) {
    const fullyQualifiedNamespace = `${this.options.namespace}.${
      this.options.queueDNS ?? "servicebus.windows.net"
    }`;
    const credential = new DefaultAzureCredential();
    this.client = new ServiceBusClient(fullyQualifiedNamespace, credential);
    this.receiver = this.client.createReceiver(this.options.requestQueue);
    this.emitter
      .on("error", this.processRequestError.bind(this))
      .on("request", this.processRequest.bind(this));
  }

  async start() {
    this.logger.info(
      `Start receiving request from ${this.options.requestQueue}`
    );
    this.receiver.subscribe({
      processMessage: this.handleRequest.bind(this),
      processError: this.handleError.bind(this),
    });
  }

  private async respondToRequest(
    requestId: string,
    replyTo: string,
    payload: any
  ) {
    const replyQueue = await this.getReplyQueue(replyTo);
    const body =
      payload instanceof Error
        ? { success: false, error: payload.message, stack: payload.stack }
        : { success: true, data: payload };

    await replyQueue.sendMessages({
      correlationId: requestId,
      body,
    });
  }

  private async processRequest(
    payload: any,
    requestId: string,
    replyTo: string
  ) {
    try {
      const result = await this.options.handler({ payload });
      await this.respondToRequest(requestId, replyTo, result);
    } catch (error) {
      throw new RequestHandlerError(error as Error, requestId, replyTo);
    }
  }

  private async processRequestError(error: Error) {
    if (error instanceof RequestHandlerError) {
      await this.respondToRequest(error.requestId, error.replyTo, error);
    }
  }

  private async getReplyQueue(queue: string): Promise<ServiceBusSender> {
    if (this.replyToQueues.has(queue)) return this.replyToQueues.get(queue);

    this.logger.debug(`Create reply queue: ${queue}`);
    const replyQueue = this.client.createSender(queue);
    this.replyToQueues.set(queue, replyQueue);

    return replyQueue;
  }

  private async handleRequest(message: ServiceBusReceivedMessage) {
    const { replyTo, messageId, body } = message;

    this.logger.debug(`emit `, body);
    this.emitter.emit("request", body, messageId, replyTo);
  }

  private async handleError({ error }: { error: Error }) {
    this.logger.warn(`Receiver Error:`, error);
  }

  async end() {
    this.logger.debug(`Closing all resources`);
    await this.receiver.close();

    await this.client.close();
  }
}
