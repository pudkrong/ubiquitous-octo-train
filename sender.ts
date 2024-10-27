import {
  ServiceBusClient,
  ServiceBusAdministrationClient,
  ServiceBusSender,
  ServiceBusReceivedMessage,
  ServiceBusReceiver,
} from "@azure/service-bus";
import { DefaultAzureCredential } from "@azure/identity";
import { randomUUID } from "crypto";
import pLimit from "p-limit";
import { hostname } from "os";
import { Request } from "./lib/request";

const namespace = "acc-d-weu-sb-ns";
const requestQueue = "acc-d-weu-sb-request";

async function main() {
  const request = new Request({
    namespace,
    requestQueue,
    timeoutInMs: 5000,
  });
  await request.start();

  for (let i = 0; i < 10; i++) {
    request
      .request({ value: i })
      .then((result) => console.log(`result for ${i} => `, result))
      .catch((error) => console.log(error));
  }

  // await request.close();

  // process.once("SIGINT", async () => await request.end());

  // const limit = pLimit(5);
  // await Promise.all(
  //   Array.from({ length: 1000 }, (_, i) => i).map((i) =>
  //     limit(() => {
  //       console.time(String(i));
  //       return request
  //         .request({ value: i }, 1500)
  //         .then((result) => {
  //           console.log(`result>`, result);
  //         })
  //         .catch((error) => {
  //           // console.log(`error>`, error);
  //           console.timeEnd(String(i));
  //         });
  //     })
  //   )
  // );
}

main()
  .then(() => console.log("Done"))
  .catch((err) => console.error("catch>>", err));
