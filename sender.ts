import { Request } from "./lib/request";
import { setTimeout } from "timers/promises";

const namespace = "acc-d-weu-sb-ns";
const requestQueue = "acc-d-weu-sb-request";

async function main() {
  process.once("SIGINT", async () => {
    console.log("Closing");
    await request.close();

    process.exit(1);
  });

  const request = new Request({
    namespace,
    requestQueue,
    timeoutInMs: 5000,
  });
  await request.initial();

  for (let i = 0; i < 10; i++) {
    await setTimeout(1000);
    request
      .request({ value: i })
      .then((result) => console.log(`result for ${i} => `, result))
      .catch((error) => console.log(error));
  }
}

main()
  .then(() => console.log("Done"))
  .catch((err) => console.error("catch>>", err));
