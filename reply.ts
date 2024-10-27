import { setTimeout } from "node:timers/promises";
import { Reply } from "./lib/reply";

const namespace = "acc-d-weu-sb-ns";
const requestQueue = "acc-d-weu-sb-request";

async function main() {
  const handler = async ({ payload }) => {
    await setTimeout(Math.random() * 2000);
    return `reply to ${payload.value}`;
  };

  const reply = new Reply({
    namespace,
    requestQueue,
    handler,
  });

  await reply.start();
  await new Promise(() => {});
}

main()
  .then(() => console.log("Done"))
  .catch(console.error);
