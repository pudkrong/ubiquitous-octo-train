import { EventEmitter } from "node:events";
import { setTimeout } from "node:timers/promises";

async function main() {
  const emitter = new EventEmitter({
    captureRejections: true,
  });

  emitter.on("error", (...args) => {
    console.log(Date.now());
    console.log(">>>>", args);
  });

  emitter.on("request", async (v) => {
    await onRequest(v);
  });

  const onRequest = async (v) => {
    await setTimeout(1000);
    throw new Error(`Error from ${v}`);
  };

  for (let i = 0; i < 3; i++) {
    emitter.emit("request", i);
  }
  console.log("done");
}

main().catch(console.error);
