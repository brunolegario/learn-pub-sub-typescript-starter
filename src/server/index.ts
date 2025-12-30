import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
  PauseKey,
} from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import {
  SimpleQueueType,
  subscribeMsgPack,
  AckType,
} from "../internal/pubsub/consume.js";
import { writeLog, type GameLog } from "../internal/gamelogic/logs.js";

async function handlerLog(gameLog: GameLog): Promise<AckType> {
  try {
    await writeLog(gameLog);
    return AckType.Ack;
  } catch (err) {
    console.error("Error writing log:", err);
    return AckType.NackDiscard;
  } finally {
    process.stdout.write("> ");
  }
}

async function main() {
  const connectionString = "amqp://guest:guest@localhost:5672/";
  const connection = await amqp.connect(connectionString);
  console.log("Starting Peril server...");

  await subscribeMsgPack(
    connection,
    ExchangePerilTopic,
    GameLogSlug,
    `${GameLogSlug}.*`,
    SimpleQueueType.Durable,
    handlerLog
  );

  // Used to run the server from a non-interactive source, like the multiserver.sh file
  if (!process.stdin.isTTY) {
    console.log("Non-interactive mode: skipping command input.");
    return;
  }

  printServerHelp();

  const confirmChannel = await connection.createConfirmChannel();

  while (true) {
    const inputs = await getInput();
    if (inputs.length === 0) {
      continue;
    }
    const command = inputs[0]!.toLowerCase();
    if (command === "pause") {
      console.log("Pausing the game...");
      try {
        await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {
          isPaused: true,
        });
      } catch (err) {
        console.error("Error publishing message:", err);
      }
    } else if (command === "resume") {
      console.log("Resuming the game...");
      try {
        await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {
          isPaused: false,
        });
      } catch (err) {
        console.error("Error publishing message:", err);
      }
    } else if (command === "quit") {
      console.log("Quitting the server...");
      break;
    } else {
      console.log(`Unknown command: ${command}`);
    }
  }

  ["SIGINT", "SIGTERM"].forEach((signal) =>
    process.on(signal, async () => {
      try {
        await connection.close();
        console.log("RabbitMQ connection closed.");
      } catch (err) {
        console.error("Error closing RabbitMQ connection:", err);
      } finally {
        process.exit(0);
      }
    })
  );
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
