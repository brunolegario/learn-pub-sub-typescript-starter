import amqp, { type ConfirmChannel, type Channel } from "amqplib";
import {
  clientWelcome,
  commandStatus,
  getInput,
  getMaliciousLog,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import {
  subscribeJSON,
  SimpleQueueType,
  AckType,
} from "../internal/pubsub/consume.js";
import {
  ArmyMovesPrefix,
  ExchangePerilDirect,
  ExchangePerilTopic,
  GameLogSlug,
  PauseKey,
  WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import {
  GameState,
  type PlayingState,
} from "../internal/gamelogic/gamestate.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import {
  commandMove,
  handleMove,
  MoveOutcome,
} from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import type {
  ArmyMove,
  RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
  return (ps: PlayingState) => {
    handlePause(gs, ps);
    process.stdout.write("> ");
    return AckType.Ack;
  };
}

function handlerMove(
  gs: GameState,
  channel: ConfirmChannel
): (move: ArmyMove) => Promise<AckType> {
  return async (move: ArmyMove) => {
    try {
      const outcome = handleMove(gs, move);
      switch (outcome) {
        case MoveOutcome.Safe:
        case MoveOutcome.SamePlayer:
          return AckType.Ack;
        case MoveOutcome.MakeWar:
          const recognition: RecognitionOfWar = {
            attacker: move.player,
            defender: gs.getPlayerSnap(),
          };

          try {
            await publishJSON(
              channel,
              ExchangePerilTopic,
              `${WarRecognitionsPrefix}.${gs.getUsername()}`,
              recognition
            );
            return AckType.Ack;
          } catch (err) {
            console.error("Error publishing war recognition:", err);
            return AckType.NackRequeue;
          }
        default:
          return AckType.NackDiscard;
      }
    } finally {
      process.stdout.write("> ");
    }
  };
}

function handlerWar(
  gs: GameState,
  ch: ConfirmChannel
): (rw: RecognitionOfWar) => Promise<AckType> {
  return async (rw: RecognitionOfWar) => {
    try {
      const resolution = handleWar(gs, rw);

      switch (resolution.result) {
        case WarOutcome.NotInvolved:
          return AckType.NackRequeue;
        case WarOutcome.NoUnits:
          return AckType.NackDiscard;
        case WarOutcome.OpponentWon:
        case WarOutcome.YouWon:
          try {
            await publishGameLog(
              ch,
              gs.getUsername(),
              `${resolution.winner} won a war against ${resolution.loser}`
            );

            return AckType.Ack;
          } catch (err) {
            console.error("Error publishing game log:", err);
            return AckType.NackRequeue;
          }
        case WarOutcome.Draw:
          try {
            await publishGameLog(
              ch,
              gs.getUsername(),
              `A war between ${resolution.attacker} and ${resolution.defender} resulted in a draw`
            );
            return AckType.Ack;
          } catch (err) {
            console.error("Error publishing game log:", err);
            return AckType.NackRequeue;
          }
        default:
          const unreachable: never = resolution;
          console.log("Unknown WarResolution:", unreachable);
          return AckType.NackDiscard;
      }
    } finally {
      process.stdout.write("> ");
    }
  };
}

async function publishGameLog(
  ch: ConfirmChannel,
  username: string,
  msg: string
) {
  const gamelog: GameLog = {
    username: username,
    message: msg,
    currentTime: new Date(),
  };

  await publishMsgPack(
    ch,
    ExchangePerilTopic,
    `${GameLogSlug}.${username}`,
    gamelog
  );
}

function handlerSpam(ch: ConfirmChannel, username: string, inputs: string[]) {
  if (inputs.length < 2) {
    console.log("usage: spam <n>");
    return;
  }
  const raw = inputs[1];
  if (!raw) {
    console.log("usage: spam <n>");
    return;
  }
  const n = parseInt(raw, 10);
  if (isNaN(n)) {
    console.log(`error: ${inputs[1]} is not a valid number`);
    return;
  }

  for (let i = 0; i < n; i++) {
    try {
      const log = getMaliciousLog();
      publishGameLog(ch, username, log);
    } catch (err) {
      console.error("Error publishing spam log:", (err as Error).message);
      continue;
    }
  }
}

async function main() {
  const connectionString = "amqp://guest:guest@localhost:5672/";
  const connection = await amqp.connect(connectionString);
  console.log("Starting Peril client...");

  const username = await clientWelcome();
  const gameState = new GameState(username);
  const channel = await connection.createConfirmChannel();

  await subscribeJSON(
    connection,
    ExchangePerilDirect,
    `${PauseKey}.${username}`,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gameState)
  );

  await subscribeJSON(
    connection,
    ExchangePerilTopic,
    `${ArmyMovesPrefix}.${username}`,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gameState, channel)
  );

  await subscribeJSON(
    connection,
    ExchangePerilTopic,
    WarRecognitionsPrefix,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    handlerWar(gameState, channel)
  );

  while (true) {
    const inputs = await getInput();
    if (inputs.length === 0) {
      continue;
    }
    const command = inputs[0]!.toLowerCase();
    if (command === "spawn") {
      try {
        console.log(`${username} is spawning a new unit...`);
        commandSpawn(gameState, inputs);
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "move") {
      try {
        console.log(`${username} is moving a unit...`);
        const move = commandMove(gameState, inputs);
        await publishJSON(
          channel,
          ExchangePerilTopic,
          `${ArmyMovesPrefix}.${username}`,
          move
        );
      } catch (err) {
        console.log((err as Error).message);
      }
    } else if (command === "status") {
      commandStatus(gameState);
    } else if (command === "help") {
      printClientHelp();
    } else if (command === "spam") {
      try {
        handlerSpam(channel, username, inputs);
      } catch (err) {
        console.log((err as Error).message);
      } finally {
        continue;
      }
    } else if (command === "quit") {
      printQuit();
      process.exit(0);
    } else {
      console.log(`Unknown command: ${command}`);
      continue;
    }
  }
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
