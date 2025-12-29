import { type ConfirmChannel } from "amqplib";

export async function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T
): Promise<void> {
  // Serialize the value to JSON bytes
  const contentBuffer = Buffer.from(JSON.stringify(value), "utf-8");

  return new Promise((resolve, reject) => {
    ch.publish(
      exchange,
      routingKey,
      contentBuffer,
      { contentType: "application/json" },
      (err) => {
        if (err !== null) {
          reject(new Error("Message was NACKed by the broker"));
        } else {
          resolve();
        }
      }
    );
  });
}
