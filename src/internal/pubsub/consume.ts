import amqp, { type Channel } from "amqplib";
import { ExchangePerilDeadLetter } from "../routing/routing.js";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType
): Promise<[Channel, amqp.Replies.AssertQueue]> {
  const ch = await conn.createChannel();

  const queue = await ch.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    autoDelete: queueType === SimpleQueueType.Transient,
    exclusive: queueType === SimpleQueueType.Transient,
    arguments: {
      "x-dead-letter-exchange": ExchangePerilDeadLetter,
    },
  });

  await ch.bindQueue(queue.queue, exchange, key);
  return [ch, queue];
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => AckType | Promise<AckType>
): Promise<void> {
  const [channel, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType
  );

  await channel.consume(
    queue.queue,
    async (msg: amqp.ConsumeMessage | null) => {
      if (!msg) return;

      let data: T;
      try {
        data = JSON.parse(msg.content.toString()) as T;
      } catch (err) {
        console.error("Error parsing JSON message:", err);
        return;
      }

      try {
        const ack = await handler(data);
        switch (ack) {
          case AckType.Ack:
            channel.ack(msg);
            console.log("Message acknowledged.");
            break;
          case AckType.NackRequeue:
            channel.nack(msg, false, true);
            console.log("Message not acknowledged, requeued.");
            break;
          case AckType.NackDiscard:
            channel.nack(msg, false, false);
            console.log("Message not acknowledged, discarded.");
            break;
          default:
            const unreachable: never = ack;
            console.error("Unknown AckType:", unreachable);
            return;
        }
      } catch (err) {
        console.error("Error handling message:", err);
        channel.nack(msg, false, false);
        return;
      }
    }
  );
}
