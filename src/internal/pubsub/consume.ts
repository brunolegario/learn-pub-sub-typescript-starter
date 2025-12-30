import amqp, { type Channel } from "amqplib";
import { ExchangePerilDeadLetter } from "../routing/routing.js";
import { decode } from "@msgpack/msgpack";

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

export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => AckType | Promise<AckType>,
  unmarshaller: (data: Buffer) => T
): Promise<void> {
  const [channel, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    key,
    queueType
  );

  await channel.prefetch(10);

  await channel.consume(
    queue.queue,
    async (msg: amqp.ConsumeMessage | null) => {
      if (!msg) return;

      let data: T;
      try {
        data = unmarshaller(msg.content);
      } catch (err) {
        console.error("Error parsing JSON message:", err);
        return;
      }

      try {
        const ack = await handler(data);
        switch (ack) {
          case AckType.Ack:
            channel.ack(msg);
            break;
          case AckType.NackRequeue:
            channel.nack(msg, false, true);
            break;
          case AckType.NackDiscard:
            channel.nack(msg, false, false);
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

export function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => AckType | Promise<AckType>
): Promise<void> {
  return subscribe(conn, exchange, queueName, key, queueType, handler, (data) =>
    JSON.parse(data.toString())
  );
}

export function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => AckType | Promise<AckType>
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data) => decode(data) as T
  );
}
