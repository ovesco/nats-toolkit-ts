import { ConnectionOptions, JetStreamOptions, StringCodec } from "nats";
import { Tracer } from "opentracing";
import pino, { Logger } from "pino";
import { v4 as uuid } from 'uuid';

export type SetupConfig = {
  natsOptions?: ConnectionOptions,
  jetStreamOptions?: JetStreamOptions,
  serializer?: (payload: object) => Uint8Array,
  deserializer?: <T extends object>(payload: Uint8Array) => T,
  tracer?: Tracer,
  logger?: Logger,
  name?: string,
  queueName?: string,
  id?: string | number,
}

export type BrokerConfig = Required<Omit<SetupConfig, 'natsOptions'>>;
export type AnyBrokerConfig = BrokerConfig;

export const buildConfig = (config: SetupConfig): BrokerConfig => {
  const codec = StringCodec();
  const serializer = config.serializer ?? ((payload) => codec.encode(JSON.stringify(payload)));
  const deserializer = config.deserializer ?? ((payload) => JSON.parse(codec.decode(payload)));
  const name = config.name || uuid();
  const tracer = config.tracer || new Tracer();
  const logger = config.logger || pino();

  return {
    ...config,
    tracer,
    logger,
    jetStreamOptions: config.jetStreamOptions || {},
    name,
    serializer,
    deserializer,
    id: config.id || uuid(),
    queueName: config.queueName || name,
  };
};