import { consumerOpts, ConsumerOptsBuilder, JetStreamClient, JetStreamPublishOptions, JsMsg, Msg, NatsConnection } from 'nats';
import { match } from 'ts-pattern';
import { TypeOf, ZodObject } from 'zod';
import { BrokerConfig } from '../ConfigBuilder';
import { BaseCallbackPayload, BaseEnvelope } from '../BaseTypes';
import BaseManager from './BaseManager';
import { Span, SpanStatusCode } from '@opentelemetry/api';

type ConsumerEnvelope<T> = BaseEnvelope & {
  data: T;
};

type ConsumerPayload<T> = BaseCallbackPayload<JsMsg, ConsumerEnvelope<T>> & {
  data: T;
};

type Consumer<T, C extends {}> = (data: ConsumerPayload<T> & C) => Promise<void> | void;

export type Envelopator<T> = (envelopeMaker: (data: T) => ConsumerEnvelope<T>) => ConsumerEnvelope<T>;
export type DispatchPayload<T> = ConsumerEnvelope<T> | Envelopator<T>;

export type ConsumerDefinitions = Record<string, ZodObject<any>>;

class ConsumerManager<C extends ConsumerDefinitions, CTX extends {} = {}> extends BaseManager<CTX> {
  
  private jetStream: JetStreamClient;

  private scheduledForStop = false;

  private pollings: Function[] = [];

  constructor(connection: NatsConnection, config: BrokerConfig, private consumers: C, context: CTX) {
    super(connection, config, context);
    this.jetStream = this.connection.jetstream(this.config.jetStreamOptions);
  }

  async consume<K extends keyof C>(subject: K, consumer: Consumer<TypeOf<C[K]>, CTX>, batch = 20, expires = 5000, optsOverride?: (opts: ConsumerOptsBuilder) => ConsumerOptsBuilder): Promise<void> {
    
    const baseOptions = consumerOpts();
    baseOptions.ackExplicit();
    baseOptions.durable(`d${this.config.queueName || this.config.name}-${subject as string}`);
    baseOptions.queue(this.config.queueName || this.config.name);
    baseOptions.deliverAll();
    const options = optsOverride ? optsOverride(baseOptions) : baseOptions;
    const subscription = await this.jetStream.pullSubscribe(subject as string, options);

    let interval: NodeJS.Timeout | null = null;

    function startPolling() {
      if (interval === null) {
        subscription.pull({ batch, expires });
        interval = setInterval(() => subscription.pull({ batch, expires}), expires);
      }
    }

    (async (sub) => {
      for await (const message of sub) {
        // When receiving a message, stop polling to let the subscription work without overloading buffer
        // by adding new messages in
        if (interval) {
          clearInterval(interval);
          interval = null;
        }

        this.withBasicPayload<JsMsg, ConsumerEnvelope<TypeOf<C[K]>>>(subject as string, 'consume', message, async (payload) => {
          super.validateEnvelope(payload.envelope, { data: payload.envelope.data, schema: this.consumers[subject] }, undefined, true);
          await consumer({
            ...payload,
            data: payload.envelope.data,
          });
        });
      }

      // When done with all messages in buffer, poll back again
      if (!this.scheduledForStop) {
        startPolling();
      }
    })(subscription);

    startPolling();

    this.pollings.push(async () => {
      if (interval) {
        clearInterval(interval);
      }
      await subscription.drain();
    });
  }

  async dispatch<K extends keyof C>(subject: K, data: DispatchPayload<TypeOf<C[K]>>, dispatchSpan?: Span, opts?: Partial<JetStreamPublishOptions>) {
    const envelope = match(typeof data)
      .with('object', () => data as ConsumerEnvelope<TypeOf<C[K]>>)
      .with('function', () => {
        const envelopator = data as Envelopator<TypeOf<C[K]>>;
        const envelopeMaker = (data: TypeOf<C[K]>) => ({
          ...super.buildBaseEnvelope(subject as string, dispatchSpan),
          data,
        });

        return envelopator(envelopeMaker);
      }).run();

    const validation = super.validateEnvelope(envelope, { data: envelope.data, schema: this.consumers[subject] });
    if (validation !== true) {
      dispatchSpan?.recordException(validation);
      dispatchSpan?.setStatus({ code: SpanStatusCode.ERROR });
      throw validation;
    }

    const payload = this.config.serializer(envelope);
    this.jetStream.publish(subject as string, payload, opts);
  }
}

export default ConsumerManager;