import { consumerOpts, ConsumerOptsBuilder, JetStreamClient, JetStreamPublishOptions, JsMsg, Msg, NatsConnection } from 'nats';
import { Span } from 'opentracing';
import { match } from 'ts-pattern';
import { TypeOf, ZodObject } from 'zod';
import { BrokerConfig } from '../ConfigBuilder';
import { Envelope } from '../Messaging';
import { BasePayload } from '../Messaging';
import BaseManager from './BaseManager';

export type ConsumerPayload<T> = BasePayload<T, JsMsg>;
type Consumer<T, C extends {}> = (data: ConsumerPayload<T> & C) => Promise<void> | void;

export type Envelopator<T> = (envelopeMaker: (data: T) => Envelope<T>) => Envelope<T>;
export type DispatchPayload<T> = Envelope<T> | Envelopator<T>;

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

        const payload = super.buildBasicPayload<TypeOf<C[K]>, JsMsg>(subject, message);
        const validation = super.validateEnvelope(payload.envelope, ['data', this.consumers[subject]]);
        if (validation !== true) {
          payload.span.logEvent('validation-error', { error: validation });
          payload.span.finish();
          return;
        }

        try {
          consumer(payload);
        } catch (error) {
          payload.span.logEvent('consumer-error', { error });
        } finally {
          payload.span.finish();
        }
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
    const span = super.buildSpan(subject as string, 'dispatch', dispatchSpan);
    const envelope = match(typeof data)
      .with('object', () => data as Envelope<TypeOf<C[K]>>)
      .with('function', () => {
        const envelopator = data as Envelopator<TypeOf<C[K]>>;
        const envelopeMaker = (data: TypeOf<C[K]>) => ({
          ...super.buildBaseEnvelope(subject as string),
          data,
        });

        return envelopator(envelopeMaker);
      }).run();

    const validation = super.validateEnvelope(envelope, ['data', this.consumers[subject]]);
    if (validation !== true) {
      throw validation;
    }

    const headers = super.buildMessageHeaders(span);
    const payload = this.config.serializer(envelope);
    this.jetStream.publish(subject as string, payload, { headers, ...opts });
    span.finish();
  }
}

export default ConsumerManager;