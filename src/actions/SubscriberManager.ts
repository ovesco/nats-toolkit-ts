import { Msg, NatsConnection, Subscription, SubscriptionOptions } from 'nats';
import { Span } from 'opentracing';
import { match } from 'ts-pattern';
import { TypeOf, ZodSchema } from 'zod';
import { BrokerConfig } from '../ConfigBuilder';
import { Envelope, BasePayload } from '../Messaging';
import BaseManager from './BaseManager';

export type SubscribePayload<T> = BasePayload<T, Msg>;
type Subscriber<T, C extends {}> = (data: SubscribePayload<T> & C) => Promise<void> | void;

export type Envelopator<T> = (envelopeMaker: (data: T) => Envelope<T>) => Envelope<T>;
export type SendPayload<T> = Envelope<T> | Envelopator<T>;

export type SubscriberDefinitions = Record<string, ZodSchema>;

class SubscriberManager<S extends SubscriberDefinitions, C extends {} = {}> extends BaseManager<C> {
  
  constructor(connection: NatsConnection, config: BrokerConfig, private subscriptions: S, context: C) {
    super(connection, config, context);
  }

  async subscribe<K extends keyof S>(subject: K, subscriber: Subscriber<TypeOf<S[K]>, C>, opts?: SubscriptionOptions): Promise<Subscription> {
    const sub = this.connection.subscribe(subject as string, {
      queue: `${this.config.queueName}-${subject as string}`,
      ...opts,
    });

    (async (subscription) => {
      for await (const message of subscription) {
        const payload = super.buildBasicPayload<TypeOf<S[K]>, Msg>(subject, message);
        const validation = super.validateEnvelope(payload.envelope, ['data', this.subscriptions[subject]]);
        if (validation !== true) {
          payload.span.logEvent('validation-error', { error: validation });
          payload.span.finish();
          return;
        }

        try {
          this.config.logger.info({ subject, type: 'subscribe', start: Date.now() })
          subscriber(payload);
        } catch (error) {
          payload.span.logEvent('subscriber-error', { error });
        } finally {
          payload.span.finish();
        }
      }
    })(sub);
    return sub;
  }

  async publish<K extends keyof S>(subject: K, message: SendPayload<TypeOf<S[K]>>, publishSpan?: Span) {
    const span = super.buildSpan(subject as string, 'publish', publishSpan);
    const envelope = match(typeof message)
      .with('object', () => message as Envelope<TypeOf<S[K]>>)
      .with('function', () => {
        const envelopator = message as Envelopator<TypeOf<S[K]>>;
        const envelopeMaker = (data: TypeOf<S[K]>) => ({
          ...super.buildBaseEnvelope(subject as string),
          data,
        });
        
        return envelopator(envelopeMaker);
      }).run();

    const validation = super.validateEnvelope(envelope, ['data', this.subscriptions[subject]]);
    if (validation !== true) {
      throw validation;
    }

    const headers = super.buildMessageHeaders(span);
    const payload = this.config.serializer(envelope);
    this.connection.publish(subject as string, payload, { headers });
    span.finish();
  }
}

export default SubscriberManager;