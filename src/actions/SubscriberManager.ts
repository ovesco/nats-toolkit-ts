import {Msg, NatsConnection, Subscription, SubscriptionOptions} from 'nats';
import {Span, SpanStatusCode} from '@opentelemetry/api';
import {match} from 'ts-pattern';
import {TypeOf, ZodType} from 'zod';
import {BrokerConfig} from '../ConfigBuilder';
import BaseManager from './BaseManager';
import {BaseCallbackPayload, BaseEnvelope} from '../BaseTypes';

type SubscribeEnvelope<T> = BaseEnvelope & {
  data: T;
};

type SubscribePayload<T> = BaseCallbackPayload<Msg, SubscribeEnvelope<T>> & {
  data: T;
};

type Subscriber<T, C extends Record<string, unknown>> = (
  data: SubscribePayload<T> & C,
) => Promise<void> | void;

type Envelopator<T> = (
  envelopeMaker: (data: T) => SubscribeEnvelope<T>,
) => SubscribeEnvelope<T>;
type SendPayload<T> = SubscribeEnvelope<T> | Envelopator<T>;

export type SubscriberDefinitions = Record<string, ZodType>;

export const defineSubscriptions = <S extends SubscriberDefinitions>(
  subs: S,
): S => {
  return subs;
};

class SubscriberManager<
  S extends SubscriberDefinitions,
  C extends Record<string, unknown> = Record<string, unknown>,
> extends BaseManager<C> {
  constructor(
    connection: NatsConnection,
    config: BrokerConfig,
    private subscriptions: S,
    context: C,
  ) {
    super(connection, config, context);
  }

  async subscribe<K extends keyof S>(
    subject: K,
    subscriber: Subscriber<TypeOf<S[K]>, C>,
    opts?: SubscriptionOptions,
  ): Promise<Subscription> {
    const sub = this.connection.subscribe(subject as string, {
      queue: `${this.config.queueName}-${subject as string}`,
      ...opts,
    });

    (async subscription => {
      for await (const message of subscription) {
        this.withBasicPayload<Msg, SubscribeEnvelope<TypeOf<S[K]>>>(
          subject as string,
          'subscribe',
          message,
          async payload => {
            super.validateEnvelope(
              payload.envelope,
              {
                data: payload.envelope.data,
                schema: this.subscriptions[subject],
              },
              undefined,
              true,
            );
            await subscriber({
              ...payload,
              data: payload.envelope.data,
            });
          },
        );
      }
    })(sub);
    return sub;
  }

  async publish<K extends keyof S>(
    subject: K,
    message: SendPayload<TypeOf<S[K]>>,
    publishSpan?: Span,
  ) {
    const envelope = match(typeof message)
      .with('object', () => message as SubscribeEnvelope<TypeOf<S[K]>>)
      .with('function', () => {
        const envelopator = message as Envelopator<TypeOf<S[K]>>;
        const envelopeMaker = (data: TypeOf<S[K]>) => ({
          ...super.buildBaseEnvelope(subject as string, publishSpan),
          data,
        });

        return envelopator(envelopeMaker);
      })
      .run();

    const validation = super.validateEnvelope(
      envelope,
      {data: envelope.data, schema: this.subscriptions[subject]},
      undefined,
    );
    if (validation !== true) {
      publishSpan?.recordException(validation);
      publishSpan?.setStatus({code: SpanStatusCode.ERROR});
      throw validation;
    }
    const payload = this.config.serializer(envelope);
    this.connection.publish(subject as string, payload);
  }
}

export default SubscriberManager;
