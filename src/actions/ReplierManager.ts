import { Msg, NatsConnection, RequestOptions, SubscriptionOptions } from 'nats';
import { Span } from 'opentracing';
import { match } from 'ts-pattern';
import { TypeOf, z, ZodError, ZodObject } from 'zod';
import { BrokerConfig } from '../ConfigBuilder';
import { BaseEnvelope, BaseEnvelopeSchema } from '../Messaging';
import { BasePayload } from '../Messaging';
import BaseManager, { ValidationChainElement } from './BaseManager';

/* Request Types */
type RequestEnvelope<T> = BaseEnvelope & {
  data: T;
};

export type RequestEnvelopator<T> = (envelopeMaker: (data: T) => RequestEnvelope<T>) => RequestEnvelope<T>;
type RequestMethodData<T> = RequestEnvelope<T> | RequestEnvelopator<T>;

/* Reply Types */
type ErrorPayload<E, K extends keyof E> = {
  code: K,
  data?: E[K],
} | {
  code: 'PAYLOAD_VALIDATION',
  data: ZodError,
} | {
  code: 'INTERNAL_ERROR',
  data: Error,
};

interface ReplyEnvelope extends BaseEnvelope {
  type: 'success' | 'error',
}

interface ErrorEnvelope<E, K extends keyof E> extends ReplyEnvelope {
  type: 'error',
  error: ErrorPayload<E, K>,
}

interface DataEnvelope<T> extends ReplyEnvelope {
  data: T,
}

const ErrorEnvelopeSchema = BaseEnvelopeSchema.extend({
  type: z.literal('error'),
  code: z.union([z.string(), z.number()]),
  data: z.any().optional(),
});

const DataEnvelopeSchema = BaseEnvelopeSchema.extend({
  type: z.literal('success'),
  data: z.any().optional(),
});

export type ReplyErrorPayload<E, K extends keyof E> = {
  code: K,
  data?: E[K],
};

type ErrorEnvelopator<ERR> = <K extends keyof ERR>(envelopeMaker: (data: ErrorPayload<ERR, K>) => ErrorEnvelope<ERR, K>) => ErrorEnvelope<ERR, K>;
type ReplyEnvelopator<T> = (envelopeMaker: (data: T) => DataEnvelope<T>) => DataEnvelope<T>;

interface ReplierDefinition {
  request: ZodObject<any>;
  reply: ZodObject<any>;
  errors: Record<string, any>;
};

export type ReplierDefinitions = Record<string, ReplierDefinition>;

type DataReplier<T> =  (data: DataEnvelope<T> | ReplyEnvelopator<T>) => void;
type ErrorReplier<ERR> = <K extends keyof ERR>(data: ErrorEnvelope<ERR, K> | ErrorEnvelopator<ERR>) => void;

interface ReplyPayload<REQ, REP, ERR> extends BasePayload<REQ, Msg> {
  reply: DataReplier<REP>;
  error: ErrorReplier<ERR>;
}

export type Replier<RDS extends ReplierDefinitions, K extends keyof RDS, C extends {}> = (data: ReplyPayload<TypeOf<RDS[K]['request']>, TypeOf<RDS[K]['reply']>, RDS[K]['errors']> & C) => Promise<void> | void;

class ReplierManager<R extends ReplierDefinitions, C extends {} = {}> extends BaseManager<C> {
  
  constructor(connection: NatsConnection, config: BrokerConfig, private repliers: R, context: C) {
    super(connection, config, context);
  }

  async reply<K extends keyof R>(subject: K, subscriber: Replier<R, K, C>, opts?: SubscriptionOptions): Promise<void> {
    const subscription = this.connection.subscribe(subject as string, {
      queue: `${this.config.queueName}-${subject as string}`,
      ...opts,
    });
    for await (const message of subscription) {
      const basicPayload = super.buildBasicPayload<TypeOf<R[K]['request']>, Msg>(subject, message);
      const replySubject = message.reply;
      if (!replySubject) {
        // No reply subject, abort
        basicPayload.span.log({ event: 'no-reply-subject'});
        basicPayload.span.finish();
        return;
      }

      const send = <T extends BaseEnvelope>(envelope: T, validationChain: ValidationChainElement<T>) => {
        const validation = super.validateEnvelope(envelope, validationChain);

        // Reply payload validation failed, notify client of internal error and end span
        if (!validation) {
          basicPayload.span.logEvent('reply-schema', { payload: 'reply' });
          replyError((builder) => builder({ code: 'INTERNAL_ERROR', data: validation }));
          basicPayload.span.finish();
          throw validation;
        }

        this.connection.publish(replySubject, this.config.serializer(envelope));
        basicPayload.span.finish();
      }

      const replyError: ErrorReplier<R[K]['errors']> = (data) => {
        const envelope = match(typeof data)
          .with('object', () => data as ErrorEnvelope<R[K]['errors'], keyof R[K]['errors']>)
          .with('function', () => {
            const envelopator = data as ErrorEnvelopator<R[K]['errors']>;
            return envelopator((err) => this.buildErrorEnvelope(subject as string, err));
          }).run();
        send(envelope, ['error', ErrorEnvelopeSchema]);
      };

      const replyData: DataReplier<TypeOf<R[K]['reply']>> = (data) => {
        const envelope = match(typeof data)
          .with('object', () => data as DataEnvelope<TypeOf<R[K]['reply']>>)
          .with('function', () => {
            const envelopator = data as ReplyEnvelopator<TypeOf<R[K]['reply']>>;
            return envelopator((data) => this.buildDataEnvelope(subject as string, data));
          }).run();
          send(envelope, ['data', DataEnvelopeSchema]);
        }

      // Failed validation, reply with error and finish
      const requestValidation = super.validateEnvelope(basicPayload.envelope, ['data', this.repliers[subject].request]);
      if (requestValidation !== true) {
        basicPayload.span.logEvent('validation-error', { error: requestValidation });
        replyError((builder) => builder({ code: 'PAYLOAD_VALIDATION', data: requestValidation }));
        basicPayload.span.finish();
        return;
      }

      // Proceed with callback
      try {
        const payload = {
          ...basicPayload,
          reply: replyData,
          error: replyError,
        };

        subscriber(payload);
      } catch (error) {
        basicPayload.span.logEvent('subscriber-error', { error });
      } finally {
        basicPayload.span.finish();
      }
    }
  }

  async request<K extends keyof R, REQ = TypeOf<R[K]['request']>>(subject: K, data: RequestMethodData<REQ>, requestSpan?: Span, opts?: RequestOptions) {
    const span = this.buildSpan(subject as string, 'request', requestSpan);
    const envelope = match(typeof data)
      .with('object', () => data as RequestEnvelope<REQ>)
      .with('function', () => {
        const envelopator = data as RequestEnvelopator<REQ>;
        const envelopeMaker = (data: REQ) => ({
          ...super.buildBaseEnvelope(subject as string),
          data,
        });
        
        return envelopator(envelopeMaker);
      }).run();

    const validation = super.validateEnvelope(envelope, ['data', this.repliers[subject]['request']]);
    if (validation !== true) {
      throw validation;
    }

    const headers = super.buildMessageHeaders(span);
    const payload = this.config.serializer(envelope);
    this.connection.request(subject as string, payload, { headers, timeout: 1000, ...opts });
    span.finish();
  }

  private buildErrorEnvelope<ERR, K extends keyof ERR>(subject: string, error: ErrorPayload<ERR, K>): ErrorEnvelope<ERR, K> {
    return {
      ...super.buildBaseEnvelope(subject),
      type: 'error',
      error,
    };
  }

  private buildDataEnvelope<T>(subject: string, data: T): DataEnvelope<T> {
    return {
      ...super.buildBaseEnvelope(subject),
      type: 'success',
      data,
    };
  }
}

export default ReplierManager;