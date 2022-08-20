import { Msg, NatsConnection, RequestOptions, Subscription, SubscriptionOptions } from 'nats';
import { Span, SpanStatusCode } from '@opentelemetry/api';
import { match } from 'ts-pattern';
import { TypeOf, z, ZodError, ZodSchema } from 'zod';
import { BrokerConfig } from '../ConfigBuilder';
import { BaseEnvelope, BaseEnvelopeSchema } from '../BaseTypes';
import { BaseCallbackPayload } from '../BaseTypes';
import BaseManager, { PropValidation } from './BaseManager';

/* Request Types */
type RequestEnvelope<T> = BaseEnvelope & {
  data: T;
};

export type RequestEnvelopator<T> = (envelopeMaker: (data: T) => RequestEnvelope<T>) => RequestEnvelope<T>;
type RequestMethodData<T> = RequestEnvelope<T> | RequestEnvelopator<T>;

/* Reply Types */
type ReplyErrorPayload<E, K extends keyof E> = {
  code: K,
  data?: E[K] extends ZodSchema ? TypeOf<E[K]> : E[K],
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

export interface ReplyErrorEnvelope<E, K extends keyof E> extends ReplyEnvelope {
  type: 'error',
  error: ReplyErrorPayload<E, K>,
}

export interface ReplyDataEnvelope<T> extends ReplyEnvelope {
  type: 'success',
  data: T,
}

export type ReplyAllTypesEnvelope<T, E> = ReplyEnvelope & (ReplyErrorEnvelope<E, keyof E> | ReplyDataEnvelope<T>);

const ErrorEnvelopeSchema = BaseEnvelopeSchema.extend({
  type: z.literal('error'),
  error: z.object({
    code: z.union([z.string(), z.number()]),
    data: z.any().optional(),
  }),
});

const DataEnvelopeSchema = BaseEnvelopeSchema.extend({
  type: z.literal('success'),
  data: z.any().optional(),
});

type ReplyErrorEnvelopator<ERR, K extends keyof ERR> = (envelopeMaker: (data: ReplyErrorPayload<ERR, K>) => ReplyErrorEnvelope<ERR, K>) => ReplyErrorEnvelope<ERR, K>;
type ReplyDataEnvelopator<T> = (envelopeMaker: (data: T) => ReplyDataEnvelope<T>) => ReplyDataEnvelope<T>;

interface ReplierDefinition {
  request: ZodSchema;
  reply: ZodSchema;
  errors?: Record<string, any>;
};

export type ReplierDefinitions = Record<string, ReplierDefinition>;

type DataReplier<T> =  (data: ReplyDataEnvelope<T> | ReplyDataEnvelopator<T>) => void;
type ErrorReplier<ERR> = <K extends keyof ERR>(data: ReplyErrorEnvelope<ERR, K> | ReplyErrorEnvelopator<ERR, K>) => void;

interface ReplyPayload<REQ, REP, ERR> extends BaseCallbackPayload<Msg, RequestEnvelope<REQ>> {
  data: REQ;
  replyWithData: DataReplier<REP>;
  replyWithError: ErrorReplier<ERR>;
}

export type Replier<RDS extends ReplierDefinitions, K extends keyof RDS, C extends {}> = (data: ReplyPayload<TypeOf<RDS[K]['request']>, TypeOf<RDS[K]['reply']>, RDS[K]['errors']> & C) => Promise<void> | void;

class ReplierManager<R extends ReplierDefinitions, C extends {} = {}> extends BaseManager<C> {
  
  constructor(connection: NatsConnection, config: BrokerConfig, private repliers: R, context: C) {
    super(connection, config, context);
  }

  reply<K extends keyof R>(subject: K, subscriber: Replier<R, K, C>, opts?: SubscriptionOptions): Subscription {
    const sub = this.connection.subscribe(subject as string, {
      queue: `${this.config.queueName}-${subject as string}`,
      ...opts,
    });
    (async (subscription) => {
      for await (const message of subscription) {
        this.withBasicPayload<Msg, RequestEnvelope<TypeOf<R[K]['request']>>>(subject as string, 'reply', message, async (payload) => {
          const replySubject = message.reply;
          if (!replySubject) {
            payload.span.addEvent('Missing reply subject');
            return;
          }
  
          const send = <T extends BaseEnvelope>(envelope: T, validationChain?: PropValidation, customEnvelopeSchema?: z.ZodObject<any>) => {
            const validation = super.validateEnvelope(envelope, validationChain, customEnvelopeSchema);
  
            // Reply payload validation failed, notify client of internal error and end span
            if (validation !== true) {
              replyWithError((builder) => builder({ code: 'INTERNAL_ERROR', data: validation }));
              throw validation;
            }
  
            this.connection.publish(replySubject, this.config.serializer(envelope));
          }
  
          const replyWithError: ErrorReplier<R[K]['errors']> = (data) => {
            const envelope = match(typeof data)
              .with('object', () => data as ReplyErrorEnvelope<R[K]['errors'], keyof R[K]['errors']>)
              .with('function', () => {
                const envelopator = data as ReplyErrorEnvelopator<R[K]['errors'], any>;
                return envelopator((err) => this.buildErrorEnvelope(subject as string, err));
              }).run();
  
            const validationChain = this.getErrorValidationChain(subject, envelope.error);
            send(envelope, validationChain, ErrorEnvelopeSchema);
          };
  
          const replyWithData: DataReplier<TypeOf<R[K]['reply']>> = (data) => {
            const envelope = match(typeof data)
              .with('object', () => data as ReplyDataEnvelope<TypeOf<R[K]['reply']>>)
              .with('function', () => {
                const envelopator = data as ReplyDataEnvelopator<TypeOf<R[K]['reply']>>;
                return envelopator((data) => this.buildDataEnvelope(subject as string, data));
              }).run();
              send(envelope, { data: envelope.data, schema: this.repliers[subject].reply }, DataEnvelopeSchema);
            }
  
          // Failed validation, reply with error and finish
          const requestValidation = super.validateEnvelope(payload.envelope, { data: payload.envelope.data, schema: this.repliers[subject].request });
          if (requestValidation !== true) {
            replyWithError((builder) => builder({ code: 'PAYLOAD_VALIDATION', data: requestValidation }));
            throw requestValidation;
          }

          const resPayload = {
            ...payload,
            data: payload.envelope.data,
            replyWithData,
            replyWithError,
          };

          try {
            await subscriber(resPayload);
          } catch (error: any) {
            replyWithError((b) => b({ code: 'INTERNAL_ERROR', data: error }));
            throw error;
          }
        });
      }
    })(sub);
    return sub;
  }

  async request<K extends keyof R, REQ = TypeOf<R[K]['request']>>(subject: K, data: RequestMethodData<REQ>, requestSpan?: Span, opts?: RequestOptions) {
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

    const validation = super.validateEnvelope(envelope, { data: envelope.data, schema: this.repliers[subject].request });
    if (validation !== true) {
      requestSpan?.recordException(validation);
      requestSpan?.setStatus({ code: SpanStatusCode.ERROR });
      throw validation;
    }

    const payload = this.config.serializer(envelope);
    const resMsg = await this.connection.request(subject as string, payload, { timeout: 1000, ...opts });
    const resData = this.config.deserializer<ReplyAllTypesEnvelope<TypeOf<R[K]['reply']>, R[K]['errors']>>(resMsg.data);
    let resValidation: true | ZodError<any> = true;
    if (resData.type === 'success') {
      resValidation = super.validateEnvelope(resData, { data: resData.data, schema: this.repliers[subject].reply }, DataEnvelopeSchema);
    } else {
      const replyValidationChain = this.getErrorValidationChain(subject, resData.error);
      resValidation = super.validateEnvelope(resData, replyValidationChain, ErrorEnvelopeSchema);
    }

    if (resValidation !== true) {
      // Reply payload is not of expected shape, reject
      requestSpan?.recordException(resValidation);
      requestSpan?.setStatus({ code: SpanStatusCode.ERROR });
      throw resValidation;
    }

    return resData;
  }

  private getErrorValidationChain<S extends keyof R>(subject: S, error: ReplyErrorPayload<any, any>): PropValidation | undefined {
    const { errors } = this.repliers[subject];
    if (errors && (error.code in errors) && ('safeParse' in errors[error.code])) {
      return { data: error.data, schema: errors[error.code] };
    }

    return undefined;
  }

  private buildErrorEnvelope<ERR, K extends keyof ERR>(subject: string, error: ReplyErrorPayload<ERR, K>): ReplyErrorEnvelope<ERR, K> {
    return {
      ...super.buildBaseEnvelope(subject),
      type: 'error',
      error,
    };
  }

  private buildDataEnvelope<T>(subject: string, data: T): ReplyDataEnvelope<T> {
    return {
      ...super.buildBaseEnvelope(subject),
      type: 'success',
      data,
    };
  }
}

export default ReplierManager;