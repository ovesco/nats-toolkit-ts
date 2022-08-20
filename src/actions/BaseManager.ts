/* eslint-disable @typescript-eslint/no-explicit-any */
import {JsMsg, Msg, NatsConnection} from 'nats';
import {Link, Span, SpanContext, SpanStatusCode} from '@opentelemetry/api';
import {v4} from 'uuid';
import {z, ZodSchema} from 'zod';
import {BrokerConfig} from '../ConfigBuilder';
import {
  BaseEnvelope,
  BaseEnvelopeSchema,
  BaseCallbackPayload,
} from '../BaseTypes';

export type PropValidation = {data: any; schema: ZodSchema};

abstract class BaseManager<
  C extends Record<string, unknown> = Record<string, unknown>,
> {
  // eslint-disable-next-line no-useless-constructor
  constructor(
    protected connection: NatsConnection,
    protected config: BrokerConfig,
    protected context: C,
  ) {}

  protected validateEnvelope<T extends BaseEnvelope>(
    envelope: T,
    subProp?: PropValidation,
    customEnvelopeSchema: z.ZodObject<any> = BaseEnvelopeSchema,
    throwException = false,
  ) {
    const subSchemas: PropValidation[] = [
      {data: envelope, schema: customEnvelopeSchema},
    ];

    if (subProp) {
      subSchemas.push(subProp);
    }

    for (const {data, schema} of subSchemas) {
      const validation = schema.safeParse(data);
      if (!validation.success) {
        if (!this.config.schemaValidation) {
          // Override validation silently
          return true;
        }

        if (throwException) {
          throw validation.error;
        } else {
          return validation.error;
        }
      }
    }

    return true;
  }

  protected withSpan(
    subject: string,
    action: string,
    parentSpanContext: SpanContext | undefined,
    callback: (span: Span) => Promise<void>,
  ) {
    const links: Link[] = [];
    if (parentSpanContext) {
      links.push({context: parentSpanContext});
    }

    return new Promise((resolve, reject) => {
      this.config.tracer.startActiveSpan(
        subject,
        {
          links,
          attributes: {
            action,
            service: this.config.name,
            instance: this.config.id,
          },
        },
        span => {
          callback(span)
            .then(() => {
              span.end();
              resolve(undefined);
            })
            .catch(error => {
              span.recordException(error);
              span.setStatus({
                code: SpanStatusCode.ERROR,
              });
              span.end();
              reject(error);
            });
        },
      );
    });
  }

  protected withBasicPayload<M extends Msg | JsMsg, E extends BaseEnvelope>(
    subject: string,
    action: string,
    message: M,
    callback: (payload: BaseCallbackPayload<M, E> & C) => Promise<void>,
  ) {
    const envelope = this.config.deserializer<E>(message.data);
    return this.withSpan(subject, action, envelope.spanContext, async span => {
      const basePayload = {
        message,
        envelope,
        span,
        tracer: this.config.tracer,
        logger: this.config.logger,
        ...this.context,
      };
      await callback(basePayload);
    });
  }

  protected buildBaseEnvelope(
    subject: string,
    parentSpan?: Span,
  ): BaseEnvelope {
    return {
      id: v4(),
      subject,
      date: Date.now(),
      service: this.config.name,
      instance: this.config.id,
      spanContext: parentSpan?.spanContext(),
    };
  }
}

export default BaseManager;
