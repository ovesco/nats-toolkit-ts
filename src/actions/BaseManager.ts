import { headers, JsMsg, Msg, NatsConnection } from 'nats';
import { FORMAT_HTTP_HEADERS, Span } from 'opentracing';
import { v4 } from 'uuid';
import { ZodSchema } from 'zod';
import { BrokerConfig } from '../ConfigBuilder';
import { BaseEnvelope, BaseEnvelopeSchema, Envelope } from '../Messaging';
import { BasePayload } from '../Messaging';

export type ValidationChainElement<T> = [keyof T | undefined, ZodSchema];

abstract class BaseManager<C extends {} = {}> {
  
  constructor(protected connection: NatsConnection, protected config: BrokerConfig, protected context: C) {
  }

  protected buildBasicPayload<T, M extends Msg | JsMsg>(subject: string | number | symbol, message: M): BasePayload<T, M> & C {
    const envelope = this.config.deserializer<Envelope<T>>(message.data);
    return {
      message,
      envelope,
      data: envelope.data,
      tracer: this.config.tracer,
      logger: this.config.logger,
      span: this.extractSpan(subject as string, message),
      ...this.context,
    };
  }

  protected validateEnvelope<T extends BaseEnvelope>(data: T, subProp?: ValidationChainElement<T>) {
    const subSchemas: ValidationChainElement<T>[] = [
      [undefined, BaseEnvelopeSchema],
    ];

    if (subProp) {
      subSchemas.push(subProp);
    }

    for (const [accessor, schema] of subSchemas) {
      const prop = accessor ? data[accessor] : data;
      const validation = schema.safeParse(prop);
      if (!validation.success) {
        return validation.error;
      }
    }

    return true;
  }

  protected buildBaseEnvelope(subject: string): BaseEnvelope {
    return {
      id: v4(),
      subject,
      date: Date.now(),
      service: this.config.name,
      instance: this.config.id,
    };
  }

  protected buildSpan(title: string, action: string, parentSpan?: Span) {
    return this.config.tracer.startSpan(title, {
      childOf: parentSpan,
      tags: {
        action,
        service: this.config.name,
        instance: this.config.id,
      },
    });
  }
  
  protected extractSpan(title: string, message: Msg | JsMsg) {
    const headers: Record<string, string> = {};
    if (message.headers) {
      for (const [key, value] of message.headers) {
        headers[key] = Array.isArray(value) ? value[0] : value as string;
      }
    }
  
    const span = this.config.tracer.startSpan(title, {
      childOf: this.config.tracer.extract(FORMAT_HTTP_HEADERS, headers) || undefined,
      tags: {
        service: this.config.name,
        instance: this.config.id,
      }
    });
  
    return span;
  };
  
  protected buildMessageHeaders(span: Span) {
    const h = headers();
    const context: Record<string, string> = {};
    this.config.tracer.inject(span.context(), FORMAT_HTTP_HEADERS, context);
    for (const [key, value] of Object.entries(context)) {
      h.append(key, value);
    }
  
    return h;
  }
}

export default BaseManager;