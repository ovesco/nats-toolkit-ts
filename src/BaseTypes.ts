import {JsMsg, Msg} from 'nats';
import {Span, SpanContext, Tracer} from '@opentelemetry/api';
import {Logger} from 'pino';
import {z} from 'zod';

export type BaseCallbackPayload<
  M extends Msg | JsMsg,
  E extends BaseEnvelope,
> = {
  message: M;
  envelope: E;
  span: Span;
  tracer: Tracer;
  logger: Logger;
};

export type BaseEnvelope = {
  date: number;
  id: string;
  subject: string;
  service: string;
  instance: string | number;
  spanContext?: SpanContext;
};

export const BaseEnvelopeSchema = z.object({
  id: z.string(),
  subject: z.string(),
  service: z.string(),
  instance: z.union([z.string(), z.number()]),
});
