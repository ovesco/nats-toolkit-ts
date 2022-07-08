import { JsMsg, Msg } from "nats";
import { Span, Tracer } from "opentracing";
import { Logger } from "pino";
import { z } from "zod";

export type BasePayload<T, M extends Msg | JsMsg> = {
  message: M,
  envelope: Envelope<T>,
  data: T,
  span: Span,
  tracer: Tracer,
  logger: Logger,
};

export type BaseEnvelope = {
  date: number;
  id: string;
  subject: string;
  service: string;
  instance: string | number;
}

export type Envelope<T> = BaseEnvelope & {
  data: T;
}

export const BaseEnvelopeSchema = z.object({
  id: z.string(),
  subject: z.string(),
  service: z.string(),
  instance: z.union([z.string(), z.number()]),
});

export const EnvelopeSchema = BaseEnvelopeSchema.extend({
  data: z.any(),
});