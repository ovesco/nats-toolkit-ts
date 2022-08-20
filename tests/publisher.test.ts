import { z } from "zod";
import getBroker from "../src/Broker";

const subscriptions = {
  'foo.number.a': z.number(),
  'foo.number.b': z.number(),
  'foo.object': z.object({
    str: z.string(),
    bol: z.boolean(),
  })
};

const repliers = {
  'foo.ask': {
    request: z.number(),
    reply: z.number(),
  },
  'foo.error': {
    request: z.void(),
    reply: z.void(),
    errors: {
      YO: z.string(),
    }
  }
}

describe('publish-subscribe', () => {  

  const buildBroker = () => getBroker({
    subscriptions,
    repliers,
    consumers: {}
    });

  type Broker = Awaited<ReturnType<typeof buildBroker>>;
  let broker: Broker = undefined as any as Broker; // Dirty but works

  beforeAll(async () => broker = await buildBroker());

  it('Should support subscriptions with builder', () => new Promise((resolve) => {
    broker.subscribe('foo.number.a', async ({ data }) => resolve(expect(data).toBe(10)));
    broker.publish('foo.number.a', builder => builder(10));
  }));

  it('Should support subscriptions with custom envelope', () => new Promise((resolve) => {
    broker.subscribe('foo.number.b', async ({ data }) => resolve(expect(data).toBe(5)));
    broker.publish('foo.number.b', {
      id: '1',
      instance: '1',
      subject: 'foo.number.b',
      service: 'test',
      data: 5,
      date: Date.now(),
    });
  }));


  /*
  it('Should support registering repliers which respond', async () => {
    broker.reply('foo.ask', ({ data, replyWithData }) => replyWithData((b) => b(data + 10)));
    const envelope = await broker.request('foo.ask', (b) => b(10));
    expect(envelope?.type).toBe('success');
    expect((envelope as ReplyDataEnvelope<number>).data).toBe(20);
  });

  it('Should support registering repliers which fail', async () => {
    broker.reply('foo.error', ({ replyWithError }) => replyWithError((b) => b({ code: 'YO', data: 'error' })));
    const envelope = await broker.request('foo.error', (b) => b());
    expect(envelope?.type).toBe('error');
    expect((envelope as ReplyErrorEnvelope<any, any>).error.data).toBe('error');
  });
  */

  it('Should support closing connection', async () => {
    await broker.connection.close();
  });
});