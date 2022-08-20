/* eslint-disable @typescript-eslint/no-explicit-any */
import {z} from 'zod';
import getBroker from '../src/Broker';

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
    },
  },
};

describe('publish-subscribe', () => {
  const buildBroker = () =>
    getBroker({
      subscriptions: {},
      repliers,
      consumers: {},
    });

  type Broker = Awaited<ReturnType<typeof buildBroker>>;
  let broker: Broker = undefined as any as Broker; // Dirty but works

  // eslint-disable-next-line no-return-assign
  beforeAll(async () => (broker = await buildBroker()));

  it('Should support registering repliers which respond', async () => {
    broker.reply('foo.ask', ({data, replyWithData}) =>
      replyWithData(b => b(data + 10)),
    );
    const envelope = await broker.request('foo.ask', b => b(10));
    expect(envelope?.type).toBe('success');
    if (envelope.type === 'success') {
      expect(envelope.data).toBe(20);
    } else {
      fail(envelope.error);
    }
  });

  it('Should support registering repliers which fail', async () => {
    broker.reply('foo.error', ({replyWithError}) =>
      replyWithError(b => b({code: 'YO', data: 'error'})),
    );
    const envelope = await broker.request('foo.error', b => b());
    if (envelope.type === 'success') {
      fail(envelope);
    } else {
      expect(envelope.error.code).toBe('YO');
    }
  });

  it('Should support closing connection', async () => {
    await broker.connection.close();
  });
});
