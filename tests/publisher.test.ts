/* eslint-disable @typescript-eslint/no-explicit-any */
import {z} from 'zod';
import getBroker from '../src/Broker';

const subscriptions = {
  'foo.number.a': z.number(),
  'foo.number.b': z.number(),
  'foo.object': z.object({
    str: z.string(),
    bol: z.boolean(),
  }),
};

describe('publish-subscribe', () => {
  const buildBroker = () =>
    getBroker({
      subscriptions,
      repliers: {},
      consumers: {},
    });

  type Broker = Awaited<ReturnType<typeof buildBroker>>;
  let broker: Broker = undefined as any as Broker; // Dirty but works

  // eslint-disable-next-line no-return-assign
  beforeAll(async () => (broker = await buildBroker()));

  it('Should support subscriptions with builder', () =>
    new Promise(resolve => {
      broker.subscribe('foo.number.a', async ({data}) =>
        resolve(expect(data).toBe(10)),
      );
      broker.publish('foo.number.a', builder => builder(10));
    }));

  it('Should support subscriptions with custom envelope', () =>
    new Promise(resolve => {
      broker.subscribe('foo.number.b', async ({data}) =>
        resolve(expect(data).toBe(5)),
      );
      broker.publish('foo.number.b', {
        id: '1',
        instance: '1',
        subject: 'foo.number.b',
        service: 'test',
        data: 5,
        date: Date.now(),
      });
    }));

  it('Should support closing connection', async () => {
    await broker.connection.close();
  });
});
