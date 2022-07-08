import { z } from "zod";
import getBroker from "../src/Broker";

const subscriptions = {
  'foo.number': z.number(),
  'foo.object': z.object({
    str: z.string(),
    bol: z.boolean(),
  })
};

describe('publish-subscribe', () => {  

  const buildBroker = () => getBroker({}, {
    subscriptions,
    repliers: {},
    consumers: {}
    },
    {},
  );

  type Broker = Awaited<ReturnType<typeof buildBroker>>;
  let broker: Broker = undefined as any as Broker; // Dirty but works

  beforeAll(async () => broker = await buildBroker());

  it('Should support registering subscriptions', () => new Promise((resolve) => {
    broker.subscribe('foo.number', async ({ data }) => {
      resolve(expect(data).toBe(10));
    });

    broker.publish('foo.number', builder => builder(10));
  }));
});