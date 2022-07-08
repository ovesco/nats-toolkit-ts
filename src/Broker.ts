import { connect } from "nats";
import ConsumerManager, { ConsumerDefinitions } from "./actions/ConsumerManager";
import ReplierManager, { ReplierDefinitions } from "./actions/ReplierManager";
import SubscriberManager, { SubscriberDefinitions } from "./actions/SubscriberManager";
import { buildConfig, SetupConfig } from "./ConfigBuilder";

type TypesConfig<S, R, C> = {
  subscriptions?: S,
  repliers?: R,
  consumers?: C,
};

async function getBroker<
SUB extends SubscriberDefinitions,
REP extends ReplierDefinitions,
CON extends ConsumerDefinitions,
CTX extends {} = {}>(types: TypesConfig<SUB, REP, CON>, config?: SetupConfig, context?: CTX) {

  const natsOptions = config?.natsOptions ||undefined;
  const connection = await connect(natsOptions);
  const brokerConfig = buildConfig(config || {});
  const subscriberManager = new SubscriberManager(connection, brokerConfig, types.subscriptions as SUB || {}, context as CTX || {});
  const replierManager = new ReplierManager(connection, brokerConfig, types.repliers as REP || {}, context as CTX || {});
  const consumerManager = new ConsumerManager(connection, brokerConfig, types.consumers as CON || {}, context as CTX || {});

  return {
    connection,

    publish: subscriberManager.publish.bind(subscriberManager),
    subscribe: subscriberManager.subscribe.bind(subscriberManager),

    request: replierManager.request.bind(replierManager),
    reply: replierManager.reply.bind(replierManager),

    dispatch: consumerManager.dispatch.bind(consumerManager),
    consume: consumerManager.consume.bind(consumerManager),
  };
};

export default getBroker;