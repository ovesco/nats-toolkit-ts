# Nats Typescript Toolkit

## This is a work in progress actively under development

A toolkit to build distributed services in Typescript on top of Nats.
It exposes a straightforward API abstracting most of the complexity of using
Nats with sensible defaults and fully typed. It takes care of common scenarios
such as payload validation, retry and more.

### Example
```Typescript
// Declare your payload types using zod schemas
const subscriptions = {
  'foo.bar': z.object({
    message: z.string(),
  }),
};

// Build a broker, passing in your types
const broker = await getBroker({ subscriptions });

// Use the broker with complete type inference
broker.subscribe('foo.bar', ({ data }) => {
  console.log(data.message); // Type = { message: string }
});

broker.publish('foo.bar', (wrap) => wrap({ message: 'hello' }));
broker.publish('foo.bar', (wrap) => wrap(10)); // Argument of type 'number' is not assignable to parameter of type '{ message: string; }'
```