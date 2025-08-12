const withFilter = require('graphql-subscriptions').withFilter;
const PubSub = require('graphql-subscriptions').PubSub;
const pubsub = new PubSub();
const { of, Observable } = require('rxjs');

module.exports = {
  Query: {
    author(_, { firstName, lastName }, context, info) {
      return { id: '1233', firstName, lastName, age:  23};
    },
  },
  Mutation: {
    createAuthor: (root, args, context, info) => {
      const authorAdded = { id: Math.random(), firstName: args.firstName, lastName: args.lastName };
      pubsub.publish('authorAdded', { authorAdded });
      return authorAdded;
    },
  },
  Subscription: {
    authorEvent: {
      subscribe: withFilter((payload, variables, context, info) => {
        const subscription = context.broker.getEvents$(['authorEvent']).subscribe(
          evt => {
            console.log(`authorEvent received: ${JSON.stringify({ authorEvent: evt.data })}`);
            pubsub.publish('authorEvent', { authorEvent: evt.data })
          },
          (error) => {
            console.error('Error listening authorEvent', error);
            process.exit(1);
          },
          () => {
            console.log('authorEvent listener STOPED :D');
            process.exit(1);
          }
        );

        context.webSocket.onUnSubscribe = Observable.create((observer) => {
          subscription.unsubscribe();
          observer.next('rxjs subscription had been terminated');
          observer.complete();
        });
        return pubsub.asyncIterator('authorEvent');
      },
        (payload, variables, context, info) => {
          //return payload.authorEvent.lastName === variables.lastName; 
          return true;
        }),
    },
    authorAdded: {
      subscribe(payload, variables, context, info) {
        context.webSocket.onUnSubscribe = of('ACTION RX STREAM');
        return pubsub.asyncIterator('authorAdded');
      },
    }
  },
}

