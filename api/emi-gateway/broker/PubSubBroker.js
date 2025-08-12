'use strict'

const { map, switchMap, filter, first, timeout, mergeMap, tap } = require('rxjs/operators');
const { Subject, of, from, defer, ReplaySubject, Observable, timer } = require('rxjs');
const uuidv4 = require('uuid/v4');

class PubSubBroker {

    constructor({ projectId, gatewayRepliesTopic, gatewayRepliesTopicSubscription, gatewayEventsTopic, gatewayEventsTopicSubscription, replyTimeOut, materializedViewTopic, materializedViewTopicSubscription }) {
        this.projectId = projectId;
        this.gatewayRepliesTopic = gatewayRepliesTopic;
        this.gatewayRepliesTopicSubscription = gatewayRepliesTopicSubscription;
        this.gatewayEventsTopic = gatewayEventsTopic;
        this.gatewayEventsTopicSubscription = gatewayEventsTopicSubscription;
        this.materializedViewTopic = materializedViewTopic;
        this.materializedViewTopicSubscription = materializedViewTopicSubscription;
        this.replyTimeOut = replyTimeOut;

        /**
         * Rx Subject for every message reply
         */
        this.subjects = {};
        this.subjects[this.gatewayRepliesTopic] = new ReplaySubject(20);
        this.subjects[this.gatewayEventsTopic] = new Subject(0);
        this.subjects[this.materializedViewTopic] = new Subject(0);
        this.senderId = uuidv4();
        /**
         * Map of verified topics
         */
        this.verifiedTopics = {};

        const PubSub = require('@google-cloud/pubsub');
        this.pubsubClient = new PubSub({
            projectId: this.projectId,
        });

        //lets start listening to messages
        this.startMessageListener();
        //this.startStats();
    }


    /**
     * Forward the Graphql query/mutation to the Microservices
     * @param {string} topic topic to publish
     * @param {string} type message(payload) type
     * @param { {root,args,jwt} } message payload {root,args,jwt}
     * @param {Object} ops {correlationId, messageId} 
     */
    forward$(topic, type, payload, ops = {}) {
        return this.getTopic$(topic).pipe(
            switchMap(topic => this.publish$(topic.name, type, payload, ops))
        );
    }

    /**
     * Forward the Graphql query/mutation to the Microservices
     * @param {string} topic topic to publish
     * @param {string} type message(payload) type
     * @param { {root,args,jwt} } message payload {root,args,jwt}
     * @param {number} timeout wait timeout millis
     * @param {boolean} ignoreSelfEvents ignore messages comming from this clien
     * @param {Object} ops {correlationId, messageId}
     * 
     * Returns an Observable that resolves the message response
     */
    forwardAndGetReply$(topic, type, payload, timeout = this.replyTimeout, ignoreSelfEvents = true, ops) {
        return this.forward$(topic, type, payload, ops)
            .pipe(
                switchMap((messageId) => this.getMessageReply$(messageId, timeout, ignoreSelfEvents))
            );
    }


    /**
    * Returns an observable that waits for the message response or throws an error if timeout is exceded
    * The observable extract the message.data and resolves to it
    * @param {string} correlationId 
    * @param {number} timeoutLimit 
    */
    getMessageReply$(correlationId, timeoutLimit = this.replyTimeout, ignoreSelfEvents = true) {
        //console.log('getMessageReply$', new Date(), "=>",correlationId);
        return this.subjects[this.gatewayRepliesTopic]
            .pipe(
                filter(msg => msg),
                //filter(msg => msg.topic === this.gatewayRepliesTopic),
                filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId),
                //.do(msg => console.log("msg.correlationId => ",msg.correlationId, " Correlation => ", correlationId))
                filter(msg => msg && msg.correlationId === correlationId),
                map(msg => msg.data),
                timeout(timeoutLimit),
                first()

            );
    }

    /**
     * Returns an observable listen to events, and returns the entire message
     * @param {array} types Message types to filter. if undefined means all types
     * @param {number} timeout 
     */
    getEvents$(types, ignoreSelfEvents = true) {
        return this.subjects[this.gatewayEventsTopic]
            .pipe(
                filter(msg => msg),
                //filter(msg => msg.topic === this.gatewayEventsTopic),
                filter(msg => types ? types.indexOf(msg.type) !== -1 : true),
                filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            );
    }

    /**
     * Publish data throught a topic
     * Returns an Observable that resolves to the sent message ID
     * @param {Topic} topicName 
     * @param {string} type message(payload) type
     * @param {Object} data 
     * @param {Object} ops {correlationId, messageId} 
     */
    publish$(topicName, type, data, { correlationId, messageId } = {}) {
        const dataBuffer = Buffer.from(JSON.stringify(data));

        return this.getTopic$(topicName)
            .pipe(
                mergeMap(topic =>
                    // Observable.fromPromise(
                    defer(() => topic.publisher().publish(dataBuffer,
                        {
                            senderId: this.senderId,
                            correlationId,
                            type,
                            replyTo: this.gatewayRepliesTopic
                        })
                    )

                    // )
                ),
                //tap(messageId => console.log(`Message published through ${topicName}, MessageId=${messageId}`, new Date()))
            );
    }


    /**
     * Returns an observable listen to messages from MaterializedViewsUpdate topic.
     * @param {array} types Message types to filter. if undefined means all types
     * @param {number} timeout 
     */
    getMaterializedViewsUpdates$(types, ignoreSelfEvents = true) {
        //console.log('getMaterializedViewsUpdates$1 ', types);
        return this.subjects[this.materializedViewTopic]
            .pipe(
                filter(msg => msg),
                //filter(msg => msg.topic === this.materializedViewTopic),
                filter(msg => types ? types.indexOf(msg.type) !== -1 : true),
                filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            )
    }



    /**
     * Gets an observable that resolves to the topic object
     * @param {string} topicName 
     */
    getTopic$(topicName) {
        //Tries to get a cached topic
        const cachedTopic = this.verifiedTopics[topicName];
        if (!cachedTopic) {
            //if not cached, then tries to know if the topic exists
            const topic = this.pubsubClient.topic(topicName);
            return defer(() => topic.exists())
                .pipe(
                    map(data => data[0]),
                    switchMap(exists => {
                        if (exists) {
                            //if it does exists, then store it on the cache and return it
                            this.verifiedTopics[topicName] = topic;
                            console.log(`Topic ${topicName} already existed and has been set into the cache`);
                            return of(topic);
                        } else {
                            //if it does NOT exists, then create it, store it in the cache and return it
                            return this.createTopic$(topicName);
                        }
                    })
                );
        }
        //return cached topic
        return of(cachedTopic);
    }

    /**
     * Creates a Topic and return an observable that resolves to the created topic
     * @param {string} topicName 
     */
    createTopic$(topicName) {
        return defer(() => this.pubsubClient.createTopic(topicName))
            .pipe(
                switchMap(() => {
                    this.verifiedTopics[topicName] = this.pubsubClient.topic(topicName);
                    console.log(`Topic ${topicName} have been created and set into the cache`);
                    return of(this.verifiedTopics[topicName]);
                })
            );
    }



    /**
     * Returns an Observable that resolves to the subscription
     * @param {string} topicName 
     * @param {string} subscriptionName 
     */
    getSubscription$(topicName, subscriptionName) {
        return this.getTopic$(topicName)
            .pipe(
                // tap(topic => console.log('getTopic => ', topic.name)),
                mergeMap(topic => defer(() => topic.subscription(subscriptionName).get({ autoCreate: true }))),
                map(results => ({
                    subscription: results[0],
                    topicName,
                    subscriptionName
                }))
            )
    }

    /**
     * Starts to listen messages
     */
    startMessageListener() {
        of({})
            .pipe(
                mergeMap(() => from([
                    { topicName: this.gatewayRepliesTopic, topicSubscriptionName: this.gatewayRepliesTopicSubscription },
                    //{ topicName: this.gatewayEventsTopic, topicSubscriptionName: this.gatewayEventsTopicSubscription },
                    { topicName: this.materializedViewTopic, topicSubscriptionName: this.materializedViewTopicSubscription }
                ])),
                mergeMap(({ topicName, topicSubscriptionName }) => this.getSubscription$(topicName, topicSubscriptionName))
            )
            .subscribe(
                ({ subscription, topicName, subscriptionName }) => {
                    subscription.on(`message`, message => {
                        //console.log('Received message response', new Date(), topicName, message.attributes.correlationId);
                        const innerSubject = this.subjects[topicName];

                        if (!this.stats[topicName][message.attributes.type]) {
                            this.stats[topicName][message.attributes.type] = 0;
                        }
                        this.stats[topicName][message.attributes.type]++;

                        innerSubject.next(
                            {
                                topic: topicName,
                                id: message.id,
                                type: message.attributes.type,
                                data: JSON.parse(message.data),
                                attributes: message.attributes,
                                correlationId: message.attributes.correlationId
                            }
                        );
                        message.ack();
                        //console.log('****** ACK MESSAGE ', message.attributes.correlationId);
                    });
                    console.log(`PubSubBroker is listening to ${topicName} under the subscription ${subscriptionName}`);
                },
                (err) => {
                    console.error(`Failed to obtain subscription `, err);
                },
                () => {
                    //console.log('GatewayReplies listener has completed!');
                }
            );

    }

    startStats() {
        this.resetStats();
        timer(5000, 60000).subscribe(
            (evt) => {
                console.log('PubSubBroker.stats: ', JSON.stringify(this.stats, null, 1));
                this.resetStats();
            },
            (err) => { console.log('PubSubBroker.stats: error', err); },
            () => { console.log('PubSubBroker.stats: completed'); },
        );
        console.log('PubSubBroker.stats: started');
    }

    resetStats() {
        this.stats = {};
        this.stats[this.gatewayRepliesTopic] = {};
        this.stats[this.gatewayEventsTopic] = {};
        this.stats[this.materializedViewTopic] = {};
    }

    /**
     * Stops broker 
     */
    disconnectBroker() {
        this.getSubscription$(this.gatewayRepliesTopic, this.gatewayRepliesTopicSubscription)
            .subscribe(
                ({ topicName, topicSubscriptionName }) => subscription.removeListener(`message`),
                (error) => console.error(`Error disconnecting Broker`, error),
                () => console.log('Broker disconnected')
            );
    }
}

module.exports = PubSubBroker;