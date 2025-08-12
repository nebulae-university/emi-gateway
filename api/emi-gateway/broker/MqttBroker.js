'use strict';

var mqtt = require('mqtt');
const { map, switchMap, filter, first, timeout } = require('rxjs/operators');
const { of, Subject, timer } = require('rxjs');
const uuidv4 = require('uuid/v4');

class MqttBroker {

    constructor({ gatewayRepliesTopic, mqttServerUrl, replyTimeout, gatewayEventsTopic, materializedViewTopic, connOps }) {
        this.gatewayRepliesTopic = gatewayRepliesTopic;
        this.gatewayEventsTopic = gatewayEventsTopic;
        this.materializedViewTopic = materializedViewTopic;
        this.mqttServerUrl = mqttServerUrl;
        this.senderId = uuidv4();
        this.replyTimeout = replyTimeout;
        this.connOps = connOps;
        /**
         * Rx Subject for every message reply
         */
        this.replies$ = new Subject();

        /**
         * MQTT Client
         */
        this.resetStats();
        //this.startStats();
        this.mqttClient = mqtt.connect(this.mqttServerUrl, connOps);
        this.configMessageListener();
    }



    /**
     * Forward the Graphql query/mutation to the Microservices
     * @param {string} topic topic to publish
     * @param {string} type message(payload) type
     * @param {Object} message payload {root,args,jwt}
     * @param {Object} ops {correlationId, messageId} 
     */
    forward$(topic, type, payload, ops = {}) {
        return this.publish$(topic, type, payload, ops);
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
        return this.replies$
            .pipe(
                filter(msg => msg),
                filter(msg => msg.topic === this.gatewayRepliesTopic),
                filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId),
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
        return this.replies$
            .pipe(
                filter(msg => msg),
                filter(msg => msg.topic === this.gatewayEventsTopic),
                filter(msg => types ? types.indexOf(msg.type) !== -1 : true),
                filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            );

    }

    /**
     * Returns an observable listen to messages from MaterializedViewsUpdate topic.
     * @param {array} types Message types to filter. if undefined means all types
     * @param {number} timeout 
     */
    getMaterializedViewsUpdates$(types, ignoreSelfEvents = true) {
        return this.replies$
            .pipe(
                filter(msg => msg),
                filter(msg => msg.topic === this.materializedViewTopic),
                filter(msg => types ? types.indexOf(msg.type) !== -1 : true),
                filter(msg => !ignoreSelfEvents || msg.attributes.senderId !== this.senderId)
            );
    }

    /**
     * Publish data throught a topic
     * Returns an Observable that resolves to the sent message ID
     * @param {string} topicName 
     * @param {string} type message(payload) type
     * @param {Object} data 
     * @param {Object} ops {correlationId, messageId} 
     */
    publish$(topicName, type, data, { correlationId, messageId } = {}) {
        const uuid = messageId || uuidv4();
        const dataBuffer = JSON.stringify(
            {
                id: uuid,
                type: type,
                data,
                attributes: {
                    senderId: this.senderId,
                    correlationId,
                    replyTo: this.gatewayRepliesTopic
                }
            }
        );

        return of({})
            .pipe(
                map(() => {
                    this.mqttClient.publish(`${topicName}`, dataBuffer, { qos: 0 });
                    return uuid;
                })
            );
    }


    /**
     * Configure to listen messages
     */
    configMessageListener() {
        const that = this;
        this.mqttClient.on('connect', function () {
            that.mqttClient.subscribe(`${that.gatewayRepliesTopic}`);
            that.mqttClient.subscribe(`${that.gatewayEventsTopic}`);
            that.mqttClient.subscribe(`${that.materializedViewTopic}`);
            console.log(`Mqtt client subscribed to ${that.gatewayRepliesTopic},  ${that.gatewayEventsTopic} and ${that.materializedViewTopic}`);
        });

        this.mqttClient.on('message', function (topic, message) {
            try {
                const envelope = JSON.parse(message);
                if (!that.stats[topic][envelope.type]) {
                    that.stats[topic][envelope.type] = 0;
                }
                that.stats[topic][envelope.type]++;
                // message is Buffer
                that.replies$.next(
                    {
                        topic: topic,
                        id: envelope.id,
                        type: envelope.type,
                        data: envelope.data,
                        attributes: envelope.attributes,
                        correlationId: envelope.attributes.correlationId
                    }
                );
            } catch (error) {
                console.error(new Date().toUTCString(), ': Mqttbroker.onMessage: ERROR', error);
            }
        });
    }

    startStats() {
        this.resetStats();
        timer(5000, 60000).subscribe(
            (evt) => {
                console.log('MqttBroker.stats: ', JSON.stringify(this.stats, null, 1));
                this.resetStats();
            },
            (err) => { console.log('MqttBroker.stats: error', err); },
            () => { console.log('MqttBroker.stats: completed'); },
        );
        console.log('MqttBroker.stats: started');
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
        this.mqttClient.end();
    }
}

module.exports = MqttBroker;