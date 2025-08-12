'use strict';

const os = require('os');

let instance = null;
class BrokerFactory {
  constructor() {
    //Service Port
    const BROKER_TYPE = process.env.BROKER_TYPE || 'PUBSUB';
    const replicaInstanceSrt = os.hostname().split('-').pop() || '';
    const replicaInstanceNumber = isNaN(replicaInstanceSrt) ? '' : '-' + parseInt(replicaInstanceSrt);

    const gatewayRepliesTopic = `${process.env.GATEWAY_REPLIES_TOPIC}${replicaInstanceNumber}`;
    const materializedViewTopic = process.env.GATEWAY_MATERIALIZED_VIEW_UPDATES_TOPIC;

    switch (BROKER_TYPE) {
      case 'PUBSUB':

        const PubSubBroker = require('./PubSubBroker');
        this.broker = new PubSubBroker({
          replyTimeOut: process.env.REPLY_TIMEOUT || 2000,
          projectId: process.env.GCLOUD_PROJECT_ID,
          gatewayRepliesTopic,
          gatewayRepliesTopicSubscription: process.env.GATEWAY_REPLIES_TOPIC_SUBSCRIPTION,
          gatewayEventsTopic: process.env.GATEWAY_EVENTS_TOPIC,
          gatewayEventsTopicSubscription: process.env.GATEWAY_EVENTS_TOPIC_SUBSCRIPTION,
          materializedViewTopic,
          materializedViewTopicSubscription: process.env.GATEWAY_MATERIALIZED_VIEW_UPDATES_TOPIC_SUBSCRIPTION,
        });

        try {
          if (process.env.SECONDARY_BROKER_ENABLED === 'true') {
            const SecondaryMqttBroker = require('./MqttBroker');
            const connOps = {
              host: process.env.MQTT_SERVER_URL,
              clientId: os.hostname(),
            };
            if (process.env.MQTT_PORT) connOps.port = process.env.MQTT_PORT;
            if (process.env.MQTT_USERNAME) connOps.username = process.env.MQTT_USERNAME;
            if (process.env.MQTT_PASSWORD) connOps.password = process.env.MQTT_PASSWORD;
            if (process.env.MQTT_PROTOCOL) connOps.protocol = process.env.MQTT_PROTOCOL;
            if (process.env.MQTT_PROTOCOL_VERSION) connOps.protocolVersion = parseInt(process.env.MQTT_PROTOCOL_VERSION);


            this.broker.secondaryBroker = new SecondaryMqttBroker({
              gatewayRepliesTopic,
              gatewayEventsTopic: process.env.GATEWAY_EVENTS_TOPIC,
              materializedViewTopic,
              projectId: process.env.GCLOUD_PROJECT_ID,
              mqttServerUrl: process.env.MQTT_SERVER_URL,
              replyTimeout: process.env.REPLY_TIMEOUT || 2000,
              connOps
            });
          }
        } catch (error) {
          console.error('===== SecondaryBroker ERROR:', error);
        }

        break;
      case 'MQTT':
        const MqttBroker = require('./MqttBroker');
        this.broker = new MqttBroker({
          gatewayRepliesTopic,
          gatewayEventsTopic: process.env.GATEWAY_EVENTS_TOPIC,
          materializedViewTopic,
          projectId: process.env.GCLOUD_PROJECT_ID,
          mqttServerUrl: process.env.MQTT_SERVER_URL,
          replyTimeout: process.env.REPLY_TIMEOUT || 2000
        });
        break;
    }
  }
  /**
   * Get the broker instance
   */
  getBroker() {
    return this.broker;
  }
}

module.exports = () => {
  if (!instance) {
    instance = new BrokerFactory();
    console.log('NEW BrokerFactory instance created.  broker=', instance.broker !== undefined, ', secondary=', instance.broker.secondaryBroker !== undefined);
  }
  return instance.broker;
};