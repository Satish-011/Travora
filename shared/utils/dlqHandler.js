/**
 * Kafka Dead Letter Queue (DLQ) & Consumer Retry Handler
 * 
 * Automatically wraps Kafka consumers with retry logic (up to 3 retries by default)
 * and publishes unrecoverable failed messages to the service's designated DLQ topic (dlq.<service>).
 */

const KAFKA_TOPICS = require('../constants/kafka-topics');

/**
 * Publishes a poisoned/failed message to the Dead Letter Queue
 */
async function publishToDLQ(producer, serviceName, topic, partition, message, error) {
     const dlqTopic = KAFKA_TOPICS.getDLQTopic(serviceName);

     const dlqPayload = {
          topic: dlqTopic,
          messages: [
               {
                    key: message.key,
                    value: message.value,
                    headers: {
                         ...(message.headers || {}),
                         'dlq-original-topic': topic,
                         'dlq-original-partition': String(partition),
                         'dlq-original-offset': String(message.offset || ''),
                         'dlq-error-message': error ? error.message : 'Unknown error',
                         'dlq-service': serviceName,
                         'dlq-failed-at': new Date().toISOString()
                    }
               }
          ]
     };

     try {
          if (producer && typeof producer.send === 'function') {
               await producer.send(dlqPayload);
          }
     } catch (dlqErr) {
          console.error(`[DLQ_FATAL] Failed to send message to DLQ topic "${dlqTopic}":`, dlqErr.message);
     }
}

/**
 * Higher-order function to wrap a Kafka consumer's eachMessage handler with retries and DLQ forwarding
 * 
 * @param {Object} producer - Connected KafkaJS producer
 * @param {string} serviceName - Name of the consuming service (e.g. 'search-service')
 * @param {Function} handler - The async message processing function ({ topic, partition, message, heartbeat })
 * @param {Object} [options]
 * @param {number} [options.maxRetries=3] - Maximum retry attempts before routing to DLQ
 * @param {number} [options.retryDelayMs=1000] - Base delay between retries
 * @returns {Function} Express/KafkaJS compatible message handler
 */
function wrapConsumerWithDLQ(producer, serviceName, handler, options = {}) {
     const maxRetries = options.maxRetries !== undefined ? options.maxRetries : 3;
     const retryDelayMs = options.retryDelayMs !== undefined ? options.retryDelayMs : 1000;

     return async function eachMessagePayload({ topic, partition, message, heartbeat }) {
          let attempt = 0;
          let lastError = null;

          while (attempt <= maxRetries) {
               try {
                    return await handler({ topic, partition, message, heartbeat });
               } catch (err) {
                    attempt++;
                    lastError = err;

                    if (attempt <= maxRetries) {
                         const delay = retryDelayMs * Math.pow(2, attempt - 1);
                         if (heartbeat && typeof heartbeat === 'function') {
                              try { await heartbeat(); } catch (_) {}
                         }
                         await new Promise(resolve => setTimeout(resolve, delay));
                    }
               }
          }

          // All retries exhausted -> Route to DLQ
          console.error(
               `[DLQ] Consumer "${serviceName}" failed processing message on topic "${topic}" ` +
               `after ${maxRetries} retries. Routing to DLQ.`
          );

          await publishToDLQ(producer, serviceName, topic, partition, message, lastError);
     };
}

module.exports = {
     wrapConsumerWithDLQ,
     publishToDLQ
};
