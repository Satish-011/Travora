const { producer, connectProducer } = require('../../config/kafka');
const logger = require('../../config/logger');
const KAFKA_TOPICS = require('../../../../shared/constants/kafka-topics');

const sendOtpEmail = async (email, otp, ttlMinutes) => {
     try {
          await connectProducer();
          await producer.send({
               topic: KAFKA_TOPICS.NOTIFICATION_OTP_EMAIL,
               messages: [
                    {
                         key: email,
                         value: JSON.stringify({ email, otp, ttlMinutes })
                    }
               ]
          });
          logger.info(`OTP email message published to Kafka for ${email}`);
     } catch (error) {
          logger.error(`Failed to publish OTP email message for ${email}:`, error);
          throw error;
     }
};

const sendWelcomeEmail = async (email, firstName) => {
     try {
          await connectProducer();
          await producer.send({
               topic: KAFKA_TOPICS.NOTIFICATION_WELCOME_EMAIL,
               messages: [
                    {
                         key: email,
                         value: JSON.stringify({ email, firstName })
                    }
               ]
          });
          logger.info(`Welcome email message published to Kafka for ${email}`);
     } catch (error) {
          logger.error(`Failed to publish Welcome email message for ${email}:`, error);
          throw error;
     }
};

module.exports = {
     sendOtpEmail,
     sendWelcomeEmail
};
