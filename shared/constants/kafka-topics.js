/**
 * Centralized Kafka Topics for Travora Microservices
 * Single source of truth for all Kafka topic names and Dead-Letter Queue helpers
 */

const KAFKA_TOPICS = {
     // Notification Topics
     NOTIFICATION_OTP_EMAIL: 'notification.otp-email',
     NOTIFICATION_WELCOME_EMAIL: 'notification.welcome-email',

     // Admin Domain Events
     ADMIN_STATION_CREATED: 'admin.station-created',
     ADMIN_TRAIN_CREATED: 'admin.train-created',
     ADMIN_ROUTE_CREATED: 'admin.route-created',
     ADMIN_SCHEDULE_CREATED: 'admin.schedule-created',
     ADMIN_SCHEDULE_CANCELLED: 'admin.schedule-cancelled',

     // Inventory Events
     INVENTORY_SEAT_AVAILABILITY_UPDATED: 'inventory.seat-availability-updated',

     // Payment Events
     PAYMENT_SUCCESS: 'payment.success',
     PAYMENT_FAILED: 'payment.failed',

     // Booking Events
     BOOKING_CONFIRMED: 'booking.confirmed',
     BOOKING_FAILED: 'booking.failed',
     BOOKING_CANCELLED: 'booking.cancelled',

     // Helper function for Dead Letter Queue topics
     getDLQTopic: (serviceName) => `dlq.${serviceName}`
};

module.exports = KAFKA_TOPICS;
