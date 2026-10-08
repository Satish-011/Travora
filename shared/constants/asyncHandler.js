/**
 * Express Async Handler Wrapper
 * Catches rejected promises from async route handlers and forwards them to Express error middleware
 */

module.exports = fn => (req, res, next) => {
     Promise.resolve(fn(req, res, next)).catch(next);
};
