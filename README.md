# Kafka Transactions Project

Spring Boot application demonstrating usage of the Kafka Transactional API to achieve exactly-once messaging semantics.

## Integration Tests

Run integration tests with `mvn clean test`

The tests demonstrate the behaviour when producing events with and without a surrounding transaction.
