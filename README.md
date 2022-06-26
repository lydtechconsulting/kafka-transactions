# Kafka Transactions Project

Spring Boot application demonstrating usage of the Kafka Transactional API to achieve exactly-once messaging semantics.

This repo accompanies the following articles:
- [Kafka Transactions: Exactly-Once Messaging](https://medium.com/lydtech-consulting/kafka-transactions-part-1-exactly-once-messaging-9949350281ff)
- [Kafka Transactions: Spring Boot Demo](https://medium.com/lydtech-consulting/kafka-transactions-part-2-spring-boot-demo-ce066713c7a7)

## Integration Tests

Run integration tests with `mvn clean test`

The tests demonstrate the behaviour when producing events with and without a surrounding transaction.
