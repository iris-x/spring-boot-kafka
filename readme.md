# Spring Boot REST application with Prometheus support

This is a template application for Spring Boot REST application instrumented to
expose Prometheus metrics.

## Endpoints used by Kubernetes

This quickstart exposes the following endpoints important for Kubernetes deployments:
- `/actuator/health` - Spring Boot endpoint indicating health. Used by Kubernetes as readiness probe.
- `/actuator/metrics` - Prometheus metrics. Invoked periodically and collected by Prometheus Kubernetes scraper.


## Kafka configuration

kafka-topics --zookeeper 10.110.215.78:2181 --list
kafka-topics --create --zookeeper 10.110.215.78:2181 --replication-factor 1 --partitions 1 --topic platsannons_visning_test

kafka-avro-console-consumer --bootstrap-server 164.135.124.52:9092 --topic platsannons_visning_test --from-beginning 

kafka-avro-console-consumer --bootstrap-server 164.135.124.52:9092 --topic test --from-beginning 
kafka-console-consumer --bootstrap-server 164.135.124.52:9092 --topic test --from-beginning 

kafka-topics --create --zookeeper 10.110.215.78:2181 --replication-factor 1 --partitions 1 --topic test_avro

kafka-console-producer --broker-list 164.135.124.52:9092 --topic test

kafka-avro-console-producer \
--broker-list 164.135.124.52:9092 --topic test_avro \
--property value.schema='{ "type": "record", "name": "value", "fields": [ {"name": "id", "type": "string", "default": "null"} ] }'


{"id": "value1"}
