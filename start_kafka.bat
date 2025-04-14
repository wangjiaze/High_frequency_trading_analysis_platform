@echo off

Z:
cd Z:\kafka\kafka_2.13-4.0.0\kafka_2.13-4.0.0
java -cp "libs/*" kafka.Kafka config\server.properties

