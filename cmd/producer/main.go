package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	producer := NewKafkaProducer()
	Publish("Hello, kafka!", "teste", producer, nil)
	producer.Flush(1000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "poc-kafka_kafka_1:9092",
	}

	producer, error := kafka.NewProducer(configMap)

	if error != nil {
		log.Println(error.Error())
	}

	return producer
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}

	error := producer.Produce(message, nil)

	if error != nil {
		return error
	}

	return nil
}
