package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	fmt.Println("Hello, go!")
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
