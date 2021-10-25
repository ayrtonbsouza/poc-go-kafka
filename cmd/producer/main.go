package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChannel := make(chan kafka.Event)

	producer := NewKafkaProducer()
	Publish("Hello, kafka!", "teste", producer, nil, deliveryChannel)

	go DeliveryReport(deliveryChannel)

	// e := <-deliveryChannel
	// msg := e.(*kafka.Message)

	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println("Erro ao enviar mensagem")
	// } else {
	// 	fmt.Println("Mensagem enviada:", msg.TopicPartition)
	// }

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

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
	}

	error := producer.Produce(message, deliveryChan)

	if error != nil {
		return error
	}

	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar mensagem")
			} else {
				fmt.Println("Mensagem enviada:", ev.TopicPartition)
			}
		}
	}
}
