package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)

	producer := NewKafkaProducer()

	// Publish("mensagem", "teste", producer, nil, deliveryChan) // como a key é igual a nil a mensagem tende a ir para partições diferentes
	Publish("mensagem", "teste", producer, []byte("transferencia"), deliveryChan) // ao definir uma key, as mensagens irão sempre para mesma partição
	go DeliveryReport(deliveryChan) // async
	producer.Flush(2000)
	// e := <-deliveryChan

	// msg := e.(*kafka.Message)

	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println("Erro ao enviar")
	// } else {
	// 	fmt.Println("Mensagem enviada: ", msg.TopicPartition)
	// }
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "go_kafka_1:9092",
		"delivery.timeout.ms":"0",
		"acks":"all",
		"enable.idempotence" : "true",
	}

	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, deliveryChan)

	if err != nil {
		return err
	}

	return nil
}


func DeliveryReport(deliveryChan chan kafka.Event){
	for e := deliveryChan{
		switch ev := e.(type){
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada: ", ev.TopicPartition)
			}
		}
	}
}