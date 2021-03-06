package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "go_kafka_1:9092",
		"client.id":         "goapp-consumer",
		"group.id":          "goapp-group",
		"auto.offset.reset": "earliest", // quando o consumidor subir serão buscadas as mensagens anteriores
	}

	c, err := kafka.NewConsumer(configMap)

	if err != nil {
		fmt.Println("erro consumer", err.Error())
	}

	topics := []string{"teste"}

	c.SubscribeTopics(topics, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Println(string(msg.Value), msg.TopicPartition)
		}
	}
}
