package kafka

import (
	"encoding/json"
	"log"
	"os"
	"time"

	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/edsonboldrini/imersao-fsfc2-simulator/app/routes"
	"github.com/edsonboldrini/imersao-fsfc2-simulator/infra/kafka"
)

// {"clientId": "1", "routeId": "1"}
func Produce(msg *ckafka.Message) {
	producer := kafka.NewKafkaProducer()
	route := routes.NewRoute()

	json.Unmarshal(msg.Value, &route)
	route.LoadPositions()
	positions, err := route.ExportJsonPositions()

	if err != nil {
		log.Println(err.Error())
	}

	for _, p := range positions {
		kafka.Publish(p, os.Getenv("KafkaProduceTopic"), producer)
		time.Sleep(time.Millisecond * 1000)
	}
}
