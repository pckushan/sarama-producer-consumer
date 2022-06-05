package main

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
)

type Customhandler struct {
}

func (c Customhandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c Customhandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (c Customhandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("consumed with key [%v] , value [%v], offset [%v], partition [%v]", string(msg.Key), string(msg.Value), msg.Offset, msg.Partition)
	}
	return nil
}

func main() {
	brokers := []string{"localhost:9092"}
	appID := "app-1"
	conf := sarama.NewConfig()
	conf.Consumer.Offsets.Initial = sarama.OffsetOldest
	gc, err := sarama.NewConsumerGroup(brokers, appID, conf)
	handlerErr(err)

	ctx := context.Background()
	topics := []string{"test_3"}
	handler := Customhandler{}
	err = gc.Consume(ctx, topics, handler)
	handlerErr(err)

}

func handlerErr(err error) {
	if err != nil {
		log.Println(err)
		return
	}
}
