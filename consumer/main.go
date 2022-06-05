package main

import (
	"github.com/Shopify/sarama"
	"log"
)

func main() {
	brokers := []string{"localhost:9092"}
	conf := sarama.NewConfig()
	c, err := sarama.NewConsumer(brokers, conf)
	handlerErr(err)

	pc, err := c.ConsumePartition("test_3", 0, sarama.OffsetOldest)
	handlerErr(err)

	for true {
		select {
		case msg := <-pc.Messages():
			log.Printf("consumed with key [%v] , value [%v], offset [%v], partition [%v]", string(msg.Key), string(msg.Value), msg.Offset, msg.Partition)
		}
	}

}

func handlerErr(err error) {
	if err != nil {
		log.Println(err)
		return
	}
}
