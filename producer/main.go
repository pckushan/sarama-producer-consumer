package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)

func main() {
	brokers := []string{"localhost:9092"}
	conf := sarama.NewConfig()
	conf.Producer.Return.Successes = true
	conf.Producer.Partitioner = sarama.NewRandomPartitioner

	syncProd, err := sarama.NewSyncProducer(brokers, conf)
	handleErr(err)

	msg := &sarama.ProducerMessage{
		Topic: "test_2",
		Key:   sarama.StringEncoder("12345"),
		Value: sarama.StringEncoder("hello world !!!"),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("user-id"),
				Value: []byte("122333"),
			},
		},
		//Metadata:  nil,
		//Offset:    0,
		//Partition: 0,
		Timestamp: time.Now(),
	}

	partition, offset, err := syncProd.SendMessage(msg)
	handleErr(err)
	log.Println(fmt.Sprintf("message produced to partition [%d] with offset [%d]", partition, offset))

}

func handleErr(err error) {
	if err != nil {
		log.Println(err)
		return
	}
}
