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

	asyncProd, err := sarama.NewAsyncProducer(brokers, conf)
	handleErr(err)

	msg := sarama.ProducerMessage{
		Topic: "test_3",
		Key:   sarama.StringEncoder("12345"),
		Value: sarama.StringEncoder("hello world ASYNC!!!"),
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

	msgs := make([]sarama.ProducerMessage, 0)

	for i := 0; i < 100; i++ {
		msg.Key = sarama.StringEncoder(fmt.Sprintf("%v", time.Now().Unix()))
		msgs = append(msgs, msg)
	}

	done := make(chan bool)
	defer close(done)

	go func() {
		for i := 0; i < 100; i++ {
			select {
			case <-asyncProd.Successes():
				log.Println("message produced successfully")
			case err := <-asyncProd.Errors():
				log.Println("message produced failed with error", err)
			}
		}
		done <- true
	}()

	for _, msg := range msgs {
		asyncProd.Input() <- &msg
	}

	<-done
}

func handleErr(err error) {
	if err != nil {
		log.Println(err)
		return
	}
}
