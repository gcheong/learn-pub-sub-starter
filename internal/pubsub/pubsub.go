package pubsub

import (
    "encoding/json"
	"context"
	"log"
	 
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/gcheong/learn-pub-sub-starter/internal/routing"

)

type SimpleQueueType string

const (
	QueueTypeDurable SimpleQueueType = "durable"
	QueueTypeTransient SimpleQueueType = "transient"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	//Marshall val to json
	data, err := json.Marshal(val)
	if err != nil {
    	return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: data})

	return err
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error){
	chann, err := conn.Channel()

	if err != nil {
		return chann, amqp.Queue{}, err
	}

	queue, err := chann.QueueDeclare(queueName, queueType == QueueTypeDurable, queueType == QueueTypeTransient, queueType == QueueTypeTransient, false, nil)

	if err != nil {
		return chann, queue, err
	}

	err = chann.QueueBind(queueName, routing.PauseKey, routing.ExchangePerilDirect, false, nil)

	return chann, queue, err

}

func SubscribeJSON[T any]( conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType,  handler func(T)) error {
	chann, queue, err := DeclareAndBind(conn, exchange, queueName, keu, queueType)
	
	if err != nil {
		return err
	}

	delivery, err := chann.Consume(queue, "", false, false, false, false, nil)

	go func() {
    	for message := range delivery {
        	var result T
			err := json.Unmarshal(message.Body, &result)

			if err != nil {
				log.Printf("Error decoding message body: %v", err)
				continue
			}

			handler(result)
    	}
	}()	



}

