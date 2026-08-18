package main

import (
	"fmt"
	"log"

	"github.com/gcheong/learn-pub-sub-starter/internal/pubsub"
	"github.com/gcheong/learn-pub-sub-starter/internal/routing"
	"github.com/gcheong/learn-pub-sub-starter/internal/gamelogic"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}

	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, "game_logs", "game_logs.*" , pubsub.QueueTypeDurable)


	gamelogic.PrintServerHelp()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		if words[0]  == "pause" {
			log.Printf("Sending Pause message!")

			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true,},
			)

			if err != nil {
				log.Printf("could not publish message: %v", err)
			}
	
			continue

		}


		if words[0]  == "resume" {
			log.Printf("Sending Resume message!")

			err = pubsub.PublishJSON(
				publishCh,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false,},
			)

			if err != nil {
			log.Printf("could not publish message: %v", err)
			}
	
			continue

		}


		if words[0]  == "quit" {
			log.Printf("Exiting!")	
			break;
		}

		log.Printf("Sorry, I don't understand that command: %s", words[0])
	}

}
