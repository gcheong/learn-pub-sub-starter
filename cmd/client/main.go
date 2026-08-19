package main

import ("fmt"
		"log"

		amqp "github.com/rabbitmq/amqp091-go"

		"github.com/gcheong/learn-pub-sub-starter/internal/gamelogic"
		"github.com/gcheong/learn-pub-sub-starter/internal/routing"
		"github.com/gcheong/learn-pub-sub-starter/internal/pubsub"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print(">")
		gs.HandlePause(ps)
		return "Ack"
	}
}

func handlerMove(gs *gamelogic.GameState, chann *amqp.Channel) func(mv gamelogic.ArmyMove) pubsub.AckType{
	return func(mv gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print(">")
		outcome := gs.HandleMove(mv)

		if outcome == gamelogic.MoveOutComeSafe{
			return pubsub.AckTypeAck
		}

		if outcome == gamelogic.MoveOutcomeMakeWar {
			routingKey := routing.WarRecognitionsPrefix + "." + mv.Player.Username
			err := pubsub.PublishJSON(
					chann,
					routing.ExchangePerilTopic,
					routingKey,
					gamelogic.RecognitionOfWar{
  					 	Attacker: mv.Player,
   						Defender: gs.GetPlayerSnap(),
					},
			)

			if err != nil {
				log.Printf("handlerMove: Could not publish war acknowledge message: %v", err)
				return pubsub.AckTypeNackRequeue
			}

			return pubsub.AckTypeAck
		}

		return pubsub.AckTypeNackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState) func(msg gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print(">")
		
		outcome, _, _ := gs.HandleWar(rw)

		switch outcome {
			case gamelogic.WarOutcomeNotInvolved:
				return pubsub.AckTypeNackRequeue
			case gamelogic.WarOutcomeNoUnits:
				return pubsub.AckTypeNackDiscard
			case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon, gamelogic.WarOutcomeDraw:
				return pubsub.AckTypeAck
			default:
				return pubsub.AckTypeNackDiscard
				
		}

		
	}
}

func main() {
	fmt.Println("Starting Peril client...")

	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	username, err := gamelogic.ClientWelcome()

	if err != nil {
		log.Fatalf("Could not get username: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	queueName := routing.PauseKey + "." + username

	// _, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey , pubsub.QueueTypeTransient)

	
	err = pubsub.SubscribeJSON(conn,routing.ExchangePerilDirect, queueName, routing.PauseKey,pubsub.QueueTypeTransient, handlerPause(gameState) )
	if err != nil {
		log.Fatalf("Couldn't bind queue: %v", err)
	}

	movesQueueName := "army_moves." + username

	publishCh, err := conn.Channel()
	if err != nil {
		log.Fatalf("Couldn't establish publishing channel: %v", err)
	}

	err = pubsub.SubscribeJSON(conn,routing.ExchangePerilTopic, movesQueueName, "army_moves.*", pubsub.QueueTypeTransient, handlerMove(gameState, publishCh ) )
	if err != nil {
		log.Fatalf("Couldn't bind queue: %v", err)
	}

	warQueueKey := routing.WarRecognitionsPrefix + ".*"
	

	err = pubsub.SubscribeJSON(conn,routing.ExchangePerilTopic, "war", warQueueKey,  pubsub.QueueTypeDurable, handlerWar(gameState))
	if err != nil {
		log.Fatalf("Couldn't bind queue: %v", err)
	}
	

	

	for {

		words := gamelogic.GetInput()
		
		if len(words) == 0 {
			continue
		}

		command := words[0]

		if command == "spawn" {
			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Printf("%v",err)
			}
			continue
		}

		if command == "move" {
			mv, err := gameState.CommandMove(words)

			if err != nil {
				fmt.Printf("%v",err)

			} else {
				fmt.Println("Move successful")
				publishCh, err := conn.Channel()
				err = pubsub.PublishJSON(
					publishCh,
					routing.ExchangePerilTopic,
					movesQueueName,
					mv,
				)

				if err != nil {
					log.Printf("Could not publish message: %v", err)
				} else {
					log.Printf("Move published! %v to queue: %s", mv, movesQueueName)
				}
			}

			continue


		}

		if command == "status" {
			gameState.CommandStatus()
			continue
		}

		if command == "help" {
			gamelogic.PrintClientHelp()
			continue
		}

		if command == "spam" {
			fmt.Println("Spamming not allowed yet!")
			continue
		}

		if command == "quit" {
			fmt.Println("Exiting client...")
			break
		}

		fmt.Printf("Unrecognized command: %s", command)
		continue

	}


}
