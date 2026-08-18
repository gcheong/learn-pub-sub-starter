package main

import ("fmt"
		"log"

		amqp "github.com/rabbitmq/amqp091-go"

		"github.com/gcheong/learn-pub-sub-starter/internal/gamelogic"
		"github.com/gcheong/learn-pub-sub-starter/internal/routing"
		"github.com/gcheong/learn-pub-sub-starter/internal/pubsub"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState){
		defer fmt.Print(">")
		gs.HandlePause(ps)
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

	pubsub.SubscribeJSON(conn,routing.ExchangePerilDirect, queueName, routing.PauseKey,pubsub.QueueTypeTransient, handlerPause(gameState) )

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
			_, err := gameState.CommandMove(words)

			if err != nil {
				fmt.Printf("%v",err)
			} else {
				fmt.Printf("Move successful")
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
