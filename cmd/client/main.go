package main

import ("fmt"
		"log"
		"os"
    	"os/signal"
    	"syscall"

		amqp "github.com/rabbitmq/amqp091-go"

		"github.com/gcheong/learn-pub-sub-starter/internal/gamelogic"
		"github.com/gcheong/learn-pub-sub-starter/internal/routing"
		"github.com/gcheong/learn-pub-sub-starter/internal/pubsub"
)

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

	queueName := routing.PauseKey + "." + username

	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, queueName, routing.PauseKey , pubsub.QueueTypeTransient)

	if err != nil {
		log.Fatalf("Could bind queue: %v", err)
	}

	//Wait for ctrl-c
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	<-signalChan
	fmt.Println("Signal received, shutting down...")


}
