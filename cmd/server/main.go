package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

    amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionStr := "amqp://guest:guest@localhost:5672/"
	connection, error := amqp.Dial(connectionStr)

	if error != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", error)
	}

	defer connection.Close()

	fmt.Println("Peril game server connected to RabbitMQ!")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Println("RabbitMQ connection closed.")

}
