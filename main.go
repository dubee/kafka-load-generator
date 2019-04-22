package main

import (
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/Shopify/sarama.v1"
)

type config struct {
	brokers            string
	topic              string
	batchSize          int
	clients            int
	producersPerClient int
	tls                bool
	sasl               bool
	username           string
	password           string
	message            string
	messagesToSend     int
}

var Config = &config{}

func init() {
	flag.StringVar(&Config.topic, "topic", "topic", "Kafka topic")
	flag.IntVar(&Config.batchSize, "batch-size", 500, "Messages batch size")
	flag.IntVar(&Config.clients, "clients", 3, "Number of clients")
	flag.IntVar(&Config.producersPerClient, "producers-per-client", 5, "Number of producers to use per client")
	flag.IntVar(&Config.messagesToSend, "messages-to-send", 100000, "Number of messages to send")
	flag.StringVar(&Config.brokers, "brokers", "localhost:9093", "Comma delimited list of Kafka brokers")
	flag.BoolVar(&Config.tls, "tls", true, "Use TLS encryption")
	flag.BoolVar(&Config.sasl, "sasl", true, "Use SASL authentication")
	flag.StringVar(&Config.username, "username", "username", "SASL username")
	flag.StringVar(&Config.password, "password", "password", "SASL password")
	flag.StringVar(&Config.message, "message", "message", "Kafka message")

	flag.Parse()
}

func main() {
	var workers Workers

	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Compression = sarama.CompressionGZIP
	producerConfig.Producer.Return.Successes = true
	producerConfig.Producer.RequiredAcks = sarama.WaitForLocal
	producerConfig.Producer.Flush.MaxMessages = Config.batchSize
	producerConfig.Net.TLS.Enable = Config.tls
	producerConfig.Net.SASL.Enable = Config.sasl
	producerConfig.Net.SASL.User = Config.username
	producerConfig.Net.SASL.Password = Config.password

	workers.Create(Config.clients, producerConfig, strings.Split(Config.brokers, ","))
	workers.Write(Config.producersPerClient, Config.message, Config.topic, Config.batchSize)

	totalSent := 0
	ticker := time.NewTicker(time.Second)

	for range ticker.C {
		sentCount, errorCount := workers.Stats()
		totalSent = totalSent + sentCount
		log.Printf("Sent: %d msg/s; Error: %d/s\n", sentCount, errorCount)

		if totalSent > Config.messagesToSend {
			log.Printf("Done, sent a total of %d messages\n", totalSent)
			os.Exit(0)
		}
	}
}
