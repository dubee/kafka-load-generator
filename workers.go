package main

import (
	"log"
	"os"
	"sync"

	"gopkg.in/Shopify/sarama.v1"
)

type Worker struct {
	client     sarama.Client
	errorCount int
	sentCount  int
	mux        sync.Mutex
}

func (w *Worker) Create(config *sarama.Config, brokers []string) {
	var err error

	w.client, err = sarama.NewClient(brokers, config)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	} else {
		log.Println("Client connected")
	}
}

func (w *Worker) Write(message string, topic string, batchSize int) {
	producer, err := sarama.NewSyncProducerFromClient(w.client)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}
	defer producer.Close()

	msgBatch := make([]*sarama.ProducerMessage, 0, batchSize)
	messageBody := sarama.StringEncoder(message)

	for i := 0; i < batchSize; i++ {
		msg := &sarama.ProducerMessage{Topic: topic, Value: messageBody}
		msgBatch = append(msgBatch, msg)
	}

	for {
		errorCount := 0
		err = producer.SendMessages(msgBatch)
		if err != nil {
			errorCount = len(err.(sarama.ProducerErrors))
			w.mux.Lock()
			w.errorCount = w.errorCount + errorCount
			w.mux.Unlock()
		}

		w.mux.Lock()
		w.sentCount = w.sentCount + len(msgBatch) - errorCount
		w.mux.Unlock()
	}

}

type Workers struct {
	workers []Worker
}

func (w *Workers) Create(numberOfWorkers int, config *sarama.Config, brokers []string) {
	var waitGroup sync.WaitGroup

	w.workers = make([]Worker, numberOfWorkers)

	log.Printf("Connecting %d workers...", numberOfWorkers)

	for i := 0; i < numberOfWorkers; i++ {
		waitGroup.Add(1)
		go func(i int) {
			w.workers[i] = Worker{}
			w.workers[i].Create(config, brokers)
			waitGroup.Done()
		}(i)
	}

	waitGroup.Wait()
}

func (w *Workers) Write(writersPerWorker int, message string, topic string, batchSize int) {
	for i := 0; i < len(w.workers); i++ {
		for j := 0; j < writersPerWorker; j++ {
			go w.workers[i].Write(message, topic, batchSize)
		}
	}
}

func (w *Workers) Stats() (int, int) {
	sentCount := 0
	errorCount := 0

	for i := 0; i < len(w.workers); i++ {
		w.workers[i].mux.Lock()
		sentCount = sentCount + w.workers[i].sentCount
		errorCount = errorCount + w.workers[i].errorCount
		w.workers[i].sentCount = 0
		w.workers[i].errorCount = 0
		w.workers[i].mux.Unlock()
	}

	return sentCount, errorCount
}
