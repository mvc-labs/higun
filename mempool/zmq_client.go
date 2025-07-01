package mempool

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/metaid/utxo_indexer/storage"
)

// ZMQClient represents a ZeroMQ client for listening to mempool events sent by Bitcoin nodes
type ZMQClient struct {
	// ZMQ connection address, e.g. "tcp://127.0.0.1:28332"
	address string

	// List of topics to listen to, e.g. "rawtx", "hashtx", etc.
	topics []string

	// Context control
	ctx    context.Context
	cancel context.CancelFunc

	// Wait for all goroutines to finish
	wg sync.WaitGroup

	// Connection and reconnection interval
	reconnectInterval time.Duration

	// Handler mapping, each topic corresponds to a handler function
	handlers map[string]MessageHandler
}

// MessageHandler is the function type for handling ZMQ messages
type MessageHandler func(topic string, data []byte) error

// NewZMQClient creates a new ZMQ client
func NewZMQClient(address string, _ *storage.SimpleDB) *ZMQClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &ZMQClient{
		address:           address,
		topics:            []string{},
		ctx:               ctx,
		cancel:            cancel,
		reconnectInterval: 5 * time.Second,
		handlers:          make(map[string]MessageHandler),
	}
}

// AddTopic adds a topic to listen to and its handler
func (c *ZMQClient) AddTopic(topic string, handler MessageHandler) {
	// Ensure topic is not duplicated
	for _, t := range c.topics {
		if t == topic {
			return
		}
	}

	c.topics = append(c.topics, topic)
	c.handlers[topic] = handler
}

// Start starts listening to ZMQ messages
func (c *ZMQClient) Start() error {
	if len(c.topics) == 0 {
		return fmt.Errorf("No topics added, please use AddTopic to add topics to listen to")
	}

	log.Printf("Connecting to ZMQ server: %s", c.address)
	log.Printf("Listening to topics: %s", strings.Join(c.topics, ", "))

	// Start listening goroutine
	c.wg.Add(1)
	go c.listen()

	return nil
}

// Stop stops listening
func (c *ZMQClient) Stop() {
	c.cancel()
	c.wg.Wait()
	log.Println("ZMQ client stopped")
}

// listen is an internal method for listening to ZMQ messages
func (c *ZMQClient) listen() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// Create a new socket
			socket := zmq4.NewSub(c.ctx)
			defer socket.Close()

			// Connect to ZMQ server
			if err := socket.Dial(c.address); err != nil {
				log.Printf("Failed to connect to ZMQ server: %v, will retry in %v",
					err, c.reconnectInterval)
				time.Sleep(c.reconnectInterval)
				continue
			}

			// Subscribe to all topics
			for _, topic := range c.topics {
				if err := socket.SetOption(zmq4.OptionSubscribe, topic); err != nil {
					log.Printf("Failed to subscribe to topic %s: %v", topic, err)
					continue
				}
			}

			log.Printf("Successfully connected to ZMQ server: %s", c.address)

			// Receive message loop
			c.receiveMessages(socket)

			// If receiveMessages returns, the connection is broken or an error occurred, reconnect
			log.Printf("ZMQ connection lost, will reconnect in %v", c.reconnectInterval)
			time.Sleep(c.reconnectInterval)
		}
	}
}

// receiveMessages receives and processes ZMQ messages
func (c *ZMQClient) receiveMessages(socket zmq4.Socket) {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// Receive message
			msg, err := socket.Recv()
			if err != nil {
				log.Printf("Failed to receive message: %v", err)
				return
			}

			// Ensure message has at least two parts: topic and data
			if len(msg.Frames) < 2 {
				log.Printf("Received message with incorrect format: %v", msg)
				continue
			}

			// First frame is topic
			topic := string(msg.Frames[0])

			// Find corresponding handler
			handler, ok := c.handlers[topic]
			if !ok {
				log.Printf("Received message for unknown topic: %s", topic)
				continue
			}

			// Call handler to process message
			if err := handler(topic, msg.Frames[1]); err != nil {
				log.Printf("Failed to process message [%s]: %v", topic, err)
			}
		}
	}
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
