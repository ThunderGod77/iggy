// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	"github.com/apache/iggy/examples/go/common"
	"github.com/apache/iggy/foreign/go/client"
	iggcon "github.com/apache/iggy/foreign/go/contracts"

	"github.com/apache/iggy/foreign/go/client/tcp"
)

var (
	StreamID     = "message-envelope-stream"
	TopicID      = "message-envelope-topic"
	PartitionID  = uint32(0)
	BatchesLimit = uint32(5)
)

type SerializableMessage interface {
	messageType() string
}

func encodeEnvelope(sm SerializableMessage) ([]byte, error) {
	payload, err := json.Marshal(sm)
	if err != nil {
		return nil, err
	}
	return json.Marshal(MessageEnvelope{
		MessageType: sm.messageType(),
		Payload:     string(payload),
	})
}

type OrderCreated struct {
	OrderID    string `json:"order_id"`
	CustomerID string `json:"customer_id"`
	Amount     int    `json:"amount"`
}

func (oc *OrderCreated) messageType() string {
	return orderCreatedType
}

type OrderConfirmed struct {
	OrderID   string `json:"order_id"`
	Timestamp int64  `json:"timestamp"`
}

func (oc *OrderConfirmed) messageType() string {
	return orderConfirmedType
}

type OrderRejected struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

func (or *OrderRejected) messageType() string {
	return orderRejectedType
}

type MessageEnvelope struct {
	MessageType string `json:"message_type"`
	Payload     string `json:"payload"`
}

const (
	orderCreatedType   = "OrderCreated"
	orderConfirmedType = "OrderConfirmed"
	orderRejectedType  = "OrderRejected"
)

type MessageGenerator struct {
	orderID int
}

func NewMessageGenerator() *MessageGenerator {
	return &MessageGenerator{orderID: 0}
}

func (ms *MessageGenerator) generate() SerializableMessage {
	ms.orderID += 1
	messageType := ms.orderID % 3
	switch messageType {
	case 0:
		{
			return &OrderCreated{
				OrderID:    fmt.Sprintf("order-%d", ms.orderID),
				CustomerID: fmt.Sprintf("customer-%d", rand.Intn(100)),
				Amount:     rand.Intn(10000),
			}
		}
	case 1:
		{
			return &OrderConfirmed{
				OrderID:   fmt.Sprintf("order-%d", ms.orderID),
				Timestamp: time.Now().UnixMilli(),
			}
		}
	default:
		{
			return &OrderRejected{
				OrderID: fmt.Sprintf("order-%d", ms.orderID),
				Reason:  "Insufficient balance",
			}
		}
	}
}

func main() {
	serverAddr, tcpOptions := getTcpOptions()

	cli, err := client.NewIggyClient(
		client.WithTcp(tcpOptions...),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := cli.Close(); err != nil {
			log.Printf("Error closing client: %v", err)
		}
	}()

	ctx := context.Background()
	log.Printf("Connecting to server at %s", serverAddr)
	if err := cli.Connect(ctx); err != nil {
		log.Printf("Failed to connect to server at %s: %v", serverAddr, err)
	}

	if _, err := cli.LoginUser(ctx, common.DefaultRootUsername, common.DefaultRootPassword); err != nil {
		log.Fatalf("Login failed: %v", err)
	}
	initSystem(ctx, cli)
	if err := produceMessages(ctx, cli); err != nil {
		log.Fatalf("Producing messages failed: %v", err)
	}
}

func initSystem(ctx context.Context, client iggcon.Client) {
	if _, err := client.CreateStream(ctx, StreamID, nil); err != nil {
		log.Printf("WARN: Stream already exists or error: %v", err)
	}
	log.Println("Stream was created.")

	streamIdentifier, _ := iggcon.NewIdentifier(StreamID)
	partitions := uint32(1)
	expiry := iggcon.IggyExpiryNeverExpire
	if _, err := client.CreateTopic(
		ctx,
		streamIdentifier,
		TopicID,
		&iggcon.TopicOptions{PartitionsCount: &partitions, MessageExpiry: &expiry}); err != nil {
		log.Printf("WARN: Topic already exists and will not be created again or error: %v", err)
	}
	log.Println("Topic was created.")
}

func produceMessages(ctx context.Context, client iggcon.Client) error {
	interval := 500 * time.Millisecond
	log.Printf(
		"Messages will be sent to stream: %s, topic: %s, partition: %d with interval %s.",
		StreamID, TopicID, PartitionID, interval)

	messagesPerBatch := 10
	sentBatches := uint32(0)
	partitioning := iggcon.PartitionId(PartitionID)

	messageGenerator := NewMessageGenerator()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if sentBatches == BatchesLimit {
			log.Printf("Sent %d batches of messages, exiting.", sentBatches)
			return nil
		}
		<-ticker.C

		messages := make([]iggcon.IggyMessage, 0, messagesPerBatch)
		for i := 0; i < messagesPerBatch; i++ {
			serializableMessage := messageGenerator.generate()
			payload, err := encodeEnvelope(serializableMessage)
			if err != nil {
				return err
			}
			message, _ := iggcon.NewIggyMessage(payload)
			messages = append(messages, message)
		}

		streamIdentifier, _ := iggcon.NewIdentifier(StreamID)
		topicIdentifier, _ := iggcon.NewIdentifier(TopicID)
		if _, err := client.SendMessages(
			ctx,
			streamIdentifier,
			topicIdentifier,
			partitioning,
			messages); err != nil {
			return err
		}
		sentBatches++
		log.Printf("Sent %d message(s).", messagesPerBatch)
	}
}

func getTcpOptions() (string, []tcp.Option) {
	tcpServerAddr := flag.String("tcp-server-address", "127.0.0.1:8090", "TCP server address")
	tlsEnabled := flag.Bool("tls", false, "Enable TLS")
	tlsCAFile := flag.String("tls-ca-file", "", "Path to CA certificate file for TLS")
	tlsDomain := flag.String("tls-domain", "", "TLS server domain name for SNI")
	flag.Parse()

	if _, err := net.ResolveTCPAddr("tcp", *tcpServerAddr); err != nil {
		log.Fatalf("Invalid server address %s! Usage: --tcp-server-address <server-address>", *tcpServerAddr)
	}

	tcpOptions := []tcp.Option{
		tcp.WithServerAddress(*tcpServerAddr),
	}

	if *tlsEnabled {
		var tlsOpts []tcp.TLSOption
		if *tlsCAFile != "" {
			tlsOpts = append(tlsOpts, tcp.WithTLSCAFile(*tlsCAFile))
		}
		if *tlsDomain != "" {
			tlsOpts = append(tlsOpts, tcp.WithTLSDomain(*tlsDomain))
		}
		tcpOptions = append(tcpOptions, tcp.WithTLS(tlsOpts...))
		log.Printf("TLS enabled with CA file: %s, domain: %s", *tlsCAFile, *tlsDomain)
	}

	return *tcpServerAddr, tcpOptions
}
