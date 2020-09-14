package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"encoding/json"

	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"

	"github.com/centrifugal/centrifuge-go"
	"github.com/dgrijalva/jwt-go"
)

func connToken(user, secret string, exp int64) string {
	claims := jwt.MapClaims{"sub": user}
	if exp > 0 {
		claims["exp"] = exp
	}
	claims["info"] = "dump1090"
	t, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		panic(err)
	}
	return t
}

func subscribeToken(channel, secret string, client string, exp int64) string {
	claims := jwt.MapClaims{"channel": channel, "client": client}
	if exp > 0 {
		claims["exp"] = exp
	}
	t, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		panic(err)
	}
	return t
}

type eventHandler struct {
	hmacSecretKey string
}

func (h *eventHandler) OnPrivateSub(_ *centrifuge.Client, e centrifuge.PrivateSubEvent) (string, error) {
	token := subscribeToken(e.Channel, h.hmacSecretKey, e.ClientID, time.Now().Unix()+10)
	return token, nil
}

func (h *eventHandler) OnConnect(_ *centrifuge.Client, _ centrifuge.ConnectEvent) {
	log.Println("Connected")
}

func (h *eventHandler) OnError(_ *centrifuge.Client, e centrifuge.ErrorEvent) {
	log.Println("Error", e.Message)
}

func (h *eventHandler) OnDisconnect(_ *centrifuge.Client, e centrifuge.DisconnectEvent) {
	log.Println("Disconnected", e.Reason)
}

type subEventHandler struct {
	conn *kafka.Conn
}

func (h *subEventHandler) OnSubscribeSuccess(sub *centrifuge.Subscription, _ centrifuge.SubscribeSuccessEvent) {
	log.Println(fmt.Sprintf("Successfully subscribed to private channel %s", sub.Channel()))
}

func (h *subEventHandler) OnSubscribeError(sub *centrifuge.Subscription, e centrifuge.SubscribeErrorEvent) {
	log.Println(fmt.Sprintf("Error subscribing to private channel %s: %v", sub.Channel(), e.Error))
}

func (h *subEventHandler) OnUnsubscribe(sub *centrifuge.Subscription, _ centrifuge.UnsubscribeEvent) {
	log.Println(fmt.Sprintf("Unsubscribed from private channel %s", sub.Channel()))
}

type SBS struct {
	Message string
}

func (h *subEventHandler) OnPublish(sub *centrifuge.Subscription, e centrifuge.PublishEvent) {
	var sbs SBS
	err := json.Unmarshal(e.Data, &sbs)
	if err != nil {
		fmt.Printf("Not an SBS message: %v\n", err)
		return
	}
	_ = h.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = h.conn.WriteMessages(
		kafka.Message{Value: []byte(sbs.Message)},
	)
	if err != nil {
		fmt.Printf("Error writing to Kafka: %v\n", err)
	}
}

func newClient(wsURL, hmacSecretKey string) *centrifuge.Client {
	c := centrifuge.New(wsURL, centrifuge.DefaultConfig())

	c.SetToken(connToken("forwarder-kafka", hmacSecretKey, 0))

	handler := &eventHandler{
		hmacSecretKey: hmacSecretKey,
	}
	c.OnPrivateSub(handler)
	c.OnDisconnect(handler)
	c.OnConnect(handler)
	c.OnError(handler)
	return c
}

func main() {
	viper.SetEnvPrefix("forwarder")
	viper.AutomaticEnv()
	viper.SetDefault("wss", "wss://centrifugo.cloud.pcftest.com:4443/connection/websocket")
	viper.SetDefault("channel", "public:dump1090-sbs")
	viper.SetDefault("hmac_secret_key", "")
	viper.SetDefault("kafka_connect", "localhost:9202")

	log.Println("Start program")
	wsURL := viper.GetString("wss")
	hmacSecretKey := viper.GetString("hmac_secret_key")

	if wsURL == "" {
		fmt.Printf("Missing wss URL, exiting.\n")
		return
	}
	if hmacSecretKey == "" {
		fmt.Printf("Missing hmac_secret_token, exiting.\n")
		return
	}

	c := newClient(wsURL, hmacSecretKey)
	defer func() { _ = c.Close() }()

	err := c.Connect()
	if err != nil {
		log.Fatalln(err)
	}
	subChannel := viper.GetString("channel")
	sub, err := c.NewSubscription(subChannel)
	if err != nil {
		log.Fatalln(err)
	}

	// Kafka
	kafkaConnect := viper.GetString("kafka_connect")

	conn, err := kafka.DialLeader(context.Background(), "tcp", kafkaConnect, "dump1090-sbs", 0)
	if err != nil {
		fmt.Printf("Failed to connect to Kafka: %v\n", err)
		return
	}
	defer func() {
		_ = conn.Close()
	}()
	subEventHandler := &subEventHandler{
		conn: conn,
	}
	sub.OnSubscribeSuccess(subEventHandler)
	sub.OnSubscribeError(subEventHandler)
	sub.OnUnsubscribe(subEventHandler)
	sub.OnPublish(subEventHandler)

	// Subscribe on private channel.
	_ = sub.Subscribe()

	select {}
}
