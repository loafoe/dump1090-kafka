package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/philips-software/gautocloud-connectors/hsdp"
	"github.com/segmentio/kafka-go"
	"github.com/spf13/viper"

	"github.com/centrifugal/centrifuge-go"
	"github.com/cloudfoundry-community/gautocloud"
	_ "github.com/cloudfoundry-community/gautocloud/connectors/amqp/client"
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

func (h *subEventHandler) OnPublish(sub *centrifuge.Subscription, e centrifuge.PublishEvent) {
	log.Println(fmt.Sprintf("New message received from channel %s: %s", sub.Channel(), string(e.Data)))
	h.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	h.conn.WriteMessages(
		kafka.Message{Value: e.Data},
	)
}

func newClient(wsURL, hmacSecretKey string) *centrifuge.Client {
	c := centrifuge.New(wsURL, centrifuge.DefaultConfig())

	c.SetToken(connToken("forwarder", hmacSecretKey, 0))

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
	var kc hsdp.KafkaCredentials
	err = gautocloud.Inject(&kc)

	if err != nil {
		fmt.Printf("No Kafka cluster found: %v\n", err)
		return
	}
	conn, err := kafka.DialLeader(context.Background(), "tcp", fmt.Sprintf("%s:%d", kc.Hostname, kc.Port), "dump1090-sbs", 0)
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
