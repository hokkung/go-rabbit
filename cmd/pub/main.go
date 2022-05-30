package main

import (
	"time"

	"google.golang.org/protobuf/proto"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "hokkung.mq-demo/proto"
)

const (
	EXCHANGE      = "pub.get.message.topic"
	ROUTING_KEY   = "#"
	QUEUE         = "sub.pub.get.message.topic.queue"
	CONSUMER_NAME = "get_message"
	TYPE          = "topic"
)

func main() {
	config := amqp.URI{
		Scheme:   "amqp",
		Host:     "localhost",
		Port:     5672,
		Username: "guest",
		Password: "guest",
		Vhost:    "pdev",
	}
	conn, err := amqp.Dial(config.String())
	if err != nil {
		log.Error().Err(err).Msg("Dial failed")
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Error().Err(err).Msg("Channel failed")
		conn.Close()
		return
	}
	defer ch.Close()

	body := &pb.BusinessEngagement{
		Bid:    "1",
		Ts:     timestamppb.New(time.Date(2022, 01, 01, 07, 0, 0, 0, time.UTC)),
		Action: "click",
		Type:   "ads",
		Total:  10,
	}
	b, _ := proto.Marshal(body)
	err = ch.Publish(
		EXCHANGE,    // exchange
		ROUTING_KEY, // routing key
		false,       // mandatory
		false,       // immediate
		amqp.Publishing{
			ContentType: "application/x-protobuf",
			Body:        b,
		})
	if err != nil {
		log.Error().Err(err).Msg("Publish failed")
		ch.Close()
		conn.Close()
		return
	}

	log.Info().Msgf(" [x] Sent %s", body)
}
