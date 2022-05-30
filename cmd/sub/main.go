package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
	"hokkung.mq-demo/cmd/sub/model"
	pb "hokkung.mq-demo/proto"

	"github.com/rs/zerolog/log"
)

const (
	EXCHANGE      = "pub.get.message.topic"
	ROUTING_KEY   = "#"
	QUEUE         = "sub.pub.get.message.topic.queue"
	CONSUMER_NAME = "get_message"
	TYPE          = "topic"
)

func main() {
	conn, err := amqp.Dial(amqp.URI{
		Scheme:   "amqp",
		Host:     "localhost",
		Port:     5672,
		Username: "guest",
		Password: "guest",
		Vhost:    "pdev",
	}.String())
	if err != nil {
		log.Error().Err(err).Msg("init failed")
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Error().Err(err).Msg("init conn failed")
		conn.Close()
		return
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		EXCHANGE, // name
		TYPE,     // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		log.Error().Err(err).Msg("ExchangeDeclare failed")
		ch.Close()
		conn.Close()
		return
	}

	_, err = ch.QueueDeclare(
		QUEUE, // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		log.Error().Err(err).Msg("QueueDeclare failed")
		ch.Close()
		conn.Close()
		return
	}

	err = ch.QueueBind(
		QUEUE,       // queue name
		ROUTING_KEY, // routing key
		EXCHANGE,    // exchange
		false,
		nil)
	if err != nil {
		log.Error().Err(err).Msg("QueueBind failed")
		ch.Close()
		conn.Close()
		return
	}

	msgs, err := ch.Consume(
		QUEUE,         // queue
		CONSUMER_NAME, // consumer
		true,          // auto ack
		false,         // exclusive
		false,         // no local
		false,         // no wait
		nil,           // args
	)
	if err != nil {
		log.Error().Err(err).Msg("Consume failed")
		ch.Close()
		conn.Close()
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			be := pb.BusinessEngagement{}
			err := proto.Unmarshal(d.Body, &be)
			if err != nil {
				log.Error().Err(err).Msg("Unmarshal failed")
			}
			log.Info().Msgf(" [x] %+v", be)
			log.Info().Msgf(" [x] %s", be.GetBid())
			ts := be.GetTs().AsTime()
			m := model.BusinessEngagement{
				Bid:   be.GetBid(),
				Date:  ts.Format("20060102"),
				Hour:  ts.Format("15"),
				Min:   ts.Format("04"),
				Sec:   ts.Format("05"),
				Total: be.GetTotal(),
			}

			log.Info().Msgf(" [x] %+v", m)
		}
	}()

	log.Info().Msg(" [*] Waiting for logs. To exit press CTRL+C")
	<-forever
}
