package main

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/ThreeDotsLabs/go-event-driven/common/clients"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/receipts"
	"github.com/ThreeDotsLabs/go-event-driven/common/clients/spreadsheets"
	commonHTTP "github.com/ThreeDotsLabs/go-event-driven/common/http"
	"github.com/ThreeDotsLabs/go-event-driven/common/log"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-redisstream/pkg/redisstream"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

type TicketsConfirmationRequest struct {
	Tickets []string `json:"tickets"`
}

const (
	topicReceipt = "issue-receipt"
	topicTracker = "append-to-tracker"
)

func main() {
	log.Init(logrus.InfoLevel)
	logger := watermill.NewStdLogger(false, false)

	clients, err := clients.NewClients(os.Getenv("GATEWAY_ADDR"), nil)
	if err != nil {
		panic(err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("REDIS_ADDR"),
	})

	publisher, err := redisstream.NewPublisher(redisstream.PublisherConfig{
		Client: rdb,
	}, logger)
	if err != nil {
		panic(err)
	}
	go func() {
		spreadsheetsClient := NewSpreadsheetsClient(clients)
		trackerSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
			Client:        rdb,
			ConsumerGroup: "trackers",
		}, logger)
		if err != nil {
			panic(err)
		}
		ctx := context.Background()
		ch, err := trackerSub.Subscribe(ctx, topicTracker)
		if err != nil {
			panic(err)
		}
		for m := range ch {
			if err := spreadsheetsClient.AppendRow(ctx, "tickets-to-print", []string{string(m.Payload)}); err != nil {
				m.Nack()
			} else {
				m.Ack()
			}
		}
	}()

	go func() {
		receiptSub, err := redisstream.NewSubscriber(redisstream.SubscriberConfig{
			Client:        rdb,
			ConsumerGroup: "receivers",
		}, logger)
		if err != nil {
			panic(err)
		}
		receiptsClient := NewReceiptsClient(clients)
		ctx := context.Background()
		ch, err := receiptSub.Subscribe(ctx, topicReceipt)
		if err != nil {
			panic(err)
		}
		for m := range ch {
			if err := receiptsClient.IssueReceipt(ctx, string(m.Payload)); err != nil {
				m.Nack()
			} else {
				m.Ack()
			}
		}
	}()
	e := commonHTTP.NewEcho()

	e.POST("/tickets-confirmation", func(c echo.Context) error {
		var request TicketsConfirmationRequest
		err := c.Bind(&request)
		if err != nil {
			return err
		}

		for _, ticket := range request.Tickets {
			publisher.Publish(topicTracker, &message.Message{
				UUID:    watermill.NewShortUUID(),
				Payload: []byte(ticket),
			})

			publisher.Publish(topicReceipt, &message.Message{
				UUID:    watermill.NewShortUUID(),
				Payload: []byte(ticket),
			})
		}

		return c.NoContent(http.StatusOK)
	})

	logrus.Info("Server starting...")

	err = e.Start(":8080")
	if err != nil && err != http.ErrServerClosed {
		panic(err)
	}
}

type ReceiptsClient struct {
	clients *clients.Clients
}

func NewReceiptsClient(clients *clients.Clients) ReceiptsClient {
	return ReceiptsClient{
		clients: clients,
	}
}

func (c ReceiptsClient) IssueReceipt(ctx context.Context, ticketID string) error {
	body := receipts.PutReceiptsJSONRequestBody{
		TicketId: ticketID,
	}

	receiptsResp, err := c.clients.Receipts.PutReceiptsWithResponse(ctx, body)
	if err != nil {
		return err
	}
	if receiptsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", receiptsResp.StatusCode())
	}

	return nil
}

type SpreadsheetsClient struct {
	clients *clients.Clients
}

func NewSpreadsheetsClient(clients *clients.Clients) SpreadsheetsClient {
	return SpreadsheetsClient{
		clients: clients,
	}
}

func (c SpreadsheetsClient) AppendRow(ctx context.Context, spreadsheetName string, row []string) error {
	request := spreadsheets.PostSheetsSheetRowsJSONRequestBody{
		Columns: row,
	}

	sheetsResp, err := c.clients.Spreadsheets.PostSheetsSheetRowsWithResponse(ctx, spreadsheetName, request)
	if err != nil {
		return err
	}
	if sheetsResp.StatusCode() != http.StatusOK {
		return fmt.Errorf("unexpected status code: %v", sheetsResp.StatusCode())
	}

	return nil
}

type Task int

const (
	TaskIssueReceipt Task = iota
	TaskAddInSheet
)

type Message struct {
	Task     Task
	TicketID string
}
type Worker struct {
	ch                 chan Message
	receiptsClient     ReceiptsClient
	spreadsheetsClient SpreadsheetsClient
}

func NewWorker(receiptsClient ReceiptsClient, spreadsheetsClient SpreadsheetsClient) Worker {
	return Worker{
		ch:                 make(chan Message, 100),
		receiptsClient:     receiptsClient,
		spreadsheetsClient: spreadsheetsClient,
	}
}
func (w *Worker) Send(m Message) {
	w.ch <- m
}
func (w *Worker) Run() {
	cts := context.Background()
	for msg := range w.ch {
		switch msg.Task {
		case TaskIssueReceipt:
			logrus.Debug("Issuing Receipt")
			err := w.receiptsClient.IssueReceipt(cts, msg.TicketID)
			if err != nil {
				logrus.WithError(err).Errorf("Error while issuing Receipt")
				w.Send(msg)
			}
		case TaskAddInSheet:
			logrus.Debug("Adding in Sheet")
			err := w.spreadsheetsClient.AppendRow(cts, "tickets-to-print", []string{msg.TicketID})
			if err != nil {
				logrus.WithError(err).Errorf("Error while adding in Sheet")
				w.Send(msg)
			}
		}
	}
}
