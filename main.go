package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/google/uuid"
)

type message struct {
	MyTimestamp string `json:"mytimestamp"`
	Data        string `json:"data"`
}

var defaultProjectID = os.Getenv("GOOGLE_CLOUD_PROJECT")

func main() {
	projectID := flag.String("project", defaultProjectID, "")
	topic := flag.String("topic", "", "")
	flag.Parse()

	// Log data you want to publish into 'data' field
	myid, _ := uuid.NewRandom()
	data := map[string]any{
		"myid":     myid,
		"name":     "エレン",
		"location": "Tokyo",
		"enabled":  true,
		"specs":    []int{100, 300},
		"detail": map[string]any{
			"types":   []string{"進撃", "戦鎚"},
			"friends": []string{"ジャン", "アルミン", "ミカサ"},
		},
	}

	// Just publish
	ctx := context.Background()
	id, err := publish(ctx, *projectID, *topic, data)
	if err != nil {
		fmt.Println(err)
		return
	}
	log.Println("published:", id)

}

func prepareData(data map[string]any) []byte {
	m := message{MyTimestamp: time.Now().Format("2006-01-02T15:04:05")}

	// Encode log to JSON
	j, _ := json.Marshal(data)
	// Set JSON data as String to send
	m.Data = string(j)

	// Encode message to JSON
	x, _ := json.Marshal(m)

	return x
}

func publish(ctx context.Context, projectID, topicID string, data map[string]any) (string, error) {

	// Prepare data for pubsub topic
	// with combining 'mytimestamp' and 'data'
	prepared := prepareData(data)

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return "", fmt.Errorf("pubsub: NewClient: %v", err)
	}
	defer client.Close()

	t := client.Topic(topicID)
	result := t.Publish(ctx, &pubsub.Message{
		Data: prepared,
	})
	id, err := result.Get(ctx)
	if err != nil {
		return "", fmt.Errorf("pubsub: result.Get: %v", err)
	}
	log.Printf("Published a message; msg ID: %v\n", id)
	return id, nil
}
