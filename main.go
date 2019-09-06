// A little test utility that writes some moderately-interesting json data
// to a specified kafka cluster with the specified topic.
//
// Example usage: kafkawrite -host kafkahost:9092 -topic mytopic

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/Shopify/sarama"
)

// readData reads from a github repo issues list and returns the resulting
// raw data.
func readData() []byte {
	req, err := http.NewRequest(
		"GET", "https://api.github.com/repos/elastic/beats/issues", nil)
	if err != nil {
		log.Fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		return bodyBytes
	} else {
		log.Fatal(fmt.Sprintf(
			"Couldn't read github: response code %v\n", resp.StatusCode))
	}
	return nil
}

// Issue represents a github issue
type Issue struct {
	//URL   url.URL `json:"url"`
	ID    int    `json:"id"`
	Title string `json:"title"`
	State string `json:"state"`
	Body  string `json:"body"`
}

// Issues is an array of github issues
type Issues []Issue

// sendData interprets the given bytes as json representation of github issues
// (Issues) and sends (a few test fields of) the individual issues to the
// target kafka cluster / topic as kafka events containing json.
func sendData(bytes []byte, host string, topic string) {
	var issues Issues
	err := json.Unmarshal(bytes, &issues)
	if err != nil {
		fmt.Println(err)
		return
	}

	sent := 0
	for _, issue := range issues {
		blob, err := json.Marshal(issue)
		if err != nil {
			log.Print(err)
			continue
		}
		err = writeToKafkaTopic(host, topic, string(blob), nil, time.Second*15)
		if err != nil {
			log.Print(err)
			continue
		}
		sent++
	}
	fmt.Printf("%v / %v messages sent\n", sent, len(issues))
}

func saramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Version = sarama.V1_0_0_0
	return config
}

func writeToKafkaTopic(
	host string, topic string, message string,
	headers []sarama.RecordHeader, timeout time.Duration,
) error {
	config := saramaConfig()
	hosts := []string{host}
	producer, err := sarama.NewSyncProducer(hosts, config)
	if err != nil {
		return err
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic:   topic,
		Value:   sarama.StringEncoder(message),
		Headers: headers,
	}

	_, _, err = producer.SendMessage(msg)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	host := flag.String("host", "", "kafka host")
	topic := flag.String("topic", "", "kafka topic")

	flag.Parse()
	if *host == "" || *topic == "" {
		log.Fatal("Host and topic must be provided.\n" +
			"Usage: kafkawrite -host <host> -topic <topic>")
	}

	data := readData()
	sendData(data, *host, *topic)
}
