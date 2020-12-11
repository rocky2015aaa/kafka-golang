package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	"github.com/Shopify/sarama"

	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	MsgLogger *log.Logger
)

func NewMsgLogger(filePath, logPrefix string, isTest bool) (*log.Logger, error) {
	if isTest {
		return log.New(os.Stdout, "INFO: ", log.LstdFlags), nil
	}

	logFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("error opening file: %v", err)
		return nil, err
	}
	msgLog := log.New(logFile, "", log.Ldate|log.Ltime|log.Lshortfile)
	msgLog.SetOutput(&lumberjack.Logger{
		Filename:   filePath,
		MaxSize:    100, // megabytes after which new file is created
		MaxBackups: 50,  // number of backups
		MaxAge:     30,  //days
		Compress:   true,
	})
	return msgLog, nil
}

func NewSyncProducer(brokerList []string, config *sarama.Config) (sarama.SyncProducer, error) {
	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		MsgLogger.Println("Failed to start Sarama producer:", err)
		return nil, err
	}

	return producer, nil
}

func NewConsumerGroup(groupName string, brokers []string, config *sarama.Config) (sarama.ConsumerGroup, error) {
	cg, err := sarama.NewConsumerGroup(brokers, groupName, config)
	if err != nil {
		MsgLogger.Println("Failed to create consumer group", err)
		return nil, err
	}

	return cg, nil
}

func CreateTLSConfiguration(certFile, keyFile, caFile string, verifySSL bool) (tlsConfig *tls.Config) {
	if certFile != "" && keyFile != "" && caFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			MsgLogger.Fatalln("Failed to get cert file", err)
		}

		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			MsgLogger.Fatalln("Failed to get ca cert file", err)
		}

		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)

		tlsConfig = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			RootCAs:            caCertPool,
			InsecureSkipVerify: verifySSL,
		}
	}

	return tlsConfig
}

func ConsumeMessages(topics []string, consumerGroup sarama.ConsumerGroup, client *KafkaAgentClient) {
	//defer client.Close()

	go func() {
		for err := range consumerGroup.Errors() {
			MsgLogger.Println("Failed to consume messages", err)
		}
	}()

	ctx := context.Background()
	for {
		err := consumerGroup.Consume(ctx, topics, client)
		if err != nil {
			MsgLogger.Println("Failed to consume messages", err)
		}
	}
}

func GetConfiguration(path, consumerMode string) map[string]interface{} {
	if consumerMode != "normal" && consumerMode != "batch" {
		MsgLogger.Fatalln("the mode flag is wrong")
	}
	configFile, err := os.Open(path)
	if err != nil {
		MsgLogger.Fatalln("Failed to load config file", err)
	}
	MsgLogger.Println("Successfully Opened config.json")
	defer configFile.Close()

	config := make(map[string]interface{})
	byteValue, err := ioutil.ReadAll(configFile)
	if err != nil {
		MsgLogger.Fatalln("Failed to read config file", err)
	}
	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		MsgLogger.Fatalln("Failed to unmarshal config file", err)
	}
	return config[consumerMode].(map[string]interface{})
}
