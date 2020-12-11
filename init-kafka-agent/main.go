package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/Shopify/sarama"
)

const (
	BROKER_URL_01 = "kafka1-internal.m-fws.io:9092"
	BROKER_URL_02 = "kafka2-internal.m-fws.io:9092"
	BROKER_URL_03 = "kafka3-internal.m-fws.io:9092"
	RESTFUL_URL   = "http://localhost:8990"

	PRODUCER_FIELD_CODE     = "code"
	PRODUCER_FIELD_DESC     = "desc"
	PRODUCER_FIELD_MESSAGE  = "message"
	PRODUCER_FIELD_CNT_INFO = "retry-cnt"

	REST_REQUEST_FIELD_METHOD    = "method"
	REST_REQUEST_FIELD_URL       = "url"
	REST_REQUEST_FIELD_PARAMETER = "param"

	ERR_CODE_RESPONSE_UNKNOWN           = "RS_RES_E00"
	ERR_CODE_RESPONSE_TYPE              = "RS_RES_E01"
	ERR_CODE_RESPONSE_PARSE             = "RS_RES_E02"
	ERR_CODE_RESPONSE_DATA              = "RS_RES_E03"
	ERR_CODE_PARAMETER_BIND             = "RS_PRM_E01"
	ERR_CODE_PARAMETER_VALIDATION       = "RS_PRM_E02"
	ERR_CODE_QUEUE_MSG_PARSE            = "QU_MSG_E01"
	ERR_CODE_QUEUE_REST_SERVER_RESPONSE = "QU_MSG_E02"

	SUCCESS_CODE_RESPONSE = "RS_RES_N01"

	RETRY_MAX          = 10
	REFLICATION_FACTOR = 3
	//REFLICATION_FACTOR = 1

	// Config Field Names
	REQUEST_CONSUMER_CLIENT_ID  = "request_consumer_id"
	RETRY_CONSUMER_CLIENT_ID    = "retry_consumer_id"
	REQUEST_CONSUMER_GROUP_NAME = "request_consumer_group_name"
	RETRY_CONSUMER_GROUP_NAME   = "retry_consumer_group_name"

	TOPIC_REQUEST  = "topic_request"
	TOPIC_RESPONSE = "topic_response"
	TOPIC_RETRY    = "topic_retry"

	RESULT_CODE_SUCCESS = "result_code_success"
	RESULT_CODE_FAILURE = "result_code_failure"
	RESULT_CODE_RETRY   = "result_code_retry"

	TOPIC_PARTITION_NUMBER = "numberOfPartitions"

	// restful server response field
	CODE   = "code"
	REF_ID = "refID"
)

func main() {
	consumerMode := flag.String("mode", "normal", "mode can be normal or batch")
	flag.Parse()

	agentConfig := GetConfiguration(*consumerMode)

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_2_0
	admin, err := sarama.NewClusterAdmin([]string{BROKER_URL_01, BROKER_URL_02, BROKER_URL_03}, config)

	if err != nil {
		log.Fatalln("Failed to create cluster admin for kafka producer ", err)
		panic(err)
	}
	fmt.Println("topics:", agentConfig[TOPIC_REQUEST].(string), agentConfig[TOPIC_RESPONSE].(string), agentConfig[TOPIC_RETRY].(string), "number of partitions:", agentConfig[TOPIC_PARTITION_NUMBER], "replica factor:", REFLICATION_FACTOR)
	createTopics(admin, []string{agentConfig[TOPIC_REQUEST].(string), agentConfig[TOPIC_RESPONSE].(string), agentConfig[TOPIC_RETRY].(string)}, agentConfig[TOPIC_PARTITION_NUMBER].(float64))
}

func createTopics(admin sarama.ClusterAdmin, topics []string, numOfPartitions float64) {
	for _, topic := range topics {
		log.Println("init created topic: ", topic)
		topicDetail := &sarama.TopicDetail{}

		topicDetail.NumPartitions = int32(numOfPartitions)
		//topicDetail.NumPartitions = int32(3)

		topicDetail.ReplicationFactor = REFLICATION_FACTOR
		topicDetail.ConfigEntries = make(map[string]*string)

		err := admin.CreateTopic(topic, topicDetail, true)
		//err := admin.CreateTopic("sync", topicDetail, true)
		if err != nil {
			log.Println("Failed to create topic:", err)
			//	panic(err)
		}
	}
}

func GetConfiguration(consumerMode string) map[string]interface{} {
	if consumerMode != "normal" && consumerMode != "batch" {
		log.Fatalln("the mode flag is wrong")
	}
	configFile, err := os.Open("./config.json")
	if err != nil {
		log.Fatalln("Failed to load config file", err)
	}
	log.Println("Successfully Opened config.json")
	defer configFile.Close()

	config := make(map[string]interface{})
	byteValue, err := ioutil.ReadAll(configFile)
	if err != nil {
		log.Fatalln("Failed to read config file", err)
	}
	err = json.Unmarshal(byteValue, &config)
	if err != nil {
		log.Fatalln("Failed to unmarshal config file", err)
	}
	return config[consumerMode].(map[string]interface{})
}
