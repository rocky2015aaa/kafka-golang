package client

import (
	"fmt"
	"reflect"
	"testing"
)

var (
	agentTestConfig             map[string]interface{}
	kafkaRequestTestAgentClient *KafkaAgentClient
)

func init() {
	MsgLogger, _ = NewMsgLogger("", "", true)
	agentTestConfig = GetConfiguration("./config.json", "batch")
	kafkaRequestTestAgentClient = &KafkaAgentClient{
		AgentConfig: agentTestConfig,
	}
}

func TestManageConfig(t *testing.T) {
	fmt.Println("--- TestManageConfig Success Case Test ---")

	var successTests = []struct {
		req      map[string]interface{}
		result   map[string]interface{}
		expected map[string]interface{}
	}{
		{
			map[string]interface{}{
				"svc": float64(81),
				"param": map[string]interface{}{
					"retryCode": "MVCC_READ_CONFLICT",
					"rtyCycle":  "7",
					"rtyMCnt":   "5",
				},
			},
			kafkaRequestTestAgentClient.AgentConfig["retry_config"].(map[string]interface{})["MVCC_READ_CONFLICT"].(map[string]interface{}),
			map[string]interface{}{
				"rtyCycle": float64(7),
				"rtyMCnt":  5,
			},
		},
		{
			map[string]interface{}{
				"svc": float64(82),
				"param": map[string]interface{}{
					"btcCode":  "2",
					"btcCycle": "3",
					"btcMCnt":  "2",
				},
			},
			kafkaRequestTestAgentClient.AgentConfig["batch_svc_list"].(map[string]interface{})["2"].(map[string]interface{}),
			map[string]interface{}{
				"batch_msg_url": "/wallet",
				"btcCycle":      float64(3),
				"btcMCnt":       2,
			},
		},
	}

	for _, test := range successTests {
		kafkaRequestTestAgentClient.ManageConfig(test.req)
		if !reflect.DeepEqual(test.result, test.expected) {
			t.Errorf("ManageConfig(%v) was incorrect, got: %v, want: %v", test.req, test.result, test.expected)
		}
	}

	fmt.Println("--- TestManageConfig Failure Case Test ---")

	var failureTests = []struct {
		req      map[string]interface{}
		result   map[string]interface{}
		expected map[string]interface{}
	}{
		{
			map[string]interface{}{
				"svc": float64(81),
				"param": map[string]interface{}{
					"retryCode": "MVCC_READ_CONFLICT",
					"rtyCycle":  "A",
					"rtyMCnt":   "B",
				},
			},
			kafkaRequestTestAgentClient.AgentConfig["retry_config"].(map[string]interface{})["MVCC_READ_CONFLICT"].(map[string]interface{}),
			map[string]interface{}{
				"rtyCycle": float64(7),
				"rtyMCnt":  float64(5),
			},
		},
		{
			map[string]interface{}{
				"svc": float64(82),
				"param": map[string]interface{}{
					"btcCode":  "2",
					"btcCycle": "C",
					"btcMCnt":  "D",
				},
			},
			kafkaRequestTestAgentClient.AgentConfig["batch_svc_list"].(map[string]interface{})["2"].(map[string]interface{}),
			map[string]interface{}{
				"batch_msg_url": "test",
				"btcCycle":      3,
				"btcMCnt":       2,
			},
		},
	}

	for _, test := range failureTests {
		kafkaRequestTestAgentClient.ManageConfig(test.req)
		if reflect.DeepEqual(test.result, test.expected) {
			t.Errorf("ManageConfig does not get error")
		}
	}

}
