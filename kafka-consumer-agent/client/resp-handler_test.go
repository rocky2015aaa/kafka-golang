package client

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/m/common/msg"
)

const (
	BATCH_RETRY    = "batch_retry"
	BATCH_RESPONSE = "batch_resp"
	QU_RTY_E00     = "QU_RTY_E00"
)

func init() {
	MsgLogger, _ = NewMsgLogger("", "", true)
}

func TestRetryHandler(t *testing.T) {
	fmt.Println("--- TestRetryHandler Success Case Test ---")

	var successTests = []struct {
		req        map[string]interface{}
		conf       map[string]interface{}
		expected_1 string
		expected_2 map[string]interface{}
	}{
		{
			map[string]interface{}{
				PRODUCER_FIELD_CNT_INFO: map[string]interface{}{
					"resp_code": "MCC_READ_CONFLICT",
					"count":     float64(1),
				},
			},
			map[string]interface{}{
				TOPIC_RETRY: BATCH_RETRY,
			},
			BATCH_RETRY,
			map[string]interface{}{
				PRODUCER_FIELD_CNT_INFO: map[string]interface{}{
					"resp_code": "MCC_READ_CONFLICT",
					"count":     2,
				},
				"type": BATCH_RETRY,
			},
		},
	}

	for _, test := range successTests {
		topic, req := retryHandler(test.req, test.conf)()
		if topic != test.expected_1 {
			t.Errorf("retryHandler(%v, %v) was incorrect, got: %v, %v, want: %v, %v", test.req, test.conf, topic, req, test.expected_1, test.expected_2)
		}
		if !reflect.DeepEqual(req, test.expected_2) {
			t.Errorf("retryHandler(%v, %v) was incorrect, got: %v, %v, want: %v, %v", test.req, test.conf, topic, req, test.expected_1, test.expected_2)
		}
	}
}

func TestFailureHandler(t *testing.T) {
	fmt.Println("--- TestFailureHandler Success Case Test ---")

	var successTests = []struct {
		req        map[string]interface{}
		conf       map[string]interface{}
		expected_1 string
		expected_2 map[string]interface{}
	}{
		{
			map[string]interface{}{},
			map[string]interface{}{
				RESULT_CODE_FAILURE: QU_RTY_E00,
				TOPIC_RETRY:         BATCH_RETRY,
				TOPIC_RESPONSE:      BATCH_RESPONSE,
			},
			BATCH_RESPONSE,
			map[string]interface{}{
				"type":              BATCH_RETRY,
				PRODUCER_FIELD_CODE: QU_RTY_E00,
			},
		},
	}

	for _, test := range successTests {
		topic, req := failureHandler(test.req, test.conf)()
		if topic != test.expected_1 {
			t.Errorf("failureHandler(%v, %v) was incorrect, got: %v, %v, want: %v, %v", test.req, test.conf, topic, req, test.expected_1, test.expected_2)
		}
		if !reflect.DeepEqual(req, test.expected_2) {
			t.Errorf("failureHandler(%v, %v) was incorrect, got: %v, %v, want: %v, %v", test.req, test.conf, topic, req, test.expected_1, test.expected_2)
		}
	}
}

func TestRespHandler(t *testing.T) {
	fmt.Println("--- TestRespHandler Success Case Test ---")

	var successTests = []struct {
		req        map[string]interface{}
		topic      string
		expected_1 string
		expected_2 map[string]interface{}
	}{
		{
			map[string]interface{}{},
			"test",
			"test",
			map[string]interface{}{},
		},
	}

	for _, test := range successTests {
		topic, req := respHandler(test.topic, test.req)()
		if topic != test.expected_1 {
			t.Errorf("respHandler(%v, %v) was incorrect, got: %v, %v, want: %v, %v", test.req, test.topic, topic, req, test.expected_1, test.expected_2)
		}
		if !reflect.DeepEqual(req, test.expected_2) {
			t.Errorf("respHandler(%v, %v) was incorrect, got: %v, %v, want: %v, %v", test.req, test.topic, topic, req, test.expected_1, test.expected_2)
		}
	}
}

func TestErrorHandler(t *testing.T) {
	fmt.Println("--- TestErrorHandler Success Case Test ---")

	var successTests = []struct {
		errObj     msg.CommonMsg
		idVal      string
		topic      string
		expected_1 string
		expected_2 map[string]interface{}
	}{
		{
			msg.CommonMsg{
				Code:    QU_RTY_E00,
				Desc:    "test_desc",
				Message: "test_message",
			},
			"test_idVal",
			BATCH_RESPONSE,
			BATCH_RESPONSE,
			map[string]interface{}{
				"id":                   "test_idVal",
				PRODUCER_FIELD_CODE:    QU_RTY_E00,
				PRODUCER_FIELD_DESC:    "test_desc",
				PRODUCER_FIELD_MESSAGE: "test_message",
			},
		},
	}

	for _, test := range successTests {
		topic, req := errorHandler(test.errObj, test.idVal, test.topic)()
		if topic != test.expected_1 {
			t.Errorf("failureHandler(%v, %v, %v) was incorrect, got: %v, %v, want: %v, %v", test.errObj, test.idVal, test.topic, topic, req, test.expected_1, test.expected_2)
		}
		if !reflect.DeepEqual(req, test.expected_2) {
			t.Errorf("failureHandler(%v, %v, %v) was incorrect, got: %v, %v, want: %v, %v", test.errObj, test.idVal, test.topic, topic, req, test.expected_1, test.expected_2)
		}
	}
}
