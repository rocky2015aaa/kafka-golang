package client

import (
	"fmt"
	"reflect"
	"testing"
)

func init() {
	MsgLogger, _ = NewMsgLogger("", "", true)
}

func TestParseMessage(t *testing.T) {
	fmt.Println("--- TestParseMessage Success Case Test ---")

	var successTests = []struct {
		value    []byte
		expected map[string]interface{}
	}{
		{
			[]byte(`{
	            "id": "5d5a5d111db4a874efff8f75",
	            "param": "{\"test\":\"test\"}",
                "method": "POST",
	            "url": "http://m.io",
                "svc": "10"
            }`),
			map[string]interface{}{
				"id":                         "5d5a5d111db4a874efff8f75",
				REST_REQUEST_FIELD_PARAMETER: "{\"test\":\"test\"}",
				REST_REQUEST_FIELD_METHOD:    "POST",
				REST_REQUEST_FIELD_URL:       "http://m.io",
				"svc":                        "10",
			},
		},
	}

	for _, test := range successTests {
		reqData, err := parseMessage(test.value)
		if err != nil {
			t.Errorf("parseMessage(%v) was incorrect, got: %v, %v, want: %v", test.value, reqData, err, test.expected)
		}
		if !reflect.DeepEqual(reqData, test.expected) {
			t.Errorf("parseMessage(%v) was incorrect, got: %v, %v, want: %v", test.value, reqData, err, test.expected)
		}
	}

	fmt.Println("--- TestParseMessage Failure Case Test ---")

	var failureTests = []struct {
		value []byte
	}{
		{
			[]byte(`{
			"id": "5d5a5d111db4a874efff8f75",
			"param": "{"test":"test"}",
		    "method": "POST",
			"url": "http://m.io",
		    "svc": "10"
		}`),
		},
		{
			[]byte(`{
					"id": "5d5a5d111db4a874efff8f75",
					"param": "{\"test\":\"test\"}",
					"url": "http://m.io",
				    "svc": "10"
				}`),
		},
		{
			[]byte(`{
			"id": "5d5a5d111db4a874efff8f75",
			"param": "{\"test\":\"test\"}",
		    "method": "POST",
			"url": "http://m.io"
		}`),
		},
	}

	for _, test := range failureTests {
		reqData, err := parseMessage(test.value)
		if err == nil {
			t.Errorf("parseMessage(%v) got nil", reqData)
		}
	}
}

func TestHandleRestServerResp(t *testing.T) {
	fmt.Println("--- TestHandleRestServerResp Success Case Test ---")

	var successTests = []struct {
		reqData     map[string]interface{}
		restResp    map[string]interface{}
		agentConfig map[string]interface{}
		expected_1  string
		expected_2  map[string]interface{}
	}{
		{
			map[string]interface{}{
				PRODUCER_FIELD_CNT_INFO: map[string]interface{}{
					"resp_code": "RS_RES_N01",
					"count":     float64(0),
				},
				"type": "batch-req",
				"svc":  "1",
			},
			map[string]interface{}{
				"svc":  "10",
				CODE:   "RS_RES_N01",
				REF_ID: "test-reference",
			},
			map[string]interface{}{
				"retry_config": map[string]interface{}{
					"MVCC_READ_CONFLICT": map[string]interface{}{
						"rtyCycle": float64(0.5),
						"rtyMCnt":  float64(10),
					},
					"PHANTOM_READ_CONFLICT": map[string]interface{}{
						"rtyCycle": float64(0.5),
						"rtyMCnt":  float64(10),
					},
					"COM_SGC_E03": map[string]interface{}{
						"rtyCycle": float64(1),
						"rtyMCnt":  float64(10),
					},
					"NET_ERROR": map[string]interface{}{
						"rtyCycle": float64(1),
						"rtyMCnt":  float64(10),
					},
				},
				RESULT_CODE_RETRY: "RETRY",
				TOPIC_RETRY:       "batch-retry",
				TOPIC_RESPONSE:    "batch-resp",
			},
			"batch-resp",
			map[string]interface{}{
				PRODUCER_FIELD_CNT_INFO: map[string]interface{}{
					"resp_code": "RS_RES_N01",
					"count":     float64(0),
				},
				REF_ID: "test-reference",
				CODE:   "RS_RES_N01",
				"type": "batch-req",
				"svc":  "1",
			},
		},
		{
			map[string]interface{}{
				PRODUCER_FIELD_CNT_INFO: map[string]interface{}{
					"resp_code": "MVCC_READ_CONFLICT",
					"count":     float64(2),
				},
				"type": "batch-retry",
				"svc":  "1",
			},
			map[string]interface{}{
				CODE:   "MVCC_READ_CONFLICT",
				REF_ID: "test-reference",
			},
			map[string]interface{}{
				"retry_config": map[string]interface{}{
					"MVCC_READ_CONFLICT": map[string]interface{}{
						"rtyCycle": float64(0.5),
						"rtyMCnt":  float64(10),
					},
					"PHANTOM_READ_CONFLICT": map[string]interface{}{
						"rtyCycle": float64(0.5),
						"rtyMCnt":  float64(10),
					},
					"COM_SGC_E03": map[string]interface{}{
						"rtyCycle": float64(1),
						"rtyMCnt":  float64(10),
					},
					"NET_ERROR": map[string]interface{}{
						"rtyCycle": float64(1),
						"rtyMCnt":  float64(10),
					},
				},
				RESULT_CODE_RETRY: "RETRY",
				TOPIC_RETRY:       "batch-retry",
				TOPIC_RESPONSE:    "batch-resp",
			},
			"batch-retry",
			map[string]interface{}{
				PRODUCER_FIELD_CNT_INFO: map[string]interface{}{
					"resp_code": "MVCC_READ_CONFLICT",
					"count":     3,
				},
				"type": "batch-retry",
				"svc":  "1",
			},
		},
		{
			map[string]interface{}{
				PRODUCER_FIELD_CNT_INFO: map[string]interface{}{
					"resp_code": "PHANTOM_READ_CONFLICT",
					"count":     float64(10),
				},
				"type": "batch-retry",
				"svc":  "1",
			},
			map[string]interface{}{
				CODE:   "PHANTOM_READ_CONFLICT",
				REF_ID: "test-reference",
			},
			map[string]interface{}{
				"retry_config": map[string]interface{}{
					"MVCC_READ_CONFLICT": map[string]interface{}{
						"rtyCycle": float64(0.5),
						"rtyMCnt":  float64(10),
					},
					"PHANTOM_READ_CONFLICT": map[string]interface{}{
						"rtyCycle": float64(0.5),
						"rtyMCnt":  float64(10),
					},
					"COM_SGC_E03": map[string]interface{}{
						"rtyCycle": float64(1),
						"rtyMCnt":  float64(10),
					},
					"NET_ERROR": map[string]interface{}{
						"rtyCycle": float64(1),
						"rtyMCnt":  float64(10),
					},
				},
				RESULT_CODE_RETRY:   "RETRY",
				RESULT_CODE_FAILURE: "FAILURE",
				TOPIC_RETRY:         "batch-retry",
				TOPIC_RESPONSE:      "batch-resp",
			},
			"batch-resp",
			map[string]interface{}{
				PRODUCER_FIELD_CNT_INFO: map[string]interface{}{
					"resp_code": "PHANTOM_READ_CONFLICT",
					"count":     float64(10),
				},
				REF_ID: "test-reference",
				CODE:   "FAILURE",
				"type": "batch-retry",
				"svc":  "1",
			},
		},
		{
			map[string]interface{}{
				PRODUCER_FIELD_CNT_INFO: map[string]interface{}{
					"resp_code": "RS_RES_N01",
					"count":     float64(3),
				},
				"type": "batch-req",
				"svc":  "1",
			},
			map[string]interface{}{
				CODE:  "RS_RES_N01",
				"svc": "10",
			},
			map[string]interface{}{
				"retry_config": map[string]interface{}{
					"MVCC_READ_CONFLICT": map[string]interface{}{
						"rtyCycle": float64(0.5),
						"rtyMCnt":  float64(10),
					},
					"PHANTOM_READ_CONFLICT": map[string]interface{}{
						"rtyCycle": float64(0.5),
						"rtyMCnt":  float64(10),
					},
					"COM_SGC_E03": map[string]interface{}{
						"rtyCycle": float64(1),
						"rtyMCnt":  float64(10),
					},
					"NET_ERROR": map[string]interface{}{
						"rtyCycle": float64(1),
						"rtyMCnt":  float64(10),
					},
				},
				RESULT_CODE_RETRY: "RETRY",
				TOPIC_RETRY:       "batch-retry",
				TOPIC_RESPONSE:    "batch-resp",
			},
			"Eventlistener Process",
			map[string]interface{}{
				PRODUCER_FIELD_CNT_INFO: map[string]interface{}{
					"resp_code": "RS_RES_N01",
					"count":     float64(3),
				},
				"type": "batch-req",
				"svc":  "1",
			},
		},
	}

	for _, test := range successTests {
		topic, req := handleRestServerResp(test.reqData, test.restResp, test.agentConfig)()
		if topic != test.expected_1 {
			t.Errorf("handleRestServerResp(%v, %v, %v) was incorrect, got: %v, %v, want: %v, %v", test.reqData, test.restResp, test.agentConfig, topic, req, test.expected_1, test.expected_2)
		}
		if !reflect.DeepEqual(req, test.expected_2) {
			t.Errorf("handleRestServerResp(%v, %v, %v) was incorrect, got: %v, %v, want: %v, %v", test.reqData, test.restResp, test.agentConfig, topic, req, test.expected_1, test.expected_2)
		}
	}
}

type netError struct {
	error
}

func (ne netError) Timeout() bool {
	return true
}

func (ne netError) Temporary() bool {
	return true
}

func (ne netError) Error() string {
	return ""
}

func TestIsErrorForRetry(t *testing.T) {
	fmt.Println("--- TestIsErrorForRetry Success Case Test ---")

	var successTests = []struct {
		value    netError
		expected bool
	}{
		{
			netError{
				fmt.Errorf("error"),
			},
			true,
		},
	}

	for _, test := range successTests {
		resp := isErrorForRetry(test.value)
		if !resp {
			t.Errorf("parseMessage(%v) was incorrect, got: %v, want: %v", test.value, resp, test.expected)
		}
	}

	fmt.Println("--- TestIsErrorForRetry Failure Case Test ---")

	var failureTests = []struct {
		value    error
		expected bool
	}{
		{
			fmt.Errorf("error"),
			true,
		},
	}

	for _, test := range failureTests {
		resp := isErrorForRetry(test.value)
		if resp {
			t.Errorf("parseMessage(%v) was incorrect, got: %v, want: %v", test.value, resp, test.expected)
		}
	}
}

func TestCreateMultipleRequest(t *testing.T) {
	fmt.Println("--- TestcreateMultipleRequest Success Case Test ---")

	var successTests = []struct {
		value    []string
		expected map[string]interface{}
	}{
		{
			[]string{
				`{"type":"batch-req","id":"STL_DAD_605433","svc":6,"param":{"svc":"6","procDate":"2019-07-20 00:00:08","walletAddr":"aRaF97Cy6tH4MBEH7m89g3GAjF86NV5wzMJr936M","xValue":"46ce90168804d850895175388e6c684f610780fe26ee0781b91eac5f79fb7ca3","sHash":"100430577120109877685509869260197274453504990476195546392436649947639109016237","rHash":"57463961385727144440005087844817137468853846169600413838176911803726013875250","refID":"STL_DAD_605433","yValue":"ce0a8f1065bc7c98464993d3f805ca88401a84084f708d152ab6a8b15e7bf915","penaltySeedSum":"17342.3","validationCheck":"true","penaltySeed":"0.2"},"method":"PUT","url":"/dad/DDW"}`,
				`{"type":"batch-req","id":"STL_DAD_605435","svc":6,"param":{"svc":"6","procDate":"2019-07-20 00:00:08","walletAddr":"EmvhraJtiPjV9tsNgDHjtMxcnDa4h7iWF2VPYfnF","xValue":"584d6c8539bbec532eabb83454f81a7b2d31cca2c23de50b3c41a7f13e2af831","sHash":"30363239317035371238069883156880407212788592268292239853895548915356892253891","rHash":"42036327478306372243160114575999642610122517022757817397248983387557445055705","refID":"STL_DAD_605435","yValue":"b6d0dbb61d27f431b43028ec14e6d965557a96aad736d8e6f8e83d74740eb780","penaltySeedSum":"17342.3","validationCheck":"true","penaltySeed":"4.0"},"method":"PUT","url":"/dad/DDW"}`,
				`{"type":"batch-req","id":"STL_DAD_605422","svc":6,"param":{"svc":"6","procDate":"2019-07-20 00:00:08","walletAddr":"e2sUAjrFoS4B6DRnRJ4kkWvWWuizNduM6zMgqvPC","xValue":"8ef24e25aae26d887e1cc754c2d844a2c809a8ac56f3c34f8d101cbaa0a061b9","sHash":"96915354928666911437896813975415775842837655131972853224128223829816340827293","rHash":"75222902346371520455418407236045213716465737370206543768992031903525745909069","refID":"STL_DAD_605422","yValue":"ba8ea1fff4ce1b123b6f50dad7b457eb2d698abf8f66c61763a675648c91b412","penaltySeedSum":"17342.3","validationCheck":"true","penaltySeed":"2.0"},"method":"PUT","url":"/dad/DDW"}`,
			},
			map[string]interface{}{
				"svc":             "6!6!6",
				"procDate":        "2019-07-20 00:00:08!2019-07-20 00:00:08!2019-07-20 00:00:08",
				"walletAddr":      "aRaF97Cy6tH4MBEH7m89g3GAjF86NV5wzMJr936M!EmvhraJtiPjV9tsNgDHjtMxcnDa4h7iWF2VPYfnF!e2sUAjrFoS4B6DRnRJ4kkWvWWuizNduM6zMgqvPC",
				"xValue":          "46ce90168804d850895175388e6c684f610780fe26ee0781b91eac5f79fb7ca3!584d6c8539bbec532eabb83454f81a7b2d31cca2c23de50b3c41a7f13e2af831!8ef24e25aae26d887e1cc754c2d844a2c809a8ac56f3c34f8d101cbaa0a061b9",
				"sHash":           "100430577120109877685509869260197274453504990476195546392436649947639109016237!30363239317035371238069883156880407212788592268292239853895548915356892253891!96915354928666911437896813975415775842837655131972853224128223829816340827293",
				"rHash":           "57463961385727144440005087844817137468853846169600413838176911803726013875250!42036327478306372243160114575999642610122517022757817397248983387557445055705!75222902346371520455418407236045213716465737370206543768992031903525745909069",
				"refID":           "STL_DAD_605433!STL_DAD_605435!STL_DAD_605422",
				"yValue":          "ce0a8f1065bc7c98464993d3f805ca88401a84084f708d152ab6a8b15e7bf915!b6d0dbb61d27f431b43028ec14e6d965557a96aad736d8e6f8e83d74740eb780!ba8ea1fff4ce1b123b6f50dad7b457eb2d698abf8f66c61763a675648c91b412",
				"penaltySeedSum":  "17342.3!17342.3!17342.3",
				"validationCheck": "true!true!true",
				"penaltySeed":     "0.2!4.0!2.0",
				"separator":       SEPARATOR,
			},
		},
	}

	for _, test := range successTests {
		request, err := createMultipleRequest(test.value)
		if err != nil {
			t.Errorf("parseMessage(%v) was incorrect, got: %v, want: %v", test.value, request, test.expected)
		}
		test.expected["refId"] = request["refId"].(string)
		test.expected["date"] = request["date"].(string)
		if !reflect.DeepEqual(request, test.expected) {
			t.Errorf("parseMessage(%v) was incorrect, got: %v, want: %v", test.value, request, test.expected)
		}
	}

	fmt.Println("--- TestcreateMultipleRequest Failure Case Test ---")

	var failureTests = []struct {
		value    []string
		expected map[string]interface{}
	}{
		{
			[]string{
				`type":"batch-req","id":"STL_DAD_605433","svc":6,"param":{"svc":"6","procDate":"2019-07-20 00:00:08","walletAddr":"aRaF97Cy6tH4MBEH7m89g3GAjF86NV5wzMJr936M","xValue":"46ce90168804d850895175388e6c684f610780fe26ee0781b91eac5f79fb7ca3","sHash":"100430577120109877685509869260197274453504990476195546392436649947639109016237","rHash":"57463961385727144440005087844817137468853846169600413838176911803726013875250","refID":"STL_DAD_605433","yValue":"ce0a8f1065bc7c98464993d3f805ca88401a84084f708d152ab6a8b15e7bf915","penaltySeedSum":"17342.3","validationCheck":"true","penaltySeed":"0.2"},"method":"PUT","url":"/dad/DDW"}`,
				`{"type":"batch-req","id":"STL_DAD_605435","svc":6,"param":{"svc":"6","procDate":"2019-07-20 00:00:08","walletAddr":"EmvhraJtiPjV9tsNgDHjtMxcnDa4h7iWF2VPYfnF","xValue":"584d6c8539bbec532eabb83454f81a7b2d31cca2c23de50b3c41a7f13e2af831","sHash":"30363239317035371238069883156880407212788592268292239853895548915356892253891","rHash":"42036327478306372243160114575999642610122517022757817397248983387557445055705","refID":"STL_DAD_605435","yValue":"b6d0dbb61d27f431b43028ec14e6d965557a96aad736d8e6f8e83d74740eb780","penaltySeedSum":"17342.3","validationCheck":"true","penaltySeed":"4.0"},"method":"PUT","url":"/dad/DDW"}`,
				`{"type":"batch-req","id":"STL_DAD_605422","svc":6,"param":{"svc":"6","procDate":"2019-07-20 00:00:08","walletAddr":"e2sUAjrFoS4B6DRnRJ4kkWvWWuizNduM6zMgqvPC","xValue":"8ef24e25aae26d887e1cc754c2d844a2c809a8ac56f3c34f8d101cbaa0a061b9","sHash":"96915354928666911437896813975415775842837655131972853224128223829816340827293","rHash":"75222902346371520455418407236045213716465737370206543768992031903525745909069","refID":"STL_DAD_605422","yValue":"ba8ea1fff4ce1b123b6f50dad7b457eb2d698abf8f66c61763a675648c91b412","penaltySeedSum":"17342.3","validationCheck":"true","penaltySeed":"2.0"},"method":"PUT","url":"/dad/DDW"}`,
			},
			map[string]interface{}{
				"svc":             "6!6!6",
				"procDate":        "2019-07-20 00:00:08!2019-07-20 00:00:08!2019-07-20 00:00:08",
				"walletAddr":      "aRaF97Cy6tH4MBEH7m89g3GAjF86NV5wzMJr936M!EmvhraJtiPjV9tsNgDHjtMxcnDa4h7iWF2VPYfnF!e2sUAjrFoS4B6DRnRJ4kkWvWWuizNduM6zMgqvPC",
				"xValue":          "46ce90168804d850895175388e6c684f610780fe26ee0781b91eac5f79fb7ca3!584d6c8539bbec532eabb83454f81a7b2d31cca2c23de50b3c41a7f13e2af831!8ef24e25aae26d887e1cc754c2d844a2c809a8ac56f3c34f8d101cbaa0a061b9",
				"sHash":           "100430577120109877685509869260197274453504990476195546392436649947639109016237!30363239317035371238069883156880407212788592268292239853895548915356892253891!96915354928666911437896813975415775842837655131972853224128223829816340827293",
				"rHash":           "57463961385727144440005087844817137468853846169600413838176911803726013875250!42036327478306372243160114575999642610122517022757817397248983387557445055705!75222902346371520455418407236045213716465737370206543768992031903525745909069",
				"refID":           "STL_DAD_605433!STL_DAD_605435!STL_DAD_605422",
				"yValue":          "ce0a8f1065bc7c98464993d3f805ca88401a84084f708d152ab6a8b15e7bf915!b6d0dbb61d27f431b43028ec14e6d965557a96aad736d8e6f8e83d74740eb780!ba8ea1fff4ce1b123b6f50dad7b457eb2d698abf8f66c61763a675648c91b412",
				"penaltySeedSum":  "17342.3!17342.3!17342.3",
				"validationCheck": "true!true!true",
				"penaltySeed":     "0.2!4.0!2.0",
				"separator":       SEPARATOR,
			},
		},
	}

	for _, test := range failureTests {
		_, err := createMultipleRequest(test.value)
		if err == nil {
			t.Errorf("parseMessage(%v) got nil err", test.value)
		}
	}
}
