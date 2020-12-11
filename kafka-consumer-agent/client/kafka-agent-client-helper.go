package client

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/m/common/msg"
	"github.com/m/kafka-agent/kafka-consumer-agent/util"
)

func createTimer(kac *KafkaAgentClient, svc string, svcConfig map[string]interface{}) {
	svcNum, err := strconv.ParseFloat(svc, 64)
	if err != nil {
		MsgLogger.Println(svc, "Failed to convert svc string to number", err)
	}

	rClient := redis.NewClient(&redis.Options{
		Addr:     REDIS_URL,
		Password: "",
		DB:       int(svcNum),
	})
	_, err = rClient.Ping().Result()
	if err != nil {
		MsgLogger.Println(svc, "Failed to start redis DB:", err)
	}

	kac.SvcList[svc] = &Svc{
		MessageTimer:     time.NewTimer(time.Second * time.Duration(util.TypeToInt(svcConfig["btcCycle"]))),
		RedisClient:      rClient,
		RestfulApiMethod: svcConfig["btcMethod"].(string),
		RestfulApiUrl:    svcConfig["btcUrl"].(string),
		TimerPipe:        make(chan struct{}),
	}

	go func(kc *KafkaAgentClient, svc string) {
		startTimer(kc, svc)
	}(kac, svc)
}

func startTimer(ka *KafkaAgentClient, svc string) {
	MsgLogger.Println("***", svc, "Timer is started", ka.SvcList[svc].MessageTimer)
	for {
		select {
		case <-ka.SvcList[svc].MessageTimer.C:
			svcNum, err := strconv.ParseFloat(svc, 64)
			if err != nil {
				MsgLogger.Println(svc, "Failed to convert svc string to number", err)
			}
			req := map[string]interface{}{
				"id":                      fmt.Sprintf("%v", time.Now().UnixNano()),
				"svc":                     svcNum,
				PRODUCER_FIELD_CNT_INFO:   map[string]interface{}{},
				MESSAGE_TOPIC:             "batch-resp",
				REST_REQUEST_FIELD_METHOD: ka.SvcList[svc].RestfulApiMethod,
				REST_REQUEST_FIELD_URL:    ka.SvcList[svc].RestfulApiUrl,
			}
			iter := ka.SvcList[svc].RedisClient.Scan(0, fmt.Sprintf("svc_%s_*", svc), ka.SvcList[svc].RedisClient.DBSize().Val()).Iterator()
			ka.flushMessages(req, iter)
			if _, ok := ka.AgentConfig["batch_svc_list"].(map[string]interface{})[svc]; !ok {
				delete(ka.SvcList, svc)
				MsgLogger.Println("svc", svc, "is removed from batch svc list by config change")
				return
			}
			batchTimeout := util.TypeToInt(ka.AgentConfig["batch_svc_list"].(map[string]interface{})[svc].(map[string]interface{})["btcCycle"])
			ka.SvcList[svc].MessageTimer.Reset(time.Second * time.Duration(batchTimeout))
			MsgLogger.Println("***", svc, "Timer is reseted by time expiration", time.Now())
		}
	}
}

func parseMessage(value []byte) (map[string]interface{}, error) {
	reqData := map[string]interface{}{}
	err := json.Unmarshal(value, &reqData)
	if val, ok := reqData["id"]; ok {
		reqData["id"] = val.(string)
	} else {
		reqData["id"] = fmt.Sprintf("%v", time.Now().UnixNano())
	}

	if err != nil {
		MsgLogger.Println("Failed to parse message", err, reqData)
		return nil, errors.New("Parsed request data is not valid")
	}

	for _, reqParam := range []string{"svc", REST_REQUEST_FIELD_PARAMETER} {
		if _, ok := reqData[reqParam]; !ok {
			err = errors.New("Parsed request data is not valid")
			MsgLogger.Println("Failed to parse message with", reqParam, "/ err:", err, "/ req data:", reqData)
			return nil, err
		}
	}

	svc := util.TypeToInt(reqData["svc"])
	if svc != 81 && svc != 82 {
		for _, reqParam := range []string{REST_REQUEST_FIELD_METHOD, REST_REQUEST_FIELD_URL} {
			if _, ok := reqData[reqParam]; !ok {
				err = errors.New("Parsed request data is not valid")
				MsgLogger.Println("Failed to parse message with", reqParam, "/ err:", err, "/ req data:", reqData)
				return nil, err
			}
		}
	}

	return reqData, nil
}

func createMultipleRequest(messages []string) (request map[string]interface{}, err error) {
	messageStructs := []map[string]interface{}{}
	//	for i := 0; i < len(messages)-1; i++ {
	for i := 0; i < len(messages); i++ {
		messageMap := make(map[string]interface{})
		err := json.Unmarshal([]byte(messages[i]), &messageMap)
		if err != nil {
			return nil, err
		}
		parameters := messageMap[REST_REQUEST_FIELD_PARAMETER].(map[string]interface{})

		messageStructs = append(messageStructs, parameters)
	}

	request = map[string]interface{}{}
	for i, messageStruct := range messageStructs {
		for k, v := range messageStruct {
			vs := v.(string)
			if val, ok := request[k]; ok {
				request[k] = val.(string) + vs
			} else {
				request[k] = vs
			}
		}
		if i < len(messageStructs)-1 {
			for k, _ := range messageStruct {
				request[k] = request[k].(string) + SEPARATOR
			}
		}
	}
	request["refId"] = randToken()
	request["date"] = time.Now().Format("2006-01-02 15:04:05")
	request["delimiter"] = SEPARATOR
	MsgLogger.Printf("request: %+v\n", request)

	return request, nil
}

func randToken() string {
	b := make([]byte, 8)
	rand.Read(b)
	return fmt.Sprintf("%x", b)
}

func getRestServerResp(reqData, agentConfig map[string]interface{}, topic string) (map[string]interface{}, error, HandlerReturn) {
	restResp, err := callRestServer(reqData, topic)
	cntInfo, cntInfoOk := reqData[PRODUCER_FIELD_CNT_INFO]

	if err == nil {
		if _, ok := restResp[PRODUCER_FIELD_CODE]; !ok {
			return nil, errors.New("Response Code is not valid"), errorHandler(msg.GetErrMsgStruct(err, ERR_CODE_QUEUE_REST_SERVER_RESPONSE), reqData["id"].(string), agentConfig[TOPIC_RESPONSE].(string))
		}
	} else {
		if isErrorForRetry(err) {
			if !cntInfoOk || cntInfo.(map[string]interface{})[""].(string) != "NET_ERROR" {
				reqData[PRODUCER_FIELD_CNT_INFO] = map[string]interface{}{
					"resp_code": "NET_ERROR",
					"count":     0,
				}
			}
			retryMax := util.TypeToInt(agentConfig["retry_config"].(map[string]interface{})["NET_ERROR"].(map[string]interface{})["rtyMCnt"])
			if util.TypeToInt(reqData[PRODUCER_FIELD_CNT_INFO].(map[string]interface{})["count"]) < retryMax {
				return nil, err, retryHandler(reqData, agentConfig)
			} else {
				return nil, err, failureHandler(reqData, agentConfig)
			}
		} else {
			MsgLogger.Println("Failed to call restful api", err)
			return nil, err, errorHandler(msg.GetErrMsgStruct(err, ERR_CODE_QUEUE_REST_SERVER_RESPONSE), reqData["id"].(string), agentConfig[TOPIC_RESPONSE].(string))
		}
	}

	respCode := restResp[CODE].(string)

	if !cntInfoOk || len(reqData[PRODUCER_FIELD_CNT_INFO].(map[string]interface{})) == 0 || reqData[PRODUCER_FIELD_CNT_INFO].(map[string]interface{})["resp_code"].(string) != respCode {
		reqData[PRODUCER_FIELD_CNT_INFO] = map[string]interface{}{
			"resp_code": respCode,
			"count":     0,
		}
	}

	return restResp, nil, nil
}

func callRestServer(reqData map[string]interface{}, topic string) (map[string]interface{}, error) {
	inputData := reqData[REST_REQUEST_FIELD_PARAMETER].(map[string]interface{})
	inputData["procType"] = topic
	MsgLogger.Printf("req data: %+v\n", reqData)

	params := url.Values{}
	for k, v := range inputData {
		switch v.(type) {
		case float64:
			params.Add(k, fmt.Sprintf("%d", int(v.(float64))))
		case string:
			params.Add(k, v.(string))
		}
	}

	req, err := http.NewRequest(reqData[REST_REQUEST_FIELD_METHOD].(string), RESTFUL_URL+reqData[REST_REQUEST_FIELD_URL].(string), bytes.NewBufferString(params.Encode()))
	if err != nil {
		MsgLogger.Println("Failed to create http request", err)
		return nil, err
	}
	req.Close = true

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{}
	resp, err := client.Do(req)
	MsgLogger.Printf("[restful response] : %+v\n", resp)
	if err != nil {
		MsgLogger.Println("Failed to get http response", err)
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	MsgLogger.Printf("[restful response body]: %+v\n", string(body))
	if err != nil {
		MsgLogger.Println("Failed to read http response", err)
		return nil, err
	}
	var restResp map[string]interface{}
	err = json.Unmarshal(body, &restResp)
	MsgLogger.Printf("[restful response json]: %+v\n", restResp)
	if err != nil {
		MsgLogger.Println("Failed to parse http response", err)
		return nil, fmt.Errorf("error: %s, response body: %s\n", err.Error(), string(body))
	}

	resp.Body.Close()
	return restResp, nil
}

func isErrorForRetry(err error) bool {
	// timeout, connection refused
	if _, ok := err.(net.Error); ok {
		return true
	}

	return false
}

func handleRestServerResp(reqData, restResp, agentConfig map[string]interface{}) HandlerReturn {
	countInfo := reqData[PRODUCER_FIELD_CNT_INFO].(map[string]interface{})
	respCode := restResp[PRODUCER_FIELD_CODE].(string)
	if respCode == SUCCESS_CODE_RESPONSE {
		if _, ok := restResp[RETRY_ID]; !ok {
			return respHandler("Eventlistener Process", reqData)
		}
	}

	retryList := agentConfig["retry_config"].(map[string]interface{})
	if _, ok := retryList[respCode]; ok {
		retryMax := util.TypeToInt(retryList[countInfo["resp_code"].(string)].(map[string]interface{})["rtyMCnt"])
		if util.TypeToInt(countInfo["count"]) < retryMax {
			return retryHandler(reqData, agentConfig)
		} else {
			restResp["id"] = reqData["id"]
			restResp["svc"] = reqData["svc"]
			restResp[PRODUCER_FIELD_CNT_INFO] = countInfo
			return failureHandler(restResp, agentConfig)
		}
	}

	restResp["id"] = reqData["id"]
	restResp["svc"] = reqData["svc"]
	restResp[PRODUCER_FIELD_CNT_INFO] = countInfo
	restResp["type"] = reqData["type"]

	return respHandler(agentConfig[TOPIC_RESPONSE].(string), restResp)
}

func configProcessor(agentConfig, param map[string]interface{}, keys ...string) {
	MsgLogger.Println("agentConfigi before config req:", agentConfig)
	MsgLogger.Println("param:", param, "keys:", keys)
	cycle, err := strconv.ParseFloat(param[keys[2]].(string), 64)
	if err != nil {
		MsgLogger.Println("Failed to convert string to float64", err)
		return
	}
	mCnt, err := strconv.Atoi(param[keys[3]].(string))
	if err != nil {
		MsgLogger.Println("Failed to convert string to int", err)
		return
	}
	method, mOk := param[keys[4]]
	url, uOk := param[keys[5]]

	if _, ok := agentConfig[keys[0]].(map[string]interface{}); ok {
		config := agentConfig[keys[0]].(map[string]interface{})
		if _, ok := config[param[keys[1]].(string)]; ok {
			val := config[param[keys[1]].(string)]
			if aType, ok := param["actType"]; ok {
				if aType.(string) == "delete" {
					delete(config, param[keys[1]].(string))
					MsgLogger.Println("agentConfig after config req:", agentConfig)
					return
				}
			}

			val.(map[string]interface{})[keys[2]] = cycle
			val.(map[string]interface{})[keys[3]] = mCnt
			if mOk && uOk {
				val.(map[string]interface{})[keys[4]] = method.(string)
				val.(map[string]interface{})[keys[5]] = url.(string)
			}
		} else {
			config[param[keys[1]].(string)] = map[string]interface{}{
				keys[2]: cycle,
				keys[3]: mCnt,
			}
			if mOk && uOk {
				config[param[keys[1]].(string)].(map[string]interface{})[keys[4]] = method.(string)
				config[param[keys[1]].(string)].(map[string]interface{})[keys[5]] = url.(string)
			}
		}
		MsgLogger.Println("agentConfig after config req:", agentConfig)
	} else {
		MsgLogger.Println(keys[0] + " is not a valid field")
	}
}
