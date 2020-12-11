package client

import (
	"github.com/m/common/msg"
	"github.com/m/kafka-agent/kafka-consumer-agent/util"
)

type HandlerReturn func() (string, map[string]interface{})

func retryHandler(reqToProducer, kConfig map[string]interface{}) HandlerReturn {
	reqToProducer["type"] = kConfig[TOPIC_RETRY].(string)
	count := util.TypeToInt(reqToProducer[PRODUCER_FIELD_CNT_INFO].(map[string]interface{})["count"])
	reqToProducer[PRODUCER_FIELD_CNT_INFO].(map[string]interface{})["count"] = count + 1
	//	reqToProducer["svc"] = reqData["svc"]

	return func() (string, map[string]interface{}) {
		MsgLogger.Printf("request to producer: %+v\n", reqToProducer)
		return kConfig[TOPIC_RETRY].(string), reqToProducer
	}
}

func failureHandler(reqToProducer, kConfig map[string]interface{}) HandlerReturn {
	reqToProducer[PRODUCER_FIELD_CODE] = kConfig[RESULT_CODE_FAILURE].(string)
	reqToProducer["type"] = kConfig[TOPIC_RETRY].(string)
	//	reqToProducer["svc"] = reqData["svc"]

	return respHandler(kConfig[TOPIC_RESPONSE].(string), reqToProducer)
}

func respHandler(topic string, reqToProducer map[string]interface{}) HandlerReturn {
	return func() (string, map[string]interface{}) {
		MsgLogger.Printf("request to producer: %+v\n", reqToProducer)
		return topic, reqToProducer
	}
}

func errorHandler(errObj msg.CommonMsg, idVal, topic string) HandlerReturn {
	reqToProducer := make(map[string]interface{})
	reqToProducer["id"] = idVal
	reqToProducer[PRODUCER_FIELD_CODE] = errObj.Code
	reqToProducer[PRODUCER_FIELD_DESC] = errObj.Desc
	reqToProducer[PRODUCER_FIELD_MESSAGE] = errObj.Message
	return func() (string, map[string]interface{}) {
		MsgLogger.Printf("request to producer: %+v\n", reqToProducer)
		return topic, reqToProducer
	}
}
