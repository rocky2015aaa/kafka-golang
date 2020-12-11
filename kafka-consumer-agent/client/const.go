package client

const (
	REDIS_URL     = "kafka1-internal.m-fws.io:6379"
	BROKER_URL_01 = "kafka1-internal.m-fws.io:9092"
	BROKER_URL_02 = "kafka2-internal.m-fws.io:9092"
	BROKER_URL_03 = "kafka3-internal.m-fws.io:9092"
	RESTFUL_URL   = "http://rest.m.io"

	PRODUCER_FIELD_CODE     = "code"
	PRODUCER_FIELD_DESC     = "desc"
	PRODUCER_FIELD_MESSAGE  = "message"
	PRODUCER_FIELD_CNT_INFO = "retry-info"

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

	BATCH_CONSUMER_CLIENT_ID  = "batch_consumer_client_id"
	BATCH_CONSUMER_GROUP_NAME = "batch_consumer_group_name"

	TOPIC_REQUEST  = "topic_request"
	TOPIC_RESPONSE = "topic_response"
	TOPIC_RETRY    = "topic_retry"

	RESULT_CODE_SUCCESS = "result_code_success"
	RESULT_CODE_FAILURE = "result_code_failure"
	RESULT_CODE_RETRY   = "result_code_retry"

	TOPIC_PARTITION_NUMBER = "numberOfPartitions"

	// restful server response field
	CODE     = "code"
	REF_ID   = "refID"
	RETRY_ID = "retryID"
)
