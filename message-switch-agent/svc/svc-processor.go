package svc

import (
	"encoding/json"
	"fmt"
	"time"
)

func Sync13(switchedMsg, mappedMsg map[string]interface{}) [][]byte {
	syncMsgs := [][]byte{}
	senderSyncData, senderSyncDataOk := mappedMsg["senderSyncData"]
	fmt.Printf("sender syncData = %v\n", senderSyncData)
	fmt.Printf("sender syncDataOk = %t\n", senderSyncDataOk)

	if senderSyncDataOk && senderSyncData != nil && len(senderSyncData.([]interface{})[1].(string)) > 0 {
		switchedMsg["param"] = map[string]interface{}{
			"walletAddr": mappedMsg["senderAddr"].(string),
			"startRefID": mappedMsg["senderSyncData"].([]interface{})[0].(string),
			"endRefID":   mappedMsg["senderSyncData"].([]interface{})[1].(string),
		}
		syncMsgs = append(syncMsgs, createSyncMsg("sender", switchedMsg))
	}

	receiverSyncData, receiverSyncDataOk := mappedMsg["receiverSyncData"]

	fmt.Printf("receiver syncData = %v\n", receiverSyncData)
	fmt.Printf("receiver syncDataOk = %t\n", receiverSyncDataOk)

	if receiverSyncDataOk && receiverSyncData != nil && receiverSyncData.(interface{}).(string) != "" {
		switchedMsg["param"] = map[string]interface{}{
			"walletAddr":      mappedMsg["receiverAddr"].(string),
			"startRefID":      mappedMsg["receiverSyncData"].(interface{}).(string),
			"endRefID":        mappedMsg["receiverSyncData"].(interface{}).(string),
			"paymentWalletYn": "Y",
		}
		syncMsgs = append(syncMsgs, createSyncMsg("receiver", switchedMsg))
	}

	return syncMsgs
}

func Sync14(switchedMsg, mappedMsg map[string]interface{}) [][]byte {
	syncMsgs := [][]byte{}
	syncData, syncDataOk := mappedMsg["nextFruitSyncData"]
	if syncDataOk && syncData != nil && len(syncData.([]interface{})[1].(string)) > 0 {
		switchedMsg["param"] = map[string]interface{}{
			"walletAddr": mappedMsg["walletAddr"].(string),
			"startRefID": mappedMsg["nextFruitSyncData"].([]interface{})[0].(string),
			"endRefID":   mappedMsg["nextFruitSyncData"].([]interface{})[1].(string),
		}
		syncMsgs = append(syncMsgs, createSyncMsg("next", switchedMsg))
	}
	return syncMsgs
}

func createSyncMsg(from string, switchedMsg map[string]interface{}) []byte {
	procDate := time.Now().Format("2006-01-02 15:04:05")
	newId := from + "_" + switchedMsg["param"].(map[string]interface{})["startRefID"].(string) + "_" + procDate
	switchedMsg["type"] = "sync"
	switchedMsg["refID"] = newId
	switchedMsg["svc"] = 14
	switchedMsg["method"] = "PUT"
	switchedMsg["url"] = "/wallet/sync"
	switchedMsg["param"].(map[string]interface{})["refID"] = newId
	switchedMsg["param"].(map[string]interface{})["procDate"] = procDate
	switchedMsg["param"].(map[string]interface{})["svc"] = "14"
	msgJson, _ := json.Marshal(switchedMsg)
	fmt.Println("**** to: sync", string(msgJson))
	return msgJson
}
