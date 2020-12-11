package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"

	"github.com/m/kafka-agent/message-switch-agent/svc"
	//"github.com/olivere/elastic"
)

var (
	brokers               = []string{"kafka1-internal.m-fws.io:9092", "kafka2-internal.m-fws.io:9092", "kafka3-internal.m-fws.io:9092"}
	topicFrom goka.Stream = "batch-resp"
	topicTo   goka.Stream = "sync"
	group     goka.Group  = "message-switch-group"
	svcFunc   map[int]func(map[string]interface{}, map[string]interface{}) [][]byte
	ctext     context.Context
)

func init() {
	fmt.Println("call init")

	svcFunc = map[int]func(map[string]interface{}, map[string]interface{}) [][]byte{
		13: svc.Sync13,
		14: svc.Sync14,
	}

	fmt.Print(svcFunc)
}

// process messages until ctrl-c is pressed
func runProcessor() {
	cb := func(ctx goka.Context, msg interface{}) {
		fmt.Println("**** from: batch-resp", msg.(string))
		switchedMsg := map[string]interface{}{}
		var mappedMsg map[string]interface{}
		err := json.Unmarshal([]byte(msg.(string)), &mappedMsg)
		if err != nil {
			fmt.Println("err:", err)
		}

		var svcInt int
		var toSync bool
		if svc, svcOk := mappedMsg["svc"]; svcOk {

			if svcStr, ok := svc.(string); ok {
				svcInt, _ = strconv.Atoi(svcStr)
			} else if svcFloat, ok := svc.(float64); ok {
				svcInt = int(svcFloat)
			}

			if svcProcessor, svcFuncOk := svcFunc[svcInt]; svcFuncOk {

				svcMsgs := svcProcessor(switchedMsg, mappedMsg)
				if len(svcMsgs) > 0 {
					for _, svcMsg := range svcMsgs {
						toSync = true
						ctx.Emit(topicTo, ctx.Key(), svcMsg)
					}
				}
			} else {
				fmt.Print("call fail")
			}
		}

		if !toSync {
			fmt.Println("*** to: none")
		}
	}

	g := goka.DefineGroup(group,
		goka.Input(topicFrom, new(codec.String), cb),
		goka.Persist(new(codec.String)),
		goka.Output(topicTo, new(codec.Bytes)),
	)

	p, err := goka.NewProcessor(brokers, g)
	if err != nil {
		log.Fatalf("error creating processor: %v", err)
	}
	ctx, cancel := context.WithCancel(ctext)
	done := make(chan bool)
	go func() {
		defer close(done)
		if err = p.Run(ctx); err != nil {
			fmt.Printf("ctx: %+v\n", ctx)
			log.Fatalf("error running processor: %v", err)
		}
	}()

	wait := make(chan os.Signal, 1)
	signal.Notify(wait, syscall.SIGINT, syscall.SIGTERM)
	<-wait   // wait for SIGINT/SIGTERM
	cancel() // gracefully stop processor
	<-done
}

func main() {
	ctext = context.Background()

	runProcessor() // press ctrl-c to stop
}
