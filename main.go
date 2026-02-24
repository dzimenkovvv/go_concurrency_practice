package main

import (
	"concurrency/miner"
	"concurrency/postman"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	var coal atomic.Int64

	mtx := sync.Mutex{}
	allMails := []string{}

	minerContext, minerCancel := context.WithCancel(context.Background())
	postmanContext, postmanCancel := context.WithCancel(context.Background())

	go func() {
		time.Sleep(3 * time.Second)
		fmt.Println("----->>> Рабочий день шахтеров окончен!")
		minerCancel()
	}()

	go func() {
		time.Sleep(6 * time.Second)
		fmt.Println("----->>> Рабочий день почтальона окончен!")
		postmanCancel()
	}()

	mailTransferPoint := postman.PostmanPool(postmanContext, 3)
	coalTransferPoint := miner.MinerPool(minerContext, 3)

	//initTime := time.Now()

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for val := range coalTransferPoint {
			coal.Add(int64(val))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for mail := range mailTransferPoint {
			mtx.Lock()
			allMails = append(allMails, mail)
			mtx.Unlock()
		}
	}()

	wg.Wait()

	/*for {
		if mailTransferPoint == nil && coalTransferPoint == nil {
			break
		}
		select {
		case val, ok := <-coalTransferPoint:
			if !ok {
				coalTransferPoint = nil
				continue
			}
			coal += val
		case mail, ok := <-mailTransferPoint:
			if !ok {
				mailTransferPoint = nil
				continue
			}
			allMails = append(allMails, mail)
		}
	}*/

	fmt.Println("Total coal:", coal.Load())
	fmt.Println("Total mails:", len(allMails))

	//fmt.Println(time.Since(initTime))
}
