package miner

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Функция miner при завершении контекста выполняет последнюю операцию, останавливается не сразу
/*func miner(ctx context.Context, wg *sync.WaitGroup, transferPoint chan<- int, n int, power int) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Я шахтер номер:", n, "мой рабочий день закончен!")
			return
		default:
			fmt.Println("Я шахтер, мой номер:", n, "начал добывать уголь!")
			time.Sleep(1 * time.Second)
			fmt.Println("Я шахтер номер:", n, "дыбыл уголь!", power)

			transferPoint <- power

			fmt.Println("Я шахтер номер:", n, "передал уголь!", power)
		}
	}
}*/

// Функция miner при завершении контекста останавливается сразу
func miner(ctx context.Context, wg *sync.WaitGroup, transferPoint chan<- int, n int, power int) {
	defer wg.Done()
	for {
		fmt.Println("Я шахтер, мой номер:", n, "начал добывать уголь!")

		select {
		case <-ctx.Done():
			fmt.Println("Я шахтер номер:", n, "мой рабочий день закончен!")
			return
		case <-time.After(1 * time.Second):
			fmt.Println("Я шахтер номер:", n, "дыбыл уголь!", power)
		}

		select {
		case <-ctx.Done():
			fmt.Println("Я шахтер номер:", n, "мой рабочий день закончен!")
			return
		case transferPoint <- power:
			fmt.Println("Я шахтер номер:", n, "передал уголь!", power)
		}
	}
}

func MinerPool(ctx context.Context, countMiners int) <-chan int {
	coalTransferPoint := make(chan int)
	wg := &sync.WaitGroup{}

	for i := 1; i <= countMiners; i++ {
		wg.Add(1)
		go miner(ctx, wg, coalTransferPoint, i, i*10)
	}

	go func() {
		wg.Wait()
		close(coalTransferPoint)
	}()

	return coalTransferPoint
}
