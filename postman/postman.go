package postman

import (
	"context"
	"fmt"
	"sync"
	"time"
)

func postman(wg *sync.WaitGroup, ctx context.Context, transferPoint chan<- string, n int, mail string) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Я почтальон номер:", n, "закончил работу!")
			return
		default:
			fmt.Println("Я почтальон номер:", n, "взял письмо", mail)
			time.Sleep(1 * time.Second)
			fmt.Println("Я почтальон номер:", n, "донес письмо до почты", mail)
			transferPoint <- mail
			fmt.Println("Я почтальон номер:", n, "передал письмо", mail)
		}

	}
}

func PostmanPool(ctx context.Context, countPostman int) <-chan string {
	mailTransferPoint := make(chan string)
	wg := &sync.WaitGroup{}

	for i := 1; i <= countPostman; i++ {
		wg.Add(1)
		go postman(wg, ctx, mailTransferPoint, i, mail(i))
	}

	go func() {
		wg.Wait()
		close(mailTransferPoint)
	}()

	return mailTransferPoint
}

func mail(n int) string {
	ptm := map[int]string{
		1: "Гонки",
		2: "Новости",
		3: "Гороскопы",
	}
	mail, ok := ptm[n]

	if !ok {
		return "Лотерея"
	}
	return mail
}
