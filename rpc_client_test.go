package main_test

import (
	"context"
	"log"
	"myGoDemo/rabbitmq/6_rpc"
	"myGoDemo/rabbitmq/6_rpc/fib"
	"sync"
	"testing"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func TestFibClient(t *testing.T) {
	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fib.ClientStart(ctx, main.REMOTE_RMQ_ADDRESS)
	defer fib.ClientStop()

	//掺一个大的 用来测试服务端计算量的负载均衡。服务端要开多个，分配到fib 50的服务，总处理量会显著更少
	wg.Add(1)
	go func() {
		num := 40
		defer wg.Done()
		log.Printf(" [x] Requesting fib(%d)", num)
		res, err := fib.FibonacciRPC(num)
		if err != nil {
			return
		}
		log.Printf("fib(%d) = %d", num, res)
	}()
	time.Sleep(200 * time.Millisecond)

	//模拟 并发的同步调用
	for i := 1; i <= 10; i++ {
		wg.Add(1) //应当与wait放在同一协程中来保证add先于wait执行
		go func(i int) {
			//wg.Add(1) //add和wait不在同一协程中会出现后者先执行的情况，会直接跑完
			defer wg.Done()

			n := i
			log.Printf(" [x] Requesting fib(%d)", n)
			res, err := fib.FibonacciRPC(n)
			if err != nil {
				log.Println(err.Error())
				return
			}
			log.Printf("fib(%d) = %d", n, res)
		}(i)
	}

	//测试客户端中断，与手动中断则一即可
	time.Sleep(3 * time.Second)

	//log.Println("测试 客户端中途关闭-方式1")
	//fib.ClientStop() // 关闭方式1
	//log.Println("测试 客户端中途关闭-方式2")
	//cancel() //关闭方式2

	//--1计数器模式
	wg.Wait()
	log.Println("所有请求已全部返回")

	//--2长开不断，与计数器择一使用
	//select {}

}
