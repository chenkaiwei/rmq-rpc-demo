package fib

import (
	"context"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

var conn *amqp.Connection
var ch *amqp.Channel
var replyQueue amqp.Queue

// 暂存res chan的检索表，供分发结果的协程按照corrId检索。chan传递的值为远程服务返回的计算结果。
//var chanMap map[string]chan int//（废弃）须用线程安全集合
var chanMap sync.Map

var curCtx context.Context
var clientCancel context.CancelFunc

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func closeAllChanInChanMap() {
	log.Println("closeAllChanInChanMap()")
	chanMap.Range(
		func(k, v any) bool {
			c := v.(chan int)
			close(c) //仅关闭chan，从chanMap中删除的工作由rpc方法的defer阶段负责
			return true
		},
	)
}

func ClientStart(ctx context.Context, mq_address string) (err error) {
	if conn != nil && !conn.IsClosed() { //conn有值且未关闭
		err = errors.New("客户端已连接，请勿重复初始化")
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	curCtx, clientCancel = context.WithCancel(ctx) //包一层cancel，供ClientStop调用，关闭dispatch result 的循环

	rand.Seed(time.Now().UTC().UnixNano())

	//==init==
	conn, err = amqp.Dial(mq_address)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err = conn.Channel()
	failOnError(err, "Failed to open a channel")
	replyQueue, err = ch.QueueDeclare(
		"",    // name
		false, // durable
		true,  // delete when unused-- consume回收后(当前客户端关闭后)自动删除queue
		true,  // exclusive
		false, // noWait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	var msgs <-chan amqp.Delivery
	msgs, err = ch.Consume(
		replyQueue.Name, // queue
		"",              // consumer
		true,            // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	failOnError(err, "Failed to register a consumer")
	//==init end==

	// ⬇️dispatch result 分发计算结果
	//本方法的返回用于表明初始化完成，所以分发返回结果的循环另开协程
	go func() {
		log.Println("dispatch result协程启动")
		defer log.Println("dispatch result协程关闭")
		//统一监听返回消息，并分发到各个调用的resChan中
		for { //兼顾ctx结束指令
			select {
			case d, ok1 := <-msgs:
				{
					if !ok1 { //不ok代表收到的是关闭信息
						log.Println("dispatch result：监听返回结果的消费者关闭")
						ClientStop()
						return
					}
					c, ok2 := chanMap.Load(d.CorrelationId)
					if !ok2 {
						log.Println("result分发通道丢失--", d.CorrelationId)
						continue
					}
					fibResChan := c.(chan int) // 类型断言
					res, err := strconv.Atoi(string(d.Body))
					failOnError(err, "Failed to convert body to integer")
					fibResChan <- res //另一端是rpc方法
					//关闭通道+从map中删除的工作由同步方法的defer阶段负责
				}
			case <-curCtx.Done():
				{
					log.Println("dispatch result 收到来自context的取消指令")
					defer ClientStop() //先返回再关闭，则ClientStop中的cancel方法不会再发到这里来。因为整个协程已经return了
					return
				}
			}
		}
	}()

	return
}

func ClientStop() (err error) {
	if ch == nil || ch.IsClosed() || conn == nil || conn.IsClosed() { //幂等，保证循环或重复调用时只生效一次
		return
	}
	log.Println("ClientStop()")

	closeAllChanInChanMap() //中断所有阻塞中的rpc方法

	defer clientCancel() //上面这个return不会触发这个defer；这个发到dispatch res后再回来也会被上面的判断拦住

	err = ch.Close()
	ch = nil
	err = conn.Close()
	conn = nil

	return
}

//供其他逻辑调用的RPC方法
func FibonacciRPC(n int) (res int, err error) {
	if ch == nil && conn == nil {
		log.Println("客户端尚未连接，请先执行ClientStart")
		return
	}

	//=====
	corrId := randomString(32)

	//发送前就要准备好resChan
	resChan := make(chan int, 1)   //用1缓冲区避免阻塞 分发结果的线程。
	chanMap.Store(corrId, resChan) //FibonacciRPC会被并发调用，所以此处应当使用线程安全的map
	defer chanMap.Delete(corrId)   //返回后销毁对应chan

	//ctx, cancel := context.WithCancel(curCtx)
	//defer cancel()
	ctx := curCtx

	err = ch.PublishWithContext(ctx,
		"",              // exchange
		"fib_rpc_queue", // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       replyQueue.Name,
			Body:          []byte(strconv.Itoa(n)),
		})
	failOnError(err, "Failed to publish a message")

	//阻塞并等待接收chan中传来的结果
	r, ok := <-resChan
	if ok {
		res = r
		close(resChan) //正常返回时，关闭工作由同步方法（本方法）负责
	} else {
		//ok==false表明收到的是由close引起的信息，chan无需由同步方法负责关闭
		err = errors.New("【FibonacciRPC()方法】无法获取返回结果，请确保fib客户端正常运行｜corrId-" + corrId + "|func:fib-" + strconv.Itoa(n))
	}

	return
}
