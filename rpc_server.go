package main

import (
	"context"
	"github.com/pkg/errors"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"strconv"
	"time"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

func fib(n int) int {
	if n == 0 {
		return 0
	} else if n == 1 {
		return 1
	} else {
		return fib(n-1) + fib(n-2)
	}
}

func isQueueExist(conn *amqp.Connection, queueName string) bool {
	//⬇️用QueueDeclarePassive检测已关闭的队列时，会导致chan关闭，所以必须另开一个专门用于检测的chan
	//另有api接口检测队列存在，http://kaiwei.me:15672/api/index.html，二者择一即可
	ch4check, _ := conn.Channel() //测试队列存在的chan，必须在判断时实时开启chan（），才能准确使用QueueDeclarePassive判断队列 在当前时刻 是否存在
	_, err := ch4check.QueueDeclarePassive(
		queueName, // name
		false,     // durable
		true,      // delete when unused-- consume回收后(当前客户端关闭后)自动删除queue
		true,      // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		var amqpErr *amqp.Error
		if errors.As(err, &amqpErr) &&
			amqpErr.Code == amqp.NotFound {
			return false
		}
	}
	return true
}

func main() {
	maxWorkerNum := 2

	conn, err := amqp.Dial(REMOTE_RMQ_ADDRESS)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare( //接收调用请求用的有名｜固定名称队列，注意区别于发送端创建的，接收返回信息的临时队列。
		"fib_rpc_queue", // name
		false,           // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	failOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		maxWorkerNum, // prefetch count 限制每个fib服务（消费者）的最大并发量：当某消费者有 maxWorkerNum 个消息未ack｜nack时，则不分配到该线程。
		0,            // prefetch size
		false,        // global
	)
	failOnError(err, "Failed to set QoS")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Printf(" [*] Awaiting RPC requests")
	//用mq监听 阻塞主线程
	for d := range msgs { //到达prefetch count上限后会先阻塞，直到有协程发了ack｜nack才回继续收到服务请求
		n, _ := strconv.Atoi(string(d.Body))
		log.Printf("收到服务请求--%d｜replyTo--%s\n", n, d.ReplyTo)

		//判断一下queue是否存在，如果客户端已经关闭就不用计算。适用于任务开销较大，浪费不起的情形。
		if !isQueueExist(conn, d.ReplyTo) {
			log.Printf("回应队列[%s] 已关闭，丢弃任务并处理下一条消息", d.ReplyTo)
			d.Nack(false, false) //拒收后进入下次循环
			continue
		}

		//worker⬇️
		go func(d amqp.Delivery) {
			n, err := strconv.Atoi(string(d.Body))
			failOnError(err, "Failed to convert body to integer")

			log.Printf(" [开始计算] fib(%d)", n)
			response := fib(n)
			time.Sleep(time.Duration(len(strconv.Itoa(response))) * time.Second) //模拟服务延迟，结果有几位数就延迟几秒

			err = ch.PublishWithContext(ctx, //发送计算服务的结果：把计算结果发送到，从请求信息带来的，临时随机名的，"返回用队列"里去。
				"",        // exchange
				d.ReplyTo, // routing key
				// 路由键（队列名）使用请求消息中携带的ReplyTo队列名。
				//一样用默认交换机的规则，路由键当队列名用。发送到"由发送端发送时携带的、用以接收返回信息的、客户端生成的临时队列"里去

				false, // mandatory
				false, // immediate
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(strconv.Itoa(response)),
				})
			failOnError(err, "Failed to publish a message")
			log.Printf("发送计算结果：fib(%d) = %d 到队列-%s\n", n, response, d.ReplyTo)

			d.Ack(false) //返回确认信息，由此腾出了空位，消息队列会把下一条符合规则的消息发过来供当前协程处理。
		}(d)
	}

}
