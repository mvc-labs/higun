package mempool

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/go-zeromq/zmq4"
	"github.com/metaid/utxo_indexer/storage"
)

// ZMQClient 表示一个ZeroMQ客户端，用于监听比特币节点发送的内存池事件
type ZMQClient struct {
	// ZMQ连接地址，如 "tcp://127.0.0.1:28332"
	address string

	// 要监听的主题列表，如 "rawtx", "hashtx" 等
	topics []string

	// 上下文控制
	ctx    context.Context
	cancel context.CancelFunc

	// 等待所有协程完成
	wg sync.WaitGroup

	// 连接和重连间隔
	reconnectInterval time.Duration

	// 处理器映射，每个主题对应一个处理函数
	handlers map[string]MessageHandler
}

// MessageHandler 是处理ZMQ消息的函数类型
type MessageHandler func(topic string, data []byte) error

// NewZMQClient 创建一个新的ZMQ客户端
func NewZMQClient(address string, _ *storage.SimpleDB) *ZMQClient {
	ctx, cancel := context.WithCancel(context.Background())

	return &ZMQClient{
		address:           address,
		topics:            []string{},
		ctx:               ctx,
		cancel:            cancel,
		reconnectInterval: 5 * time.Second,
		handlers:          make(map[string]MessageHandler),
	}
}

// AddTopic 添加一个要监听的主题及其处理器
func (c *ZMQClient) AddTopic(topic string, handler MessageHandler) {
	// 确保主题不重复
	for _, t := range c.topics {
		if t == topic {
			return
		}
	}

	c.topics = append(c.topics, topic)
	c.handlers[topic] = handler
}

// Start 开始监听ZMQ消息
func (c *ZMQClient) Start() error {
	if len(c.topics) == 0 {
		return fmt.Errorf("没有添加任何主题，请先使用AddTopic添加要监听的主题")
	}

	log.Printf("开始连接ZMQ服务器: %s", c.address)
	log.Printf("监听主题: %s", strings.Join(c.topics, ", "))

	// 启动监听协程
	c.wg.Add(1)
	go c.listen()

	return nil
}

// Stop 停止监听
func (c *ZMQClient) Stop() {
	c.cancel()
	c.wg.Wait()
	log.Println("ZMQ客户端已停止")
}

// listen 是内部方法，用于监听ZMQ消息
func (c *ZMQClient) listen() {
	defer c.wg.Done()

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// 创建一个新的socket
			socket := zmq4.NewSub(c.ctx)
			defer socket.Close()

			// 连接到ZMQ服务器
			if err := socket.Dial(c.address); err != nil {
				log.Printf("连接ZMQ服务器失败: %v, 将在%v后重试",
					err, c.reconnectInterval)
				time.Sleep(c.reconnectInterval)
				continue
			}

			// 订阅所有主题
			for _, topic := range c.topics {
				if err := socket.SetOption(zmq4.OptionSubscribe, topic); err != nil {
					log.Printf("订阅主题 %s 失败: %v", topic, err)
					continue
				}
			}

			log.Printf("成功连接到ZMQ服务器: %s", c.address)

			// 接收消息循环
			c.receiveMessages(socket)

			// 如果receiveMessages返回，说明连接断开或出错，重新连接
			log.Printf("ZMQ连接断开，将在%v后重新连接", c.reconnectInterval)
			time.Sleep(c.reconnectInterval)
		}
	}
}

// receiveMessages 接收ZMQ消息并处理
func (c *ZMQClient) receiveMessages(socket zmq4.Socket) {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			// 接收消息
			msg, err := socket.Recv()
			if err != nil {
				log.Printf("接收消息失败: %v", err)
				return
			}

			// 确保消息至少有两部分：主题和数据
			if len(msg.Frames) < 2 {
				log.Printf("收到格式不正确的消息: %v", msg)
				continue
			}

			// 第一帧是主题
			topic := string(msg.Frames[0])

			// 查找对应的处理器
			handler, ok := c.handlers[topic]
			if !ok {
				log.Printf("收到未知主题的消息: %s", topic)
				continue
			}

			// 调用处理器处理消息
			if err := handler(topic, msg.Frames[1]); err != nil {
				log.Printf("处理消息失败 [%s]: %v", topic, err)
			}
		}
	}
}

// min 返回两个整数中的较小值
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
