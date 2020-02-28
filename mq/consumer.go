package mq

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/rocketmq-client-go"
	"github.com/apache/rocketmq-client-go/consumer"
	"github.com/apache/rocketmq-client-go/primitive"
	log "github.com/sirupsen/logrus"
)

type Consumer struct {
	MQ       rocketmq.PushConsumer
	Topic    string
	Callback map[string]func(taskId, data, msgId string) error
	mutex    sync.Mutex
}

const (
	START = "*"
)

var defaultConsumer *Consumer

func NewConsumer(conf *ConsumerConfig) (*Consumer, error) {

	c, err := rocketmq.NewPushConsumer(
		consumer.WithGroupName(conf.GroupId),
		consumer.WithNamespace(conf.Instancename),
		consumer.WithNameServer(conf.NameServers),
		consumer.WithCredentials(primitive.Credentials{
			AccessKey: conf.AccessKey,
			SecretKey: conf.SecretKey,
		}),
		//consumer.WithConsumerModel(model),
	)
	if err != nil {
		log.WithFields(log.Fields{
			"conf": conf,
		}).Errorf("create rocketMQ consumer error = %v\n", err)
		return nil, fmt.Errorf("new RocektMQ consumer error = %v", err)
	}
	log.WithFields(log.Fields{
		"conf": conf,
	}).Debug("create rocketMQ consumer success")

	mc := &Consumer{
		MQ:       c,
		Topic:    conf.Topic,
		Callback: make(map[string]func(string, string, string) error),
		mutex:    sync.Mutex{},
	}
	return mc, nil
}

// 初始化默认的consumer
func InitMQConsumer(conf *ConsumerConfig) (*Consumer, error) {
	if defaultConsumer != nil {
		return defaultConsumer, nil
	}

	mc, err := NewConsumer(conf)
	if err != nil {
		log.Errorf("init consumer error = %v", err)
		return nil, err
	}
	defaultConsumer = mc
	return defaultConsumer, nil
	//var model = consumer.Clustering
	//if conf.Broadcasting == 1 {
	//	model = consumer.BroadCasting
	//}

}

func GetMQConsumer() *Consumer {
	return defaultConsumer
}

func (c *Consumer) CallbackFunc(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
	for _, msg := range msgs {
		data := string(msg.Body)
		key := msg.GetKeys()
		msgId := msg.MsgId

		if cb, ok := c.Callback[msg.GetTags()]; ok {
			err := cb(key, data, msgId)
			if err != nil {
				log.WithFields(log.Fields{
					"taskid": key,
					"data":   data,
					"msgid":  msg.MsgId,
				}).WithError(err).Warn("consumer callback error")
				return consumer.ConsumeRetryLater, err
			}
		}

		// 支持全部订阅
		if cb, ok := c.Callback[START]; ok {
			err := cb(key, data, msgId)
			if err != nil {
				log.WithFields(log.Fields{
					"taskid": key,
					"data":   data,
					"msgid":  msg.MsgId,
				}).WithError(err).Warn("consumer callback error")
				return consumer.ConsumeRetryLater, err
			}
		}
	}
	return consumer.ConsumeSuccess, nil
}

// 需要支持 类似于 consumer.subscribe("topic", "tag1 || tag2 || tag3") 这种形式
func (c *Consumer) Register(name string, callback func(taskId, data, msgId string) error) error {
	var err error

	// 需要每个标签都注册
	tags := strings.Split(name, "||")
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		c.Callback[tag] = callback
	}

	err = c.MQ.Subscribe(c.Topic, consumer.MessageSelector{
		Type:       consumer.TAG,
		Expression: name,
	}, c.CallbackFunc)
	return err
}

func (c *Consumer) Start() error {
	err := c.MQ.Start()
	if err != nil {
		log.Debug("start rocketMQ consumer error = %v\n", err)
		return fmt.Errorf("start RocketMQ consumer error=%v", err)
	}
	return nil
}

func (c *Consumer) Release() error {
	err := c.MQ.Shutdown()
	if err != nil {
		log.Panicf("shutdown rocketMQ consumer error = %v\n", err)
		return fmt.Errorf("shutdown RocketMQ consumer error=%v", err)
	}
	return nil
}

func Start() error {
	if defaultConsumer != nil {
		return defaultConsumer.Start()
	}
	return errors.New("couldn't start consumer before init it")
}

func Release() error {
	if defaultConsumer != nil {
		return defaultConsumer.Release()
	}
	return errors.New("couldn't release consumer before init it")
}
