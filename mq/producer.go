package mq

import (
	"context"

	"github.com/apache/rocketmq-client-go"
	"github.com/apache/rocketmq-client-go/primitive"
	"github.com/apache/rocketmq-client-go/producer"
	log "github.com/sirupsen/logrus"
)

type Producer struct {
	MQ    rocketmq.Producer
	Topic string
}

var defaultProducer *Producer

const (
	retryOnError = 2
)

func NewProducer(conf *Config) *Producer {
	return &Producer{}
}

func InitProducer(conf *Config) *Producer {
	if defaultProducer != nil {
		return defaultProducer
	}

	credentials := primitive.Credentials{
		AccessKey: conf.AccessKey,
		SecretKey: conf.SecretKey,
	}
	p, err := rocketmq.NewProducer(
		producer.WithGroupName(conf.GroupId),
		producer.WithNamespace(conf.Instancename),
		producer.WithNameServer(conf.NameServers),
		//producer.WithRetry(retryOnError),
		producer.WithCredentials(credentials),
	)
	if err != nil {
		log.WithFields(log.Fields{
			"conf": conf,
		}).Panicf("create producer error = %v", err)
	}

	err = p.Start()
	if err != nil {
		log.Panicf("start producer error = %v", err)
	}

	defaultProducer = &Producer{
		MQ:    p,
		Topic: conf.Topic,
	}
	log.WithFields(log.Fields{
		"conf": conf,
	}).Debug("init producer success!")
	return defaultProducer
}

func GetProducer() *Producer {
	return defaultProducer
}

func CloseMQProducer() {
	err := defaultProducer.MQ.Shutdown()
	if err != nil {
		log.Errorf("shutdown RocketMQ producer error = %v", err)
	}
}

/*
发布消息
tags: topic下的标签
taskId: 任务Id
data: 消息内容，由业务方决定
options: 可选参数，比如可以选择不通过默认topic发布消息
*/
func (m *Producer) PublishMessage(tags, taskId, data string, options ...string) (string, error) {
	topic := m.Topic
	if len(options) > 0 {
		topic = options[0]
	}

	msg := primitive.NewMessage(topic, []byte(data))
	msg.WithTag(tags)
	msg.WithKeys([]string{taskId})

	result, err := m.MQ.SendSync(context.Background(), msg)
	if err != nil {
		log.WithFields(log.Fields{
			"msg":    msg,
			"result": result,
		}).Errorf("public message error = %v", err)
		return "", err
	}

	log.WithFields(log.Fields{
		"msg":    msg,
		"result": result,
	}).Info("publish message success!")

	return result.MsgID, nil
}
