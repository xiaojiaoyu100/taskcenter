package client

import (
	"github.com/xiaojiaoyu100/taskcenter/mq"
)

func Init(config map[string]string) error {
	cnf := &mq.ConsumerConfig{
		Instancename: config["instance_name"],
		NameServers:  []string{config["namesvr"]},
		GroupId:      config["groupid"],
		GroupName:    config["group_name"],
		Topic:        config["topic"],
		AccessKey:    config["access_key"],
		SecretKey:    config["secret_key"],
	}

	_, err := mq.InitMQConsumer(cnf)
	return err
}

func AddTaskHandler(name string, callback func(taskid string, data string, msgid string) error) error {
	consumer := mq.GetMQConsumer()
	// TODO： 各个业务需要处理自己错误逻辑,  callback 是用来实现业务逻辑的
	return consumer.Register(name, callback)
}

func Release() error {
	return mq.Release()
}

func Start() error {
	return mq.Start()
}

func New(conf *mq.ConsumerConfig) (*mq.Consumer, error) {
	return mq.NewConsumer(conf)
}
