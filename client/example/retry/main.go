package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/xiaojiaoyu100/taskcenter/client"
	"github.com/xiaojiaoyu100/taskcenter/mq"
)

// 发送消息
// curl -X POST -H "Content-Type: application/json"
// -D '{"name": "tag1", "type": 1, "data": "\"{\"id\": 1}\""}'
// http://taskcent.com/v1/api/task

func main() {
	customFormatter := new(logrus.TextFormatter)
	customFormatter.FullTimestamp = true                        // 显示完整时间
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.000" // 时间格式

	logrus.SetFormatter(customFormatter)
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.DebugLevel)

	conf := &mq.ConsumerConfig{
		Instancename: "MQ_INST_1314867367055240_BbJBcaGg",
		NameServers:  []string{"http://MQ_INST_1314867367055240_BbJBcaGg.mq-internet-access.mq-internet.aliyuncs.com:80"},
		GroupId:      "GID_task_c",
		GroupName:    "",
		Topic:        "taskcenter_message_1",
		AccessKey:    "",
		SecretKey:    "",
	}

	boot := make(chan bool)
	c, err := client.New(conf)
	if err != nil {
		logrus.Fatalf("create client error = %v", err)
	}
	defer c.Release()

	if err := c.Register("tag1", handleTask); err != nil {
		logrus.Fatalf("add task handler error = %v", err)
	}

	err = c.Start()
	if err != nil {
		logrus.Errorf("start consumer error = %v", err)
	}

	<-boot

}

// 如果正常消费返回nil，错误的话返回error 会促发重试, 默认的重试次数是16次
func handleTask(taskid string, data string, msgid string) error {
	fmt.Printf("%v: handleTask taskid=%s, data=%s, msgid=%s\n", time.Now(), taskid, data, msgid)
	return errors.New("retry error")
}
