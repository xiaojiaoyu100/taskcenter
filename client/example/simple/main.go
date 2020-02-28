package main

import (
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

	boot := make(chan bool)

	// 调用Init 初始化的是全局的client
	err := client.Init(map[string]string{
		"groupid":       "GID_task_c",
		"instance_name": "MQ_INST_1314867367055240_BbJBcaGg",
		"namesvr":       "http://MQ_INST_1314867367055240_BbJBcaGg.mq-internet-access.mq-internet.aliyuncs.com:80",
		"access_key":    "",
		"secret_key":    "",
		"topic":         "taskcenter_message_1",
		"servicename":   "servicename",
	})
	if err != nil {
		logrus.Errorf("get consumer config error = %v", err)
	}
	defer mq.Release()

	// 新增任务处理函数需要在消费者启动之前
	if err = client.AddTaskHandler("tag1", handleTask); err != nil {
		logrus.Errorf("add handler error = %v", err)
	}

	if err = mq.Start(); err != nil {
		logrus.Errorf("start consumer error = %v", err)
	}
	<-boot

}

// 如果正常消费返回nil，错误的话返回error 会促发重试
func handleTask(taskid string, data string, msgid string) error {
	fmt.Printf("%v: handleTask taskid=%s, data=%s, msgid=%s\n", time.Now(), taskid, data, msgid)
	return nil
}
