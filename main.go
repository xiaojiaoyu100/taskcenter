package main

import (
	"fmt"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	"github.com/xiaojiaoyu100/taskcenter/server"
)

func main() {
	customFormatter := new(logrus.TextFormatter)
	customFormatter.FullTimestamp = true                        // 显示完整时间
	customFormatter.TimestampFormat = "2006-01-02 15:04:05.000" // 时间格式

	logrus.SetFormatter(customFormatter)
	logrus.SetOutput(os.Stdout)
	logrus.SetLevel(logrus.DebugLevel)

	initConfig()
	conf := map[string]string{
		"app.name_alert_count":   viper.GetString("task_center.name_alert_count"),
		"app.total_alert_count":  viper.GetString("task_center.total_alert_count"),
		"mongodb.uri":            viper.GetString("mongodb.uri"),
		"mongodb.db":             viper.GetString("mongodb.db"),
		"redis.addr":             viper.GetString("redis.db"),
		"redis.password":         viper.GetString("redis.password"),
		"redis.db":               viper.GetString("redis.db"),
		"rocketmq.group_id":      viper.GetString("rocketmq.group_id"),
		"rocketmq.instance_name": viper.GetString("rocketmq.instance_name"),
		"rocketmq.namesvr":       viper.GetString("rocketmq.namesvr"),
		"rocketmq.access_key":    viper.GetString("rocketmq.access_key"),
		"rocketmq.secret_key":    viper.GetString("rocketmq.secret_key"),
		"rocketmq.topic":         viper.GetString("rocketmq.topic"),
	}
	svr := server.InitServer(conf)
	svr.Run(viper.GetInt("task_center.port"))

}

func initConfig() {
	configFile := os.Getenv("TASK_CENTER_CONFIG_PATH")
	if configFile != "" {
		viper.SetConfigFile(configFile)
	} else {
		viper.SetConfigName("task_center")
		viper.SetConfigType("toml")
		viper.AddConfigPath(".")
	}

	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("unable to read config: %v\n", err)
		os.Exit(1)
	}
}
