package server

import (
	"context"
	ginprometheus "github.com/zsais/go-gin-prometheus"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"

	"github.com/xiaojiaoyu100/taskcenter/app"
	"github.com/xiaojiaoyu100/taskcenter/cache"
	"github.com/xiaojiaoyu100/taskcenter/httpapi"
	"github.com/xiaojiaoyu100/taskcenter/logic"
	"github.com/xiaojiaoyu100/taskcenter/models"
	"github.com/xiaojiaoyu100/taskcenter/models/mqlog"
	"github.com/xiaojiaoyu100/taskcenter/models/taskinfo"
	"github.com/xiaojiaoyu100/taskcenter/models/tasklog"
	"github.com/xiaojiaoyu100/taskcenter/models/taskrunning"
	"github.com/xiaojiaoyu100/taskcenter/mq"
)

var router *gin.Engine

type TaskServer struct {
	Running      bool
	TimerRunning bool
	RedisCfg     cache.RedisConfig
	DbCfg        models.DbConfig
	MqCfg        mq.Config
}

func CreateIndex() {
	session, db := models.GetDB()
	defer session.Close()

	_, err := mqlog.Index(db)
	if err != nil {
		log.Infof("create mqlog index error: %v\n", err)
	}
	_, err = taskinfo.Index(db)
	if err != nil {
		log.Infof("create taskinfo index error: %v\n", err)
	}
	_, err = taskrunning.Index(db)
	if err != nil {
		log.Infof("create taskruning index error: %v\n", err)
	}
	_, err = tasklog.Index(db)
	if err != nil {
		log.Infof("create tasklog index error: %v\n", err)
	}
}

func (svr *TaskServer) Run(port int) {
	models.InitDB(svr.DbCfg.Url, svr.DbCfg.DbName)
	defer models.CloseDB()

	cache.InitCache(svr.RedisCfg)
	defer cache.DefaultCache.CloseRedis()

	mq.InitProducer(&svr.MqCfg)
	defer mq.CloseMQProducer()

	var ticker *time.Ticker = time.NewTicker(1 * time.Second)

	svr.Running = true
	svr.TimerRunning = true
	go func() {
		for t := range ticker.C {
			logic.OnTimer(t)
			if svr.Running == false {
				svr.TimerRunning = false
				break
			}
		}
		log.Infof("timer stop")
		svr.TimerRunning = false
	}()

	CreateIndex()

	router = gin.New()
	p := ginprometheus.NewPrometheus("gin")
	p.Use(router)
	httpapi.SetHandler(router)

	s := &http.Server{
		Addr:         ":" + strconv.Itoa(port),
		Handler:      router,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		log.Infof("task server start port:%d", port)
		if err := s.ListenAndServe(); err != nil {
			if svr.Running == true {
				log.Panicf("task server listen err:%s, port:%d", err, port)
			}
		}
	}()

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGQUIT, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	log.Infof("task server shutdown %s", <-ch)
	svr.Running = false

	if err := s.Shutdown(context.Background()); err != nil {
		log.Errorf("task server shutdown error:%s", err)
	}
	// 等待定时器关闭
	for a := 0; a < 3; a++ {
		if svr.TimerRunning == false {
			break
		}
		time.Sleep(1 * time.Second)
	}

	log.Infof("task server stop finish")
}

// useage
// svr, err := InitServer(config)
// svr.Run(8080)
func InitServer(conf map[string]string) *TaskServer {
	gin.SetMode(gin.ReleaseMode)

	redisDB, err := strconv.Atoi(conf["redis.db"])
	if err != nil {
		redisDB = 0
	}
	nameWarningCount, err := strconv.Atoi(conf["app.name_alert_count"])
	if err != nil {
		nameWarningCount = 0
	}
	totalWarningCount, err := strconv.Atoi(conf["app.total_alert_count"])
	if err != nil {
		totalWarningCount = 0
	}

	cfg := cache.RedisConfig{
		Addr: conf["redis.addr"],
		Pwd:  conf["redis.password"],
		Db:   redisDB,
	}

	dbCfg := models.DbConfig{
		Url:    conf["mongodb.uri"],
		DbName: conf["mongodb.db"],
	}

	rocketMqCfg := mq.Config{
		Instancename: conf["rocketmq.instance_name"],
		NameServers:  strings.Split(conf["rocketmq.namesvr"], ","),
		GroupId:      conf["rocketmq.group_id"],
		GroupName:    conf["rocketmq.group_name"],
		Topic:        conf["rocketmq.topic"],
		AccessKey:    conf["rocketmq.access_key"],
		SecretKey:    conf["rocketmq.secret_key"],
	}

	app.DefaultTopicConfig = app.TopicConfig{
		NameAlertCountPerMin:  nameWarningCount,
		TotalAlertCountPerMin: totalWarningCount,
	}

	svr := TaskServer{
		RedisCfg: cfg,
		DbCfg:    dbCfg,
		MqCfg:    rocketMqCfg,
	}
	return &svr
}
