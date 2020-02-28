package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	"github.com/xiaojiaoyu100/taskcenter/cache"
	"github.com/xiaojiaoyu100/taskcenter/common"
	"github.com/xiaojiaoyu100/taskcenter/crontab"
	"github.com/xiaojiaoyu100/taskcenter/logic"
	"github.com/xiaojiaoyu100/taskcenter/models"
	"github.com/xiaojiaoyu100/taskcenter/models/taskinfo"
	"github.com/xiaojiaoyu100/taskcenter/models/taskrunning"
	"github.com/xiaojiaoyu100/taskcenter/schema"
)

type AddTaskResponse struct {
	TaskId string `json:"taskid"`
	Name   string `json:"name"`
}

type DelTaskResponse struct {
	SucIds []string `json:"SucIds"`
}

const (
	INVALID_PARAM = "invalid param"
)

/*
## 新增任务

    POST /api/v1/add-task

Content-Type: json

* `name` (string) - 任务名, 这里对应的rocketMQ
* `type` (int, required) - 任务类型, 1 - 单次任务， 2 - 定时任务, 3 - crontab 任务
* `data` (string, required) - 任务内容, json 字符串
* `crontab` (string, option) - crontab任务配置
* `loop_time` (int, option) - 循化时间间隔, 单位秒
* `loop_count` (int, option) - 循化次数
* `start_time` (int, option) - 任务开始时间戳，单位秒
* `end_time` (int, option) - 任务结束时间， 单位秒
* `max_runtime` (int, option) - 业务预计任务最长执行时间，单位秒， 超过这个时间没有ack会重新发一个任务
* `ack` (int, option) - 是否需要确认机制, 0 - 否, 1 - 是， 默认是否

Return
* `err` (int) - 错误状态码
* `msg` (string) - 错误信息
* `name` (string) - 任务名称
* `task_id` (string) - 任务id

*/
func AddTask(c *gin.Context) {
	var args schema.AddTaskArgs
	if err := json.NewDecoder(c.Request.Body).Decode(&args); err != nil {
		c.JSON(http.StatusBadRequest, INVALID_PARAM)
		return
	}

	if !common.In(args.Type, models.TASK_ONCE, models.TASK_CRON, models.TASK_LOOP) {
		c.AbortWithStatusJSON(http.StatusBadRequest, ErrorCode{Err: 400, Msg: "params type error"})
		return
	}
	if args.Type == models.TASK_CRON {
		ok := crontab.CheckCronString(args.Cron)
		if !ok {
			c.AbortWithStatusJSON(http.StatusBadRequest, ErrorCode{Err: 400, Msg: "params cron invalid"})
			return
		}
	}
	if args.Type == models.TASK_LOOP {
		if args.LoopTime <= 0 {
			c.AbortWithStatusJSON(http.StatusBadRequest, ErrorCode{Err: 400, Msg: "params loop_time invalid"})
			return
		}
	}

	endTime := int64(args.EndTime)
	if endTime > 0 && endTime < time.Now().Unix() {
		c.AbortWithStatusJSON(http.StatusBadRequest, ErrorCode{Err: 400, Msg: "end_time should large than current timestamp"})
		return
	}

	taskId, err := logic.AddTask(&args)

	logrus.Infof("add task result req=%s, task_id=%s, err=%s", c.Request.Body, taskId, err)
	if err != nil {
		c.JSON(http.StatusInternalServerError, ErrorCode{Err: 500, Msg: err.Error()})
		return
	}

	response := AddTaskResponse{
		TaskId: taskId,
		Name:   args.Name,
	}
	c.JSON(http.StatusOK, response)
}

func DelTaskByName(c *gin.Context) {
	var requestBody struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(c.Request.Body).Decode(&requestBody); err != nil {
		c.JSON(http.StatusBadRequest, ErrorCode{Err: 400, Msg: "request body format error"})
		return
	}

	log.Infof("DelTaskByName req=%s", requestBody.Name)

	if requestBody.Name == "" {
		c.JSON(http.StatusBadRequest, ErrorCode{Err: 400, Msg: "name should not be empty"})
		return
	}

	session, db := models.GetDB()
	defer session.Close()

	tasklist, err := taskinfo.QueryTaskByName(db, requestBody.Name)
	if err != nil {
		log.Warnf("taskinfo.QueryTaskByName error name=%s, err=%s", requestBody.Name, err.Error())
		c.JSON(http.StatusInternalServerError, ErrorCode{Err: 500, Msg: err.Error()})
	}

	delIds := []string{}
	for _, task := range tasklist {
		id := task.Id.Hex()
		err := taskinfo.DelTask(db, id)
		if err != nil {
			log.Warnf("DelTaskByName-taskinfo.DelTask error id=%s, err=%s", id, err.Error())
			continue
		}
		err = taskrunning.DelByTaskId(db, id)
		if err != nil {
			log.Warnf("DelTaskByName-taskrunning.DelByTaskId error id=%s, err=%s", id, err.Error())
			continue
		}
		delIds = append(delIds, id)
	}

	log.Infof("DelTaskByName req=%s, delIds=%v", requestBody.Name, delIds)

	var resp DelTaskResponse
	resp.SucIds = delIds
	c.JSON(http.StatusOK, resp)
}

func DelTask(c *gin.Context) {
	var requestBody struct {
		Ids []string `json:"ids"`
	}
	if err := json.NewDecoder(c.Request.Body).Decode(&requestBody); err != nil {
		c.JSON(http.StatusBadRequest, ErrorCode{Err: 400, Msg: "request body format error"})
		return
	}
	if len(requestBody.Ids) == 0 {
		c.JSON(http.StatusBadRequest, ErrorCode{Err: 400, Msg: "id should not by empty"})
		return
	}

	log.Infof("DelTask req=%s", requestBody.Ids)

	session, db := models.GetDB()
	defer session.Close()

	sucIds := []string{}
	for _, id := range requestBody.Ids {
		err := taskinfo.DelTask(db, id)
		if err != nil {
			log.Warnf("taskinfo.DelTask error id=%s, err=%s", id, err.Error())
			continue
		}
		err = taskrunning.DelByTaskId(db, id)
		if err != nil {
			log.Warnf("taskrunning.DelByTaskId error id=%s, err=%s", id, err.Error())
			continue
		}
		sucIds = append(sucIds, id)
	}

	log.Infof("DelTask req=%s, succ=%v", requestBody.Ids, sucIds)

	var resp DelTaskResponse
	resp.SucIds = sucIds
	c.JSON(http.StatusOK, resp)
}

func GetTaskById(c *gin.Context) {
	taskid := c.DefaultQuery("taskid", "")
	if taskid == "" {
		c.JSON(http.StatusBadRequest, ErrorCode{Err: 400, Msg: "taskid is empty"})
		return
	}

	session, db := models.GetDB()
	defer session.Close()

	taskinfo, err := taskinfo.QueryTaskById(db, taskid)
	if err != nil {
		log.Warnf("GetTaskById error taskid=%s, err=%s", taskid, err.Error())
		c.JSON(http.StatusInternalServerError, ErrorCode{Err: 500, Msg: err.Error()})
	}
	c.JSON(http.StatusOK, taskinfo)
}

func GetTaskByName(c *gin.Context) {
	name := c.DefaultQuery("name", "")
	if name == "" {
		c.JSON(http.StatusBadRequest, ErrorCode{Err: 400, Msg: "name should not be empty"})
		return
	}

	session, db := models.GetDB()
	defer session.Close()

	tasklist, err := taskinfo.QueryTaskByName(db, name)
	if err != nil {
		log.Warnf("GetTaskByName error name=%s, err=%s", name, err.Error())
		c.JSON(http.StatusInternalServerError, ErrorCode{Err: 500, Msg: err.Error()})
	}
	c.JSON(http.StatusOK, tasklist)
}

func ShowTaskStat(c *gin.Context) {
	data, err := cache.DefaultCache.GetTaskCount()
	if err != nil {
		c.JSON(http.StatusInternalServerError, err.Error())
		return
	}
	c.JSON(http.StatusOK, data)
}

func SetHandler(g *gin.Engine) {
	v1 := g.Group("/api")
	v1.GET("/v1/query-task-by-id", GetTaskById)
	v1.GET("/v1/query-task-by-name", GetTaskByName)
	v1.POST("/v1/delete-task", DelTask)
	v1.POST("/v1/delete-task-by-name", DelTaskByName)
	v1.POST("/v1/task", AddTask)
	v1.GET("/v1/task/stat", ShowTaskStat)
}
