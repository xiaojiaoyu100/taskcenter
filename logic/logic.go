package logic

import (
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2/bson"

	"github.com/xiaojiaoyu100/taskcenter/app"
	"github.com/xiaojiaoyu100/taskcenter/cache"
	"github.com/xiaojiaoyu100/taskcenter/crontab"
	"github.com/xiaojiaoyu100/taskcenter/models"
	"github.com/xiaojiaoyu100/taskcenter/models/mqlog"
	"github.com/xiaojiaoyu100/taskcenter/models/taskinfo"
	"github.com/xiaojiaoyu100/taskcenter/models/tasklog"
	"github.com/xiaojiaoyu100/taskcenter/models/taskrunning"
	"github.com/xiaojiaoyu100/taskcenter/mq"
	"github.com/xiaojiaoyu100/taskcenter/schema"
)

func PushDataToMq(name string, taskid string, data string, exeTime int64) error {
	var session, db = models.GetDB()
	defer session.Close()

	producer := mq.GetProducer()

	// 先写log看下是否有重复投递的再发送到mq
	logId := bson.NewObjectId()
	tlog := tasklog.TaskLog{
		Id:      logId,
		Name:    name,
		TaskId:  taskid,
		ExeTime: exeTime,
	}

	err := tasklog.AddTaskLog(db, tlog)
	if err != nil {
		log.Warnf("add task log err task=%v, err=%v", tlog, err)
		return err
	}

	mqid, err := producer.PublishMessage(name, taskid, data)
	log.Infof("PUSH MESSAGE: %v, %v, %v, err=%v\n", name, taskid, data, err)
	if err != nil {
		return err
	}

	_log := mqlog.MqLog{
		Name:      name,
		TaskId:    taskid,
		MqId:      mqid,
		ExeTime:   exeTime,
		TaskLogId: logId.Hex(),
		LogTime:   time.Now(),
	}
	err = mqlog.AddMqLog(db, _log)
	if err != nil {
		log.Warnf("mqlog.AddMqLog error log=%v, err=%v", _log, err.Error())
	}

	return nil
}

func CheckRunningTaskStatus(name string) {
	nameCount, totalCount, err := cache.DefaultCache.IncreaseTaskCount(name)
	if err != nil {
		log.WithFields(log.Fields{
			"name": name,
		}).Errorf("increase running task count error")
	}
	nameAlertCountPerMin := int64(app.DefaultTopicConfig.NameAlertCountPerMin)
	totalAlertCountPerMin := int64(app.DefaultTopicConfig.TotalAlertCountPerMin)
	if nameAlertCountPerMin > 0 && nameCount > nameAlertCountPerMin {
		log.WithFields(log.Fields{
			"name":         name,
			"currentCount": nameCount,
			"maxCount":     app.DefaultTopicConfig.NameAlertCountPerMin,
		}).Warningf("#NAME_TO_MANY_TASK")
	}
	if totalAlertCountPerMin > 0 && totalCount > totalAlertCountPerMin {
		log.WithFields(log.Fields{
			"currentTotalCount": totalCount,
			"maxTotalCount":     app.DefaultTopicConfig.TotalAlertCountPerMin,
		}).Warning("#TOTAL_TO_MANY_TASK")
	}
}

/*
新增任务
*/
func AddTask(task *schema.AddTaskArgs) (string, error) {
	session, db := models.GetDB()
	defer session.Close()

	id := bson.NewObjectId()
	now := time.Now().Unix()
	newTask := taskinfo.TaskInfo{
		Id:         id,
		Name:       task.Name,
		Type:       task.Type,
		Data:       task.Data,
		Cron:       task.Cron,
		LoopTime:   task.LoopCount,
		LoopCount:  task.LoopTime,
		StartTime:  task.StartTime,
		EndTime:    task.EndTime,
		MaxRuntime: task.MaxRuntime,
		Ack:        task.Ack,
		CreatedAt:  time.Now(),
	}

	retry, err := taskinfo.AddTask(db, newTask)
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	log.Infof("AddTask retry=%v, err=%v", retry, errMsg)

	if err != nil {
		return "", err
	}

	// 消息重复，直接返回空
	if retry == true {
		existedTask, err := taskinfo.FindTaskByNameAndData(db, task.Name, task.Data)
		if err != nil {
			return "", err
		}
		return existedTask.Id.Hex(), nil
	}

	t := task.Type
	name := task.Name
	data := task.Data

	taskId := id.Hex()
	if t == models.TASK_ONCE {
		err := PushDataToMq(name, taskId, task.Data, now)
		if err != nil {
			return "", err
		}
		// name 数量报警
		CheckRunningTaskStatus(name)
	} else if t == models.TASK_LOOP {
		err := PushDataToMq(name, taskId, data, now)
		if err != nil {
			return "", err
		}
		running := taskrunning.RunningTask{
			Id:              bson.NewObjectId(),
			TaskId:          taskId,
			StartTime:       now,
			NextOpTime:      now + int64(task.LoopTime),
			LoopTime:        int64(task.LoopTime),
			Name:            name,
			Data:            data,
			Type:            t,
			Ack:             task.Ack,
			MaxExeCount:     task.LoopCount,
			CurrentExeCount: 0,
			TaskStartTime:   task.StartTime,
			TaskEndTime:     task.EndTime,
		}
		err = taskrunning.AddTask(db, running)
		if err != nil {
			log.Errorf("LoopTask task=%s, error: %v\n", newTask, err)
			return "", err
		}
	} else if t == models.TASK_CRON {
		now := time.Now().Unix()

		nextExeTime, err := crontab.ParseAndGetNextTime(task.Cron)
		if err != nil {
			return "", err
		}

		running := taskrunning.RunningTask{
			Id:              bson.NewObjectId(),
			TaskId:          taskId,
			StartTime:       now,
			NextOpTime:      nextExeTime.Unix(),
			Name:            name,
			Data:            data,
			Cron:            task.Cron,
			Type:            t,
			Ack:             task.Ack,
			MaxExeCount:     task.LoopCount,
			CurrentExeCount: 0,
			TaskStartTime:   task.StartTime,
			TaskEndTime:     task.EndTime,
		}
		err = taskrunning.AddTask(db, running)
		if err != nil {
			return "", err
		}
	}

	return taskId, err
}

func OnTimer(t time.Time) bool {
	if cache.DefaultCache.GetLock(t.Unix()) == false {
		return true
	}

	session, db := models.GetDB()
	defer session.Close()

	for {
		taskList, err := taskrunning.GetOpTask(db, t.Unix())
		if err != nil {
			log.WithFields(log.Fields{
				"current_time": t.Unix(),
				"err":          err,
			}).Error("GetOpTask error")
			continue
		}

		// 已经处理完毕
		if len(*taskList) <= 0 {
			break
		}

		taskIdToRemove := make([]string, 0)
		for _, task := range *taskList {
			// 还每到执行时间
			if task.StartTime != 0 && task.StartTime > time.Now().Unix() {
				continue
			}
			// 到了任务时间
			if task.TaskEndTime != 0 && task.TaskEndTime < time.Now().Unix() {
				taskIdToRemove = append(taskIdToRemove, task.TaskId)
				log.WithFields(log.Fields{
					"task_id":      task.TaskId,
					"end_time":     task.TaskEndTime,
					"current_time": time.Now().Unix(),
				}).Info("remove end_time expire task")
				continue
			}
			// 有执行次数限制
			if task.MaxExeCount != 0 && task.CurrentExeCount >= int64(task.MaxExeCount) {
				taskIdToRemove = append(taskIdToRemove, task.TaskId)
				log.WithFields(log.Fields{
					"task_id":          task.TaskId,
					"current_exe_time": task.CurrentExeCount,
					"max_exe_time":     task.MaxExeCount,
				}).Info("remove limit loop_count task")
				continue
			}

			err := PushDataToMq(task.Name, task.TaskId, task.Data, task.NextOpTime)
			if err != nil {
				continue
			}
			// 成功推送过去的统计
			CheckRunningTaskStatus(task.Name)
			// 这里并不能保证按照正确的时间间隔执行
			if task.Type == models.TASK_LOOP {
				lastOpTime := task.LoopTime + time.Now().Unix()
				err := taskrunning.UpdateOpData(db, task.TaskId, lastOpTime)
				if err != nil {
					log.Errorf("UpdateLastOpTime err task=%v, err=%v", task, err)
				}
			} else if task.Type == models.TASK_CRON {
				nextExeTime, err := crontab.ParseAndGetNextTime(task.Cron)
				if err != nil {
					log.Errorf("parse cron err, task=%v, err=%v", task, err)
				}
				lastOpTime := nextExeTime.Unix()
				err = taskrunning.UpdateOpData(db, task.TaskId, lastOpTime)
				if err != nil {
					log.Errorf("UpdateLastOpTime err task=%v, err=%v", task, err)
				}
			}
		}

		// 删除掉已经过期的任务
		if len(taskIdToRemove) > 0 {
			err = taskrunning.BulkDeleteTaskById(db, taskIdToRemove)
			if err != nil {
				log.WithFields(log.Fields{
					"err":      err,
					"task_ids": taskIdToRemove,
				}).Error("remove expire running task error")
			} else {
				log.WithFields(log.Fields{
					"task_ids": taskIdToRemove,
				}).Info("remove expire running task success")
			}
		}
	}

	return true
}
