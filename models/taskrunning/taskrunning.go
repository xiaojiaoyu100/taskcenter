package taskrunning

import (
	log "github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	COLL_TASKRUNNING = "task_running"
)

const (
	HANDLE_NUM = 100
)

const (
	Id              = "_id"
	TaskId          = "task_id"
	StartTime       = "start_time"
	NextOpTime      = "next_op_time"
	LoopTime        = "loop_time"
	Runtime         = "runtime"
	Name            = "name"
	Data            = "data"
	Cron            = "crontab"
	Type            = "type"
	Ack             = "ack"
	MaxExeCount     = "max_exe_count"
	CurrentExeCount = "current_exe_count"
	TaskStartTime   = "task_start_time"
	TaskEndTime     = "task_end_time"
)

type RunningTask struct {
	Id              bson.ObjectId `bson:"_id"`
	TaskId          string        `bson:"task_id"`
	StartTime       int64         `bson:"start_time"`   // 本次开始执行的时间
	NextOpTime      int64         `bson:"next_op_time"` // 下次执行时间
	LoopTime        int64         `bson:"loop_time"`    // 循环时间
	Runtime         int           `bson:"runtime"`      // 预计运行时间，超过这个时间没有ack会重发
	Name            string        `bson:"name"`
	Data            string        `bson:"data"`
	Cron            string        `bson:"crontab"`
	Type            int           `bson:"type"`
	Ack             bool          `bson:"ack"`
	MaxExeCount     int           `bson:"max_exe_count"`     // 任务最大执行次数
	CurrentExeCount int64         `bson:"current_exe_count"` // 当前执行的次数
	TaskStartTime   int64         `bson:"task_start_time"`   // 任务开始执行时间，若存在则任务在此之前不执行
	TaskEndTime     int64         `bson:"task_end_time"`     // 任务开始结束时间，若存在，则之后任务不再执行
}

/*
获取
*/
func GetOpTask(db *mgo.Database, now int64) (*[]RunningTask, error) {
	var tasks []RunningTask
	query := bson.M{NextOpTime: bson.M{"$lte": now}}
	err := db.C(COLL_TASKRUNNING).Find(query).Sort(NextOpTime).Limit(HANDLE_NUM).All(&tasks)

	if err != nil {
		log.WithFields(log.Fields{
			"query": query,
			"err":   err.Error(),
		}).Error("GetOpTask error")
		return nil, err
	}
	return &tasks, nil
}

func UpdateOpData(db *mgo.Database, taskid string, lasttime int64) error {
	query := bson.M{TaskId: taskid}
	updata := bson.M{
		"$set": bson.M{NextOpTime: lasttime},
		"$inc": bson.M{CurrentExeCount: 1},
	}
	return db.C(COLL_TASKRUNNING).Update(query, updata)
}

func AddTask(db *mgo.Database, task RunningTask) error {
	return db.C(COLL_TASKRUNNING).Insert(&task)
}

func DelByTaskId(db *mgo.Database, taskid string) error {
	query := bson.M{TaskId: taskid}
	return db.C(COLL_TASKRUNNING).Remove(query)
}

func DelTask(db *mgo.Database, id string) error {
	_id := bson.ObjectIdHex(id)
	query := bson.M{Id: _id}
	return db.C(COLL_TASKRUNNING).Remove(query)
}

func BulkDeleteTaskById(db *mgo.Database, taskIds []string) error {
	query := bson.M{
		TaskId: bson.M{
			"$in": taskIds,
		},
	}
	return db.C(COLL_TASKRUNNING).Remove(query)
}

func Index(db *mgo.Database) (*mgo.Collection, error) {
	coll := db.C(COLL_TASKRUNNING)
	err := coll.EnsureIndex(mgo.Index{
		Key:        []string{TaskId},
		Unique:     true,
		Background: true,
	})
	err = coll.EnsureIndex(mgo.Index{
		Key:        []string{NextOpTime},
		Background: true,
	})
	return coll, err
}
