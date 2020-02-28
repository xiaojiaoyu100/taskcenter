package taskinfo

import (
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	COLL_TASK_INFO = "task_info"
)

const (
	Id         = "_id"
	Name       = "name"
	Type       = "type"
	Data       = "data"
	Cron       = "crontab"
	LoopTime   = "loop_time"
	LoopCount  = "times"
	StartTime  = "start_time"
	EndTime    = "end_time"
	Runtime    = "runtime"
	MaxRunTime = "max_runtime"
	Ack        = "ack"
	CreateTime = "create_time"
)

type TaskInfo struct {
	Id         bson.ObjectId `bson:"_id" json:"_id"`
	Name       string        `bson:"name" json:"name"` // 这里对应的应该是topic
	Type       int           `bson:"type" json:"type"`
	Data       string        `bson:"data" json:"data"`
	Cron       string        `bson:"crontab" json:"crontab"`         // crontab
	LoopTime   int           `bson:"loop_time" json:"loop_time"`     // 循环时间
	LoopCount  int           `bson:"loot_count" json:"loop_count"`   // 循环次数
	StartTime  int64         `bson:"start_time" json:"start_time"`   // 循环开始时间
	EndTime    int64         `bson:"end_time" json:"end_time"`       // 循化结束时间
	MaxRuntime int           `bson:"max_runtime" json:"max_runtime"` // 预计运行时间，超过这个时间没有ack会重发
	Ack        bool          `bson:"ack" json:"ack"`                 // 是否需要确认机制
	CreatedAt  time.Time     `bson:"created_at" json:"created_at"`   // 创建时间
}

func AddTask(db *mgo.Database, task TaskInfo) (bool, error) {
	err := db.C(COLL_TASK_INFO).Insert(&task)
	// 有重复的任务存在
	if mgo.IsDup(err) {
		return true, nil
	}
	return false, err
}

func DelTask(db *mgo.Database, taskId string) error {
	_id := bson.ObjectIdHex(taskId)
	query := bson.M{Id: _id}
	return db.C(COLL_TASK_INFO).Remove(query)
}

func DelTaskByName(db *mgo.Database, name string) error {
	query := bson.M{Name: name}
	return db.C(COLL_TASK_INFO).Remove(query)
}

func QueryTaskById(db *mgo.Database, taskId string) (TaskInfo, error) {
	_id := bson.ObjectIdHex(taskId)
	query := bson.M{Id: _id}
	result := TaskInfo{}
	err := db.C(COLL_TASK_INFO).Find(query).One(&result)
	return result, err
}

func QueryTaskByName(db *mgo.Database, name string) ([]TaskInfo, error) {
	query := bson.M{Name: name}
	taskInfos := make([]TaskInfo, 0)
	err := db.C(COLL_TASK_INFO).Find(query).Sort("-" + CreateTime).All(&taskInfos)
	return taskInfos, err
}

func FindTaskByNameAndData(db *mgo.Database, name, data string) (*TaskInfo, error) {
	query := bson.M{Name: name, Data: data}

	var result TaskInfo
	err := db.C(COLL_TASK_INFO).Find(query).One(&result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func Index(db *mgo.Database) (*mgo.Collection, error) {
	coll := db.C(COLL_TASK_INFO)
	err := coll.EnsureIndex(mgo.Index{
		Key:        []string{Name, "-" + Data},
		Unique:     true,
		Background: true,
	})
	return coll, err
}
