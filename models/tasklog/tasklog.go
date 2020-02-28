package tasklog

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	COLL_TASKLOG = "tasklog"
)

const (
	Id      = "_id"
	Name    = "name"
	ExeTime = "exetime"
	Taskid  = "taskid"
)

type TaskLog struct {
	// 防止一些并发的情况重复调用 taskid和exctime做唯一
	Id      bson.ObjectId `bson:"_id"`
	Name    string        `bson:"name"`
	ExeTime int64         `bson:"exetime"`
	TaskId  string        `bson:"taskid"`
}

func AddTaskLog(db *mgo.Database, log TaskLog) error {
	return db.C(COLL_TASKLOG).Insert(&log)
}

func Index(db *mgo.Database) (*mgo.Collection, error) {
	coll := db.C(COLL_TASKLOG)
	err := coll.EnsureIndex(mgo.Index{
		Key:        []string{Taskid, ExeTime},
		Unique:     true,
		Background: true,
	})
	err = coll.EnsureIndex(mgo.Index{
		Key:        []string{Name, "-" + ExeTime},
		Background: true,
	})
	return coll, err
}
