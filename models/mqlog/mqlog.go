package mqlog

import (
	"time"

	"gopkg.in/mgo.v2"
)

const (
	COLL_MQLOG = "task_mqlog"
)

const (
	Id        = "_id"
	Name      = "name"
	ExeTime   = "exetime"
	TaskId    = "taskid"
	TaskLogId = "tasklogid"
)

type MqLog struct {
	Name      string    `bson:"name"`
	ExeTime   int64     `bson:"exetime"`
	TaskId    string    `bson:"taskid"`
	TaskLogId string    `bson:"tasklogid"`
	MqId      string    `bson:"mqid"`
	LogTime   time.Time `bson:"logtime"`
}

func AddMqLog(db *mgo.Database, log MqLog) error {
	return db.C(COLL_MQLOG).Insert(log)
}

func Index(db *mgo.Database) (*mgo.Collection, error) {
	coll := db.C(COLL_MQLOG)
	err := coll.EnsureIndex(mgo.Index{
		Key:        []string{Name, "-" + ExeTime},
		Background: true,
	})
	err = coll.EnsureIndex(mgo.Index{
		Key:        []string{TaskLogId},
		Background: true,
	})
	err = coll.EnsureIndex(mgo.Index{
		Key:        []string{TaskId},
		Background: true,
	})
	return coll, err
}
