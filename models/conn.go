package models

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/mgo.v2"
)

var (
	session     *mgo.Session
	sessionOnce sync.Once
	dbname      string
)

const POOLSIZE = 100

type DbConfig struct {
	Url    string
	DbName string
}

// Connect - Connect to mongo db
func InitDB(url string, db string) {
	dbname = db
	sessionOnce.Do(func() {
		var err error
		session, err = mgo.Dial(url)
		if err != nil {
			log.Panicf("mongodb connect error url=%s, err=%s", url, err.Error())
		}
		log.Debugf("Mongo connect success url=%s, db=%s", url, db)

		session.SetMode(mgo.Monotonic, true)
		session.SetSocketTimeout(120 * time.Second)
		session.SetPoolLimit(POOLSIZE)
	})
}

// 注意这个函数只关闭全局的Session,不是复制的Session
func CloseDB() {
	session.Close()
}

//
func copySession() *mgo.Session {
	// return session.Copy()
	return session.Copy()
}

// 注意一定要调用defer session.Close()
func GetDB() (*mgo.Session, *mgo.Database) {
	session := copySession()
	db := session.DB(dbname)

	return session, db
}
