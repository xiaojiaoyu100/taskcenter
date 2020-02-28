package crontab

import (
	"time"

	"gopkg.in/robfig/cron.v2"
)

func CheckCronString(s string) bool {
	_, err := cron.Parse(s)
	if err != nil{
		return false
	}
	return true
}

func ParseAndGetNextTime(s string)(*time.Time, error) {
	scheduler, err := cron.Parse(s)
	if err != nil {
		return nil, err
	}
	t := scheduler.Next(time.Now())
	return &t, nil
}

