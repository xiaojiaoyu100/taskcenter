package schema

type AddTaskArgs struct {
	Name       string `json:"name"`
	Type       int    `json:"type"`
	Data       string `json:"data"`
	Cron       string `json:"crontab"`     // crontab, 定时任务类型会使用到
	LoopTime   int    `json:"loop_time"`   // 循环执行任务之间间隔时间
	LoopCount  int    `json:"loop_count"`  // 执行次数
	StartTime  int64  `json:"start_time"`  // 循环开始时间
	EndTime    int64  `json:"end_time"`    // 循环结束时间
	MaxRuntime int    `json:"max_runtime"` // 预计运行时间，超过这个时间没有ack会重发
	Ack        bool   `json:"ack"`         // 是否需要确认机制
}
