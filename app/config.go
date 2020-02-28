package app

type TopicConfig struct {
	// 每个topic每分钟报警数量
	NameAlertCountPerMin int
	// 总的报警数量
	TotalAlertCountPerMin int
}

// 用全局的变量，是否存在问题
var DefaultTopicConfig TopicConfig
