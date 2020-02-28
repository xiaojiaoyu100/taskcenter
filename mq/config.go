package mq

type Config struct {
	Instancename string   `mapstructure:"instance_name" json:"instancename"`
	NameServers  []string `mapstructure:"name_severs" json:"name_servers"`
	GroupId      string   `mapstructure:"group_id" json:"group_id"`
	GroupName    string   `mapstructure:"group_name" json:"group_name"`
	Topic        string   `mapstructure:"topic" json:"topic"`
	AccessKey    string   `mapstructure:"access_key" json:"access_key"`
	SecretKey    string   `mapstructure:"secret_key" json:"secret_key"`
}

type ConsumerConfig struct {
	Instancename string   `mapstructure:"instance_name" json:"instancename"`
	NameServers  []string `mapstructure:"name_servers" json:"name_servers"`
	GroupId      string   `mapstructure:"group_id" json:"group_id"`
	GroupName    string   `mapstructure:"group_name" json:"group_name"`
	Topic        string   `mapstructure:"topic" json:"topic"`
	AccessKey    string   `mapstructure:"access_key" json:"access_key"`
	SecretKey    string   `mapstructure:"secret_key" json:"secret_key"`
	Broadcasting int      `mapstructure:"Broadcasting" json:"broadcasting"`
	ThreadCount  int      `mapstructure:"thread_count" json:"thread_count"`
}
