package httpapi

import "encoding/json"

type ErrorCode struct {
	Err   int              `json:"err"`
	Msg   string           `json:"msg"`
	MsgZh string           `json:"msg_zh,omitempty"`
	Desc  map[string][]int `json:"desc,omitempty"`
}

func (err ErrorCode) Error() string {
	data, _ := json.Marshal(err)
	return string(data)
}

func (err ErrorCode) HasError() bool {
	return err.Err != 0
}
