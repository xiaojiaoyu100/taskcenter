package crontab

import (
	"testing"
)

func TestCheckCronString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"TestCaseOK1", args{s: "1,5,3 * 1-10 * *"}, true},
		{"TestCaseOK2", args{"55-58 * * * *"}, true},
		{"TestCaseError1", args{""}, false},
		{"TestCaseError2", args{"* * 34 * *"}, false},
		{"TestCaseError3", args{"* 25 * * * "}, false},

	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := CheckCronString(tt.args.s); got != tt.want {
				t.Errorf("CheckCronString() = %v, want %v", got, tt.want)
			}
		})
	}
}