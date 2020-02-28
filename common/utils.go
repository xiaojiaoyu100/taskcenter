package common

func In(x int, target ...int) bool {
	for _, v := range target {
		if x == v {
			return true
		}
	}
	return false
}
