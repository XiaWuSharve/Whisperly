package utils

func ToByte(d bool) byte {
	if d {
		return 1
	} else {
		return 0
	}
}

func ToBool(d byte) bool {
	return d == 1
}
