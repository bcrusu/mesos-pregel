package util

func Round(f float64) int {
	// more details here https://github.com/golang/go/issues/4594 on why Go is missing the math.Round() function
	if f < -0.5 {
		return int(f - 0.5)
	}
	if f > 0.5 {
		return int(f + 0.5)
	}
	return 0
}
