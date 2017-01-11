package util

func ConvertSliceFromIntToInt32(input []int) []int32 {
	result := make([]int32, len(input))
	for i, v := range input {
		result[i] = int32(v)
	}
	return result
}

func ConvertSliceFromInt32ToInt(input []int32) []int {
	result := make([]int, len(input))
	for i, v := range input {
		result[i] = int(v)
	}
	return result
}
