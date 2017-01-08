package util

type IntSet struct {
	m map[int]bool
}

func (s *IntSet) Add(value int) {
	s.ensureMap()
	s.m[value] = true
}

func (s *IntSet) Contains(value int) bool {
	if s.m == nil {
		return false
	}

	return s.m[value]
}

func (s *IntSet) Remove(value int) {
	if s.m == nil {
		return
	}

	delete(s.m, value)
}

func (s *IntSet) Count() int {
	if s.m == nil {
		return 0
	}

	return len(s.m)
}

func (s *IntSet) IsEmpty() bool {
	return len(s.m) == 0
}

func (s *IntSet) Clear() {
	s.m = nil
}

func (s *IntSet) Clone() *IntSet {
	result := &IntSet{}
	if s.m != nil {
		result.ensureMap()
		for k := range s.m {
			result.m[k] = true
		}
	}
	return result
}

func (s *IntSet) RemoveFirst() (int, bool) {
	if s.IsEmpty() {
		return 0, false
	}

	var result int
	for result = range s.m {
		break
	}
	delete(s.m, result)

	return result, true
}

func (s *IntSet) ToArray() []int {
	result := make([]int, len(s.m))
	i := 0
	for v := range s.m {
		result[i] = v
		i++
	}

	return result
}

func (s *IntSet) ensureMap() {
	if s.m == nil {
		s.m = make(map[int]bool)
	}
}
