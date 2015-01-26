package main

type IntSet map[int64]struct{}

func NewIntSet() IntSet {
	return make(map[int64]struct{})
}

func (set IntSet) Add(v int64) {
	if _, ok := set[v]; ok {
		return
	}
	set[v] = struct{}{}
}

func (set IntSet) IsMember(v int64) bool {
	if _, ok := set[v]; ok {
		return true
	}
	return false
}

func (set IntSet) Remove(v int64) {
	if _, ok := set[v]; !ok {
		return
	}
	delete(set, v)
}

func (set IntSet) Clone() IntSet {
	n := make(map[int64]struct{})
	for k, v := range set {
		n[k] = v
	}
	return n
}
