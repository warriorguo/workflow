package utils

func ReverseMap[K comparable, V comparable](m map[K]V) map[V]K {
	reverseMap := make(map[V]K)
	for k, v := range m {
		reverseMap[v] = k
	}
	return reverseMap
}

func CloneMap[K comparable, V any](m map[K]V) map[K]V {
	cloneM := make(map[K]V)
	for k, v := range m {
		cloneM[k] = v
	}
	return cloneM
}

func UniqueSlice[K comparable](a []K) []K {
	m := make(map[K]bool)
	for i := 0; i < len(a); {
		v := a[i]
		if !m[v] {
			m[v] = true
			i++
			continue
		}
		a = append(a[:i], a[i+1:]...)
	}
	return a
}
