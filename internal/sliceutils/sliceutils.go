/*
Copyright © 2023 VECI Group Tech S.L.
This file is part of kafka-client.
*/

package sliceutils

import "strings"

func Contains(slice []string, element string) bool {
	for _, s := range slice {
		if element == s {
			return true
		}
	}
	return false
}

type Filter func(string) bool

func (f Filter) And(filter Filter) Filter {
	return func(s string) bool {
		return f(s) && filter(s)
	}
}

func FilterSlice(slice []string, filter Filter) []string {
	filtered := make([]string, 0, len(slice))
	for _, element := range slice {
		if filter(element) {
			filtered = append(filtered, element)
		}
	}
	return filtered
}

func IsNotEqual(s string) Filter {
	return func(x string) bool {
		return s != x
	}
}

func HasPrefix(prefix string) Filter {
	return func(x string) bool {
		return strings.HasPrefix(x, prefix)
	}
}

func HasSuffix(prefix string) Filter {
	return func(x string) bool {
		return strings.HasSuffix(x, prefix)
	}
}
