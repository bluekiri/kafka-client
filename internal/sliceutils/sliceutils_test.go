package sliceutils_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/bluekiri/kafka-client/internal/sliceutils"
)

func TestContainsPositive(t *testing.T) {
	const expected = "expected"
	array := []string {
		"",
		"a",
		expected,
		"b",
	}

	if !sliceutils.Contains(array, expected) {
		t.Fatalf("Contains should have returned true")
	}
}

func TestContainsNegative(t *testing.T) {
	const not_expected = "this is not expected"
	array := []string {
		"",
		"a",
		not_expected + " some sufix",
		"b",
	}

	if sliceutils.Contains(array, not_expected) {
		t.Fatalf("Contains should have returned false")
	}
}

const dontCare = ""
func TestAnd(t *testing.T) {
	testCases := []struct {
		a bool
		b bool
		want bool
	}{
		{false, false, false},
		{false, true, false},
		{true, false, false},
		{true, true, true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v AND %v", tc.a, tc.b), func(t *testing.T) {
			actual := constantFilter(tc.a).And(constantFilter(tc.b))(dontCare)
			if actual != tc.want {
				t.Errorf("expected %v but got %v", tc.want, actual)
			}
		})
	}
}

func TestIsNotEqual(t *testing.T) {
	const string1 = "string1"
	const string2 = "string2"
	testCases := []struct {
		a string
		b string
		want bool
	}{
		{string1, string1, false},
		{string1, string2, true},
		{string2, string1, true},
		{string2, string2, false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v IsNotEqual %v", tc.a, tc.b), func(t *testing.T) {
			actual := sliceutils.IsNotEqual(tc.a)(tc.b)
			if actual != tc.want {
				t.Errorf("expected %v but got %v", tc.want, actual)
			}
		})
	}
}

func TestHasPrefix(t *testing.T) {
	const prefix = "prefix"
	const suffix = "suffix"
	testCases := []struct {
		prefix string
		value string
		want bool
	}{
		{prefix, "", false},
		{prefix, suffix, false},
		{prefix, suffix+prefix, false},
		{prefix, prefix+suffix, true},
		{prefix, prefix, true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v HasPrefix %v", tc.value, tc.prefix), func(t *testing.T) {
			actual := sliceutils.HasPrefix(tc.prefix)(tc.value)
			if actual != tc.want {
				t.Errorf("expected %v but got %v", tc.want, actual)
			}
		})
	}
}

func TestHasSuffix(t *testing.T) {
	const prefix = "prefix"
	const suffix = "suffix"
	testCases := []struct {
		suffix string
		value string
		want bool
	}{
		{suffix, "", false},
		{suffix, prefix, false},
		{suffix, suffix+prefix, false},
		{suffix, prefix+suffix, true},
		{suffix, suffix, true},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%v HasSuffix %v", tc.value, tc.suffix), func(t *testing.T) {
			actual := sliceutils.HasSuffix(tc.suffix)(tc.value)
			if actual != tc.want {
				t.Errorf("expected %v but got %v", tc.want, actual)
			}
		})
	}
}

func TestFilterSlice(t *testing.T) {
	testCases := []struct {
		slice []string
		filter sliceutils.Filter
		want []string
	}{
		{[]string{}, constantFilter(true), []string{}},
		{[]string{"1", "2", "3", "2"}, constantFilter(false), []string{}},
		{[]string{"1", "2", "3", "2"}, constantFilter(true), []string{"1", "2", "3", "2"}},
		{[]string{"1", "2", "3", "2"}, sliceutils.IsNotEqual("2"), []string{"1", "3"}},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("FilterSlice-%d", i), func(t *testing.T) {
			actual := sliceutils.FilterSlice(tc.slice, tc.filter)
			if !reflect.DeepEqual(actual, tc.want) {
				t.Errorf("expected %v but got %v", tc.want, actual)
			}
		})
	}
}

func constantFilter(value bool) sliceutils.Filter {
	return func(_ string) bool {
		return value
	}
}