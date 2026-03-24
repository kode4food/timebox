package id

import (
	"strings"

	"github.com/kode4food/timebox"
)

type (
	// Joiner joins AggregateID parts into a single string
	Joiner func(timebox.AggregateID) string

	// Parser parses a single string into an AggregateID
	Parser func(string) timebox.AggregateID
)

// MakeCodec makes separator-aware AggregateID join and parse funcs
func MakeCodec(sep byte) (Joiner, Parser) {
	return makeJoiner(sep), makeParser(sep)
}

func makeJoiner(sep byte) Joiner {
	return func(id timebox.AggregateID) string {
		n := max(len(id)-1, 0)
		for _, part := range id {
			n += escapedLen(string(part), sep)
		}

		res := make([]byte, 0, n)
		for i, part := range id {
			if i > 0 {
				res = append(res, sep)
			}
			res = appendEscaped(res, string(part), sep)
		}
		return string(res)
	}
}

func makeParser(sep byte) Parser {
	return func(value string) timebox.AggregateID {
		res := make(timebox.AggregateID, 0, countParts(value, sep))
		var part []byte
		escaped := false
		start := 0

		for i := 0; i < len(value); i++ {
			c := value[i]
			switch c {
			case '\\':
				if !escaped {
					part = part[:0]
					escaped = true
				}
				part = append(part, value[start:i]...)
				if i+1 < len(value) {
					part = append(part, value[i+1])
					i++
				} else {
					part = append(part, '\\')
				}
				start = i + 1
			case sep:
				if escaped {
					part = append(part, value[start:i]...)
					res = append(res, timebox.ID(string(part)))
					escaped = false
				} else {
					res = append(res, timebox.ID(strings.Clone(value[start:i])))
				}
				start = i + 1
			}
		}
		if escaped {
			part = append(part, value[start:]...)
			return append(res, timebox.ID(string(part)))
		}
		return append(res, timebox.ID(strings.Clone(value[start:])))
	}
}

func appendEscaped(res []byte, value string, sep byte) []byte {
	start := 0
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c != '\\' && c != sep {
			continue
		}
		res = append(res, value[start:i]...)
		res = append(res, '\\', c)
		start = i + 1
	}
	if start == 0 {
		return append(res, value...)
	}
	return append(res, value[start:]...)
}

func countParts(value string, sep byte) int {
	if value == "" {
		return 1
	}

	res := 1
	esc := false
	for i := 0; i < len(value); i++ {
		c := value[i]
		switch {
		case esc:
			esc = false
		case c == '\\':
			esc = true
		case c == sep:
			res++
		}
	}
	return res
}

func escapedLen(value string, sep byte) int {
	res := len(value)
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '\\' || c == sep {
			res++
		}
	}
	return res
}
