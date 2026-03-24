package id

import "github.com/kode4food/timebox"

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
			for j := range len(part) {
				c := part[j]
				if c == '\\' || c == sep {
					res = append(res, '\\')
				}
				res = append(res, c)
			}
		}
		return string(res)
	}
}

func makeParser(sep byte) Parser {
	return func(value string) timebox.AggregateID {
		res := make(timebox.AggregateID, 0, countParts(value, sep))
		part := make([]byte, 0, len(value))
		esc := false

		for i := range len(value) {
			c := value[i]
			switch {
			case esc:
				part = append(part, c)
				esc = false
			case c == '\\':
				esc = true
			case c == sep:
				res = append(res, timebox.ID(string(part)))
				part = part[:0]
			default:
				part = append(part, c)
			}
		}
		if esc {
			part = append(part, '\\')
		}
		return append(res, timebox.ID(string(part)))
	}
}

func countParts(value string, sep byte) int {
	if value == "" {
		return 1
	}

	res := 1
	esc := false
	for i := range len(value) {
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
	for i := range len(value) {
		c := value[i]
		if c == '\\' || c == sep {
			res++
		}
	}
	return res
}
