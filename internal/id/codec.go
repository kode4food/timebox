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
			n += len(part)
		}

		var b strings.Builder
		b.Grow(n)

		for i, part := range id {
			if i > 0 {
				b.WriteByte(sep)
			}
			for j := range len(part) {
				c := part[j]
				if c == '\\' || c == sep {
					b.WriteByte('\\')
				}
				b.WriteByte(c)
			}
		}
		return b.String()
	}
}

func makeParser(sep byte) Parser {
	return func(value string) timebox.AggregateID {
		res := make(timebox.AggregateID, 0, 1)
		var b strings.Builder
		esc := false

		for i := range len(value) {
			c := value[i]
			switch {
			case esc:
				b.WriteByte(c)
				esc = false
			case c == '\\':
				esc = true
			case c == sep:
				res = append(res, timebox.ID(b.String()))
				b.Reset()
			default:
				b.WriteByte(c)
			}
		}
		if esc {
			b.WriteByte('\\')
		}
		return append(res, timebox.ID(b.String()))
	}
}
