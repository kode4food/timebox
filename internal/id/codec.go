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

// Parts returns an AggregateID's parts as the string type storage wants
func Parts[T ~string](aggID timebox.AggregateID) []T {
	parts := aggID.Parts()
	res := make([]T, len(parts))
	for i, part := range parts {
		res[i] = T(part)
	}
	return res
}

func makeJoiner(sep byte) Joiner {
	return func(id timebox.AggregateID) string {
		parts := id.Parts()
		n := max(len(parts)-1, 0)
		for _, part := range parts {
			n += escapedLen(string(part), sep)
		}

		var b strings.Builder
		b.Grow(n)
		for i, part := range parts {
			if i > 0 {
				b.WriteByte(sep)
			}
			appendEscaped(&b, string(part), sep)
		}
		return b.String()
	}
}

func makeParser(sep byte) Parser {
	return func(value string) timebox.AggregateID {
		typ, key, split := cutEscaped(value, sep)
		if !split {
			return timebox.NewAggregateType(timebox.ID(typ))
		}
		return timebox.NewAggregateID(timebox.ID(typ), timebox.ID(key))
	}
}

func appendEscaped(b *strings.Builder, value string, sep byte) {
	start := 0
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c != '\\' && c != sep {
			continue
		}
		b.WriteString(value[start:i])
		b.WriteByte('\\')
		b.WriteByte(c)
		start = i + 1
	}
	if start == 0 {
		b.WriteString(value)
		return
	}
	b.WriteString(value[start:])
}

// cutEscaped splits value at its first unescaped sep, unescaping both sides.
// Everything past that separator is the key, however many separators it
// contains, so a joined AggregateID always parses back to at most two parts
func cutEscaped(value string, sep byte) (string, string, bool) {
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\':
			i++
		case sep:
			return unescape(value[:i]), unescape(value[i+1:]), true
		}
	}
	return unescape(value), "", false
}

func unescape(value string) string {
	if strings.IndexByte(value, '\\') < 0 {
		return value
	}

	var b strings.Builder
	b.Grow(len(value))
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		if i+1 < len(value) {
			i++
			b.WriteByte(value[i])
			continue
		}
		b.WriteByte('\\')
	}
	return b.String()
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
