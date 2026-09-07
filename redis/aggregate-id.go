package redis

import (
	"strings"

	"github.com/kode4food/timebox"
)

const keySeparator = ':'

func joinAggregateID(id timebox.AggregateID) string {
	var b strings.Builder
	b.Grow(escapedLen(string(id.Type)) + escapedLen(string(id.Key)) + 1)
	appendEscaped(&b, string(id.Type))
	b.WriteByte(keySeparator)
	appendEscaped(&b, string(id.Key))
	return b.String()
}

func parseAggregateID(value string) timebox.AggregateID {
	typ, key := cutEscaped(value)
	return timebox.NewAggregateID(timebox.ID(typ), timebox.ID(key))
}

func appendEscaped(b *strings.Builder, value string) {
	start := 0
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c != '\\' && c != keySeparator {
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

// cutEscaped takes everything past the first unescaped separator as the key,
// however many separators it contains
func cutEscaped(value string) (string, string) {
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\\':
			i++
		case keySeparator:
			return unescape(value[:i]), unescape(value[i+1:])
		}
	}
	return unescape(value), ""
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

func escapedLen(value string) int {
	res := len(value)
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c == '\\' || c == keySeparator {
			res++
		}
	}
	return res
}
