package redis

import _ "embed"

var (
	//go:embed lua/append.lua
	luaAppend string

	//go:embed lua/archive.lua
	luaArchive string

	//go:embed lua/get-events.lua
	luaGetEvents string

	//go:embed lua/get-snapshot.lua
	luaGetSnapshot string

	//go:embed lua/put-snapshot.lua
	luaPutSnapshot string
)
