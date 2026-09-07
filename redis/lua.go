package redis

import _ "embed"

var (
	//go:embed lua/append.lua
	luaAppend string

	//go:embed lua/consume-archive.lua
	luaConsumeArchive string

	//go:embed lua/get-events.lua
	luaGetEvents string

	//go:embed lua/get-events-trim.lua
	luaGetEventsTrim string

	//go:embed lua/get-snapshot.lua
	luaGetSnapshot string

	//go:embed lua/get-snapshot-trim.lua
	luaGetSnapshotTrim string

	//go:embed lua/publish-archive.lua
	luaPublishArchive string

	//go:embed lua/put-snapshot.lua
	luaPutSnapshot string

	//go:embed lua/put-snapshot-trim.lua
	luaPutSnapshotTrim string
)
