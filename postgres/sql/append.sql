SELECT timebox_append_events(
	$1, $2, $3, $4, $5, $6::text[], $7::boolean[],
	$8::bigint[], $9::text[], $10::bytea[]
)
