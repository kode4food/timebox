package compliance

import "testing"

func Run(t *testing.T, p Profile) {
	t.Helper()

	t.Run("Lifecycle", func(t *testing.T) { runLifecycle(t, p) })
	t.Run("Events", func(t *testing.T) { runEvents(t, p) })
	t.Run("Aggregates", func(t *testing.T) { runAggregates(t, p) })
	t.Run("Indexing", func(t *testing.T) { runIndexing(t, p) })
	t.Run("Snapshots", func(t *testing.T) { runSnapshots(t, p) })
	t.Run("Archive", func(t *testing.T) { runArchive(t, p) })
}
