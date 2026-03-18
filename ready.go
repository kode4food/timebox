package timebox

// AlwaysReady can be embedded in a Persistence to provide a Ready()
// implementation for backends that are immediately available
type AlwaysReady struct{}

var readyNowCh = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// Ready returns a pre-closed channel, indicating immediate readiness
func (AlwaysReady) Ready() <-chan struct{} {
	return readyNowCh
}
