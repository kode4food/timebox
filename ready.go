package timebox

var readyNowCh = func() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// ReadyNow returns a closed readiness channel
func ReadyNow() <-chan struct{} {
	return readyNowCh
}
