package utils

type Backoff struct {
}

func (b *Backoff) Retry(handler func() error) error {
	return handler()
}
