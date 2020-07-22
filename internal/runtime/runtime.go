package runtime

const (
	// Pull mode is the classic mode recommended by Prometheus - exporter listens for scrapes made by remote system.
	PullMode int = 1
	// Push mode is the old-style mode when exporter push collected metrics into remote system.
	PushMode int = 2
)
