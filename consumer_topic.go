package kafka

type subscriptionEvent struct {
	subscribed bool
	topic      string
	partition  int32
}

type Topic struct {
	Name         string
	lagTolerance int64
	partitionLag map[int32]int64
}

type TopicOption func(topic *Topic)

func NewTopic(name string, opts ...TopicOption) *Topic {
	t := &Topic{
		Name:         name,
		partitionLag: map[int32]int64{},
	}

	for _, opt := range opts {
		opt(t)
	}

	if t.lagTolerance == 0 {
		t.lagTolerance = 500
	}

	return t
}

// WithLagTolerance sets custom lag tolerance threshold used for monitoring.
// Consumer lagging behind with more messages than the configured tolerance will be reported as unhealthy.
// Default is 500 messages.
func WithLagTolerance(tolerance int64) TopicOption {
	return func(t *Topic) {
		t.lagTolerance = tolerance
	}
}
