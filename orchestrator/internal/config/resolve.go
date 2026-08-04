package config

// QueueByName returns the queue definition with the given name.
func (c *Config) QueueByName(name string) (QueueDef, bool) {
	for _, q := range c.Queues.Definitions {
		if q.Name == name {
			return q, true
		}
	}
	return QueueDef{}, false
}

// DefaultQueue returns the first defined queue, used when a task does not
// specify one.
func (c *Config) DefaultQueue() (QueueDef, bool) {
	if len(c.Queues.Definitions) == 0 {
		return QueueDef{}, false
	}
	return c.Queues.Definitions[0], true
}

// ResolveQueue returns the queue for a (possibly empty) name: the named queue
// if it exists, otherwise the default queue.
func (c *Config) ResolveQueue(name string) (QueueDef, bool) {
	if name != "" {
		if q, ok := c.QueueByName(name); ok {
			return q, true
		}
	}
	return c.DefaultQueue()
}
