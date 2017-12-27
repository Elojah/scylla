package scylla

import (
	"sync"

	"github.com/elojah/services"
)

// Namespaces maps configs used for scylla service with config file namespaces.
type Namespaces struct {
	Scylla services.Namespace
}

// Launcher represents a scylla launcher.
type Launcher struct {
	*services.Configs
	ns Namespaces

	s *Service
	m sync.Mutex
}

// NewLauncher returns a new scylla Launcher.
func (s *Service) NewLauncher(ns Namespaces, nsRead ...services.Namespace) *Launcher {
	return &Launcher{
		Configs: services.NewConfigs(nsRead...),
		s:       s,
		ns:      ns,
	}
}

// Up starts the scylla service with new configs.
func (l *Launcher) Up(configs services.Configs) error {
	l.m.Lock()
	defer l.m.Unlock()

	sconfig := Config{}
	if err := sconfig.Dial(configs[l.ns.Scylla]); err != nil {
		// Add namespace key when returning error with logrus
		return err
	}
	return l.s.Dial(sconfig)
}

// Down stops the scylla service.
func (l *Launcher) Down(configs services.Configs) error {
	l.m.Lock()
	defer l.m.Unlock()

	return l.s.Close()
}
