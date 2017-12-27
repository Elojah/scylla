package scylla_test

import (
	"testing"

	"github.com/elojah/scylla"
	"github.com/elojah/services"
)

func TestUp(t *testing.T) {
	s := &scylla.Service{}
	l := s.NewLauncher(scylla.Namespaces{
		Scylla: "scylla",
	}, "scylla")
	ls := services.Launchers{}
	ls = append(ls, l)
	if err := ls.Up("config_test.json"); err != nil {
		t.Error(err)
	}
}
