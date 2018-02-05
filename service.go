package scylla

import (
	"github.com/gocql/gocql"
)

// Service represents the scylla service.
type Service struct {
	cluster *gocql.ClusterConfig
	Session *gocql.Session
}

// Dial sends the new config to Service.
func (s *Service) Dial(c Config) error {
	s.cluster = gocql.NewCluster(c.Hosts...)
	s.cluster.Keyspace = c.Keyspace
	s.cluster.Timeout = c.Timeout
	// Authentication
	s.cluster.Authenticator = &gocql.PasswordAuthenticator{
		Username: c.Username,
		Password: c.Password,
	}
	// SSL secure connection
	if c.CertPath != "" && c.KeyPath != "" {
		s.cluster.SslOpts = &gocql.SslOptions{
			CertPath: c.CertPath,
			KeyPath:  c.KeyPath,
		}
	}

	if err := s.cluster.Consistency.UnmarshalText([]byte(c.Consistency)); err != nil {
		return err
	}

	// Optional arguments
	if c.NumRetries != 0 {
		s.cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: c.NumRetries}
	}

	var err error
	if s.Session, err = s.cluster.CreateSession(); err != nil {
		return err
	}
	return nil
}

// Close closes the session to cluster session.
func (s *Service) Close() error {
	s.Session.Close()
	return nil
}

// Healthcheck returns if database responds.
func (s *Service) Healthcheck() error {
	q := s.Session.Query(`SELECT dateof(now()) FROM system.local`)
	return q.Exec()
}
