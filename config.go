package scylla

import (
	"errors"
	"time"
)

// Config is scylla structure config.
type Config struct {
	Hosts       []string      `json:"hosts"`
	Keyspace    string        `json:"keyspace"`
	Timeout     time.Duration `json:"timeout"`
	NumRetries  int           `json:"num_retries"`
	Consistency string        `json:"consistency"`
	Username    string        `json:"username"`
	Password    string        `json:"password"`
	CertPath    string        `json:"cert_path"`
	KeyPath     string        `json:"key_path"`
}

// Equal returns is both configs are equal.
func (c Config) Equal(rhs Config) bool {
	if len(c.Hosts) != len(rhs.Hosts) {
		return false
	}
	for i := range c.Hosts {
		if c.Hosts[i] != rhs.Hosts[i] {
			return false
		}
	}
	return (c.Keyspace == rhs.Keyspace &&
		c.Timeout == rhs.Timeout &&
		c.NumRetries == rhs.NumRetries &&
		c.Consistency == rhs.Consistency &&
		c.Username == rhs.Username &&
		c.Password == rhs.Password &&
		c.CertPath == rhs.CertPath &&
		c.KeyPath == rhs.KeyPath)
}

// Dial set the config from a config namespace.
func (c *Config) Dial(fileconf interface{}) error {
	var err error

	fconf, ok := fileconf.(map[string]interface{})
	if !ok {
		return errors.New("namespace empty")
	}

	cVal, ok := fconf["hosts"]
	if !ok {
		return errors.New("missing key hosts")
	}
	cHosts, ok := cVal.([]interface{})
	if !ok {
		return errors.New("key hosts invalid. must be array")
	}
	c.Hosts = make([]string, len(cHosts))
	for i := range cHosts {
		if c.Hosts[i], ok = cHosts[i].(string); !ok {
			return errors.New("key hosts invalid. must be string array")
		}
	}
	cVal, ok = fconf["keyspace"]
	if !ok {
		return errors.New("missing key keyspace")
	}
	if c.Keyspace, ok = cVal.(string); !ok {
		return errors.New("key keyspace invalid. must be string")
	}
	cVal, ok = fconf["timeout"]
	if !ok {
		return errors.New("missing key timeout")
	}
	cTimeout, ok := cVal.(string)
	if !ok {
		return errors.New("key timeout invalid. must be string")
	}
	if c.Timeout, err = time.ParseDuration(cTimeout); err != nil {
		return err
	}
	cVal, ok = fconf["num_retries"]
	if !ok {
		return errors.New("missing key num_retries")
	}
	cNumRetries, ok := cVal.(float64)
	if !ok {
		return errors.New("key num_retries invalid. must be number")
	}
	c.NumRetries = int(cNumRetries)
	cVal, ok = fconf["consistency"]
	if !ok {
		return errors.New("missing key consistency")
	}
	if c.Consistency, ok = cVal.(string); !ok {
		return errors.New("key consistency invalid. must be string")
	}
	cVal, ok = fconf["username"]
	if !ok {
		return errors.New("missing key username")
	}
	if c.Username, ok = cVal.(string); !ok {
		return errors.New("key username invalid. must be string")
	}
	cVal, ok = fconf["password"]
	if !ok {
		return errors.New("missing key password")
	}
	if c.Password, ok = cVal.(string); !ok {
		return errors.New("key password invalid. must be string")
	}
	cVal, ok = fconf["cert_path"]
	if !ok {
		return errors.New("missing key cert_path")
	}
	if c.CertPath, ok = cVal.(string); !ok {
		return errors.New("key cert_path invalid. must be string")
	}
	cVal, ok = fconf["key_path"]
	if !ok {
		return errors.New("missing key key_path")
	}
	if c.KeyPath, ok = cVal.(string); !ok {
		return errors.New("key key_path invalid. must be string")
	}

	return nil
}
