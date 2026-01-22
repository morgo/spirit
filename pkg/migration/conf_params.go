package migration

import "github.com/go-ini/ini"

// confParams abstracts parameters loaded from ini file. Will provide defaults
// when receiver is nil or parameter is not defined.
type confParams struct {
	host, database, user, tlsMode, tlsCA string
	password                             *string
	port                                 int
}

func (c *confParams) GetHost() string {
	if c == nil || c.host == "" {
		return defaultHost
	}

	return c.host
}

func (c *confParams) GetDatabase() string {
	if c == nil || c.database == "" {
		return defaultDatabase
	}

	return c.database
}

func (c *confParams) GetUser() string {
	if c == nil || c.user == "" {
		return defaultUsername
	}

	return c.user
}

func (c *confParams) GetPassword() string {
	if c == nil || c.password == nil {
		return defaultPassword
	}

	return *c.password
}

func (c *confParams) GetTLSMode() string {
	if c == nil || c.tlsMode == "" {
		return defaultTLSMode
	}

	return c.tlsMode
}

// N.B. There is no default for tls-ca
func (c *confParams) GetTLSCA() string {
	if c == nil {
		return ""
	}

	return c.tlsCA
}

func (c *confParams) GetPort() int {
	if c == nil || c.port == 0 {
		return defaultPort
	}

	return c.port
}

// newConfParams attempts to load a confParams struct from a path to an ini file.
func newConfParams(confFilePath string) (*confParams, error) {
	confParams := &confParams{}

	if confFilePath == "" {
		return confParams, nil
	}

	creds, err := ini.Load(confFilePath)
	if err != nil {
		return nil, err
	}

	if creds.HasSection("client") {
		clientSection := creds.Section("client")
		confParams.host = clientSection.Key("host").String()
		confParams.database = clientSection.Key("database").String()
		confParams.user = clientSection.Key("user").String()
		confParams.tlsMode = clientSection.Key("tls-mode").String()
		confParams.tlsCA = clientSection.Key("tls-ca").String()
		confParams.port = clientSection.Key("port").MustInt()

		if clientSection.HasKey("password") {
			pw := clientSection.Key("password").String()
			confParams.password = &pw
		}
	}

	return confParams, nil
}
