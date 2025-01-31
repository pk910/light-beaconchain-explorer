package types

import "time"

// Config is a struct to hold the configuration data
type Config struct {
	Logging struct {
		OutputLevel  string `yaml:"outputLevel" envconfig:"LOGGING_OUTPUT_LEVEL"`
		OutputStderr bool   `yaml:"outputStderr" envconfig:"LOGGING_OUTPUT_STDERR"`

		FilePath  string `yaml:"filePath" envconfig:"LOGGING_FILE_PATH"`
		FileLevel string `yaml:"fileLevel" envconfig:"LOGGING_FILE_LEVEL"`
	} `yaml:"logging"`

	Server struct {
		Port string `yaml:"port" envconfig:"FRONTEND_SERVER_PORT"`
		Host string `yaml:"host" envconfig:"FRONTEND_SERVER_HOST"`
	} `yaml:"server"`

	Metrics struct {
		Enabled bool   `yaml:"enabled" envconfig:"METRICS_ENABLED"`
		Public  bool   `yaml:"public" envconfig:"METRICS_PUBLIC"`
		Port    string `yaml:"port" envconfig:"METRICS_PORT"`
		Host    string `yaml:"host" envconfig:"METRICS_HOST"`
	} `yaml:"metrics"`

	Chain struct {
		DisplayName string `yaml:"displayName" envconfig:"CHAIN_DISPLAY_NAME"`

		// optional features
		WhiskForkEpoch *uint64 `yaml:"whiskForkEpoch" envconfig:"WHISK_FORK_EPOCH"`
	} `yaml:"chain"`

	Frontend struct {
		Enabled bool `yaml:"enabled" envconfig:"FRONTEND_ENABLED"`
		Debug   bool `yaml:"debug" envconfig:"FRONTEND_DEBUG"`
		Pprof   bool `yaml:"pprof" envconfig:"FRONTEND_PPROF"`
		Minify  bool `yaml:"minify" envconfig:"FRONTEND_MINIFY"`

		SiteDomain      string `yaml:"siteDomain" envconfig:"FRONTEND_SITE_DOMAIN"`
		SiteLogo        string `yaml:"siteLogo" envconfig:"FRONTEND_SITE_LOGO"`
		SiteName        string `yaml:"siteName" envconfig:"FRONTEND_SITE_NAME"`
		SiteSubtitle    string `yaml:"siteSubtitle" envconfig:"FRONTEND_SITE_SUBTITLE"`
		SiteDescription string `yaml:"siteDescription" envconfig:"FRONTEND_SITE_DESCRIPTION"`

		EthExplorerLink     string `yaml:"ethExplorerLink" envconfig:"FRONTEND_ETH_EXPLORER_LINK"`
		PublicRPCUrl        string `yaml:"publicRpcUrl" envconfig:"FRONTEND_PUBLIC_RPC_URL"`
		RainbowkitProjectId string `yaml:"rainbowkitProjectId" envconfig:"FRONTEND_RAINBOWKIT_PROJECT_ID"`

		ValidatorNamesYaml            string        `yaml:"validatorNamesYaml" envconfig:"FRONTEND_VALIDATOR_NAMES_YAML"`
		ValidatorNamesInventory       string        `yaml:"validatorNamesInventory" envconfig:"FRONTEND_VALIDATOR_NAMES_INVENTORY"`
		ValidatorNamesRefreshInterval time.Duration `yaml:"validatorNamesRefreshInterval" envconfig:"FRONTEND_VALIDATOR_REFRESH_INTERVAL"`
		ValidatorNamesResolveInterval time.Duration `yaml:"validatorNamesResolveInterval" envconfig:"FRONTEND_VALIDATOR_RESOLVE_INTERVAL"`

		PageCallTimeout  time.Duration `yaml:"pageCallTimeout" envconfig:"FRONTEND_PAGE_CALL_TIMEOUT"`
		HttpReadTimeout  time.Duration `yaml:"httpReadTimeout" envconfig:"FRONTEND_HTTP_READ_TIMEOUT"`
		HttpWriteTimeout time.Duration `yaml:"httpWriteTimeout" envconfig:"FRONTEND_HTTP_WRITE_TIMEOUT"`
		HttpIdleTimeout  time.Duration `yaml:"httpIdleTimeout" envconfig:"FRONTEND_HTTP_IDLE_TIMEOUT"`
		AllowDutyLoading bool          `yaml:"allowDutyLoading" envconfig:"FRONTEND_ALLOW_DUTY_LOADING"`

		ShowSensitivePeerInfos bool `yaml:"showSensitivePeerInfos" envconfig:"FRONTEND_SHOW_SENSITIVE_PEER_INFOS"`
		ShowPeerDASInfos       bool `yaml:"showPeerDASInfos" envconfig:"FRONTEND_SHOW_PEER_DAS_INFOS"`
		ShowSubmitDeposit      bool `yaml:"showSubmitDeposit" envconfig:"FRONTEND_SHOW_SUBMIT_DEPOSIT"`
		ShowSubmitElRequests   bool `yaml:"showSubmitElRequests" envconfig:"FRONTEND_SHOW_SUBMIT_EL_REQUESTS"`
	} `yaml:"frontend"`

	RateLimit struct {
		Enabled    bool `yaml:"enabled" envconfig:"RATELIMIT_ENABLED"`
		ProxyCount uint `yaml:"proxyCount" envconfig:"RATELIMIT_PROXY_COUNT"`
		Rate       uint `yaml:"rate" envconfig:"RATELIMIT_RATE"`
		Burst      uint `yaml:"burst" envconfig:"RATELIMIT_BURST"`
	} `yaml:"rateLimit"`

	BeaconApi struct {
		Endpoint  string           `yaml:"endpoint" envconfig:"BEACONAPI_ENDPOINT"`
		Endpoints []EndpointConfig `yaml:"endpoints"`

		LocalCacheSize       int    `yaml:"localCacheSize" envconfig:"BEACONAPI_LOCAL_CACHE_SIZE"`
		SkipFinalAssignments bool   `yaml:"skipFinalAssignments" envconfig:"BEACONAPI_SKIP_FINAL_ASSIGNMENTS"`
		AssignmentsCacheSize int    `yaml:"assignmentsCacheSize" envconfig:"BEACONAPI_ASSIGNMENTS_CACHE_SIZE"`
		RedisCacheAddr       string `yaml:"redisCacheAddr" envconfig:"BEACONAPI_REDIS_CACHE_ADDR"`
		RedisCachePrefix     string `yaml:"redisCachePrefix" envconfig:"BEACONAPI_REDIS_CACHE_PREFIX"`
	} `yaml:"beaconapi"`

	ExecutionApi struct {
		Endpoint  string           `yaml:"endpoint" envconfig:"EXECUTIONAPI_ENDPOINT"`
		Endpoints []EndpointConfig `yaml:"endpoints"`

		LogBatchSize       int `yaml:"logBatchSize" envconfig:"EXECUTIONAPI_LOG_BATCH_SIZE"`
		DepositDeployBlock int `yaml:"depositDeployBlock" envconfig:"EXECUTIONAPI_DEPOSIT_DEPLOY_BLOCK"` // el block number from where to crawl the deposit system contract (should be <=, but close to deposit contract deployment)
		ElectraDeployBlock int `yaml:"electraDeployBlock" envconfig:"EXECUTIONAPI_ELECTRA_DEPLOY_BLOCK"` // el block number from where to crawl the electra system contracts (should be <=, but close to electra fork activation block)
	} `yaml:"executionapi"`

	Indexer struct {
		ResyncFromEpoch   *uint64 `yaml:"resyncFromEpoch" envconfig:"INDEXER_RESYNC_FROM_EPOCH"`
		ResyncForceUpdate bool    `yaml:"resyncForceUpdate" envconfig:"INDEXER_RESYNC_FORCE_UPDATE"`

		InMemoryEpochs                  uint16 `yaml:"inMemoryEpochs" envconfig:"INDEXER_IN_MEMORY_EPOCHS"`
		ActivityHistoryLength           uint16 `yaml:"activityHistoryLength" envconfig:"INDEXER_ACTIVITY_HISTORY_LENGTH"`
		DisableSynchronizer             bool   `yaml:"disableSynchronizer" envconfig:"INDEXER_DISABLE_SYNCHRONIZER"`
		SyncEpochCooldown               uint   `yaml:"syncEpochCooldown" envconfig:"INDEXER_SYNC_EPOCH_COOLDOWN"`
		MaxParallelValidatorSetRequests uint   `yaml:"maxParallelValidatorSetRequests" envconfig:"INDEXER_MAX_PARALLEL_VALIDATOR_SET_REQUESTS"`
	} `yaml:"indexer"`

	TxSignature struct {
		DisableLookupLoop bool          `yaml:"disableLookupLoop" envconfig:"TXSIG_DISABLE_LOOKUP_LOOP"`
		LookupInterval    time.Duration `yaml:"lookupInterval" envconfig:"TXSIG_LOOKUP_INTERVAL"`
		LookupBatchSize   uint64        `yaml:"lookupBatchSize" envconfig:"TXSIG_LOOKUP_INTERVAL"`
		ConcurrencyLimit  uint64        `yaml:"concurrencyLimit" envconfig:"TXSIG_CONCURRENCY_LIMIT"`
		Disable4Bytes     bool          `yaml:"disable4Bytes" envconfig:"TXSIG_DISABLE_4BYTES"`
		RecheckTimeout    time.Duration `yaml:"recheckTimeout" envconfig:"TXSIG_RECHECK_TIMEOUT"`
	} `yaml:"txsig"`

	MevIndexer struct {
		Relays          []MevRelayConfig `yaml:"relays"`
		RefreshInterval time.Duration    `yaml:"refreshInterval" envconfig:"MEVINDEXER_REFRESH_INTERVAL"`
	} `yaml:"mevIndexer"`

	Database struct {
		Engine string `yaml:"engine" envconfig:"DATABASE_ENGINE"`
		Sqlite struct {
			File         string `yaml:"file" envconfig:"DATABASE_SQLITE_FILE"`
			MaxOpenConns int    `yaml:"maxOpenConns" envconfig:"DATABASE_SQLITE_MAX_OPEN_CONNS"`
			MaxIdleConns int    `yaml:"maxIdleConns" envconfig:"DATABASE_SQLITE_MAX_IDLE_CONNS"`
		} `yaml:"sqlite"`
		Pgsql struct {
			Username     string `yaml:"user" envconfig:"DATABASE_PGSQL_USERNAME"`
			Password     string `yaml:"password" envconfig:"DATABASE_PGSQL_PASSWORD"`
			Name         string `yaml:"name" envconfig:"DATABASE_PGSQL_NAME"`
			Host         string `yaml:"host" envconfig:"DATABASE_PGSQL_HOST"`
			Port         string `yaml:"port" envconfig:"DATABASE_PGSQL_PORT"`
			MaxOpenConns int    `yaml:"maxOpenConns" envconfig:"DATABASE_PGSQL_MAX_OPEN_CONNS"`
			MaxIdleConns int    `yaml:"maxIdleConns" envconfig:"DATABASE_PGSQL_MAX_IDLE_CONNS"`
		} `yaml:"pgsql"`
		PgsqlWriter struct {
			Username     string `yaml:"user" envconfig:"DATABASE_PGSQL_WRITER_USERNAME"`
			Password     string `yaml:"password" envconfig:"DATABASE_PGSQL_WRITER_PASSWORD"`
			Name         string `yaml:"name" envconfig:"DATABASE_PGSQL_WRITER_NAME"`
			Host         string `yaml:"host" envconfig:"DATABASE_PGSQL_WRITER_HOST"`
			Port         string `yaml:"port" envconfig:"DATABASE_PGSQL_WRITER_PORT"`
			MaxOpenConns int    `yaml:"maxOpenConns" envconfig:"DATABASE_PGSQL_WRITER_MAX_OPEN_CONNS"`
			MaxIdleConns int    `yaml:"maxIdleConns" envconfig:"DATABASE_PGSQL_WRITER_MAX_IDLE_CONNS"`
		} `yaml:"pgsqlWriter"`
	} `yaml:"database"`

	KillSwitch struct {
		DisableSSZEncoding      bool `yaml:"disableSSZEncoding" envconfig:"KILLSWITCH_DISABLE_SSZ_ENCODING"`
		DisableSSZRequests      bool `yaml:"disableSSZRequests" envconfig:"KILLSWITCH_DISABLE_SSZ_REQUESTS"`
		DisableBlockCompression bool `yaml:"disableBlockCompression" envconfig:"KILLSWITCH_DISABLE_BLOCK_COMPRESSION"`
	} `yaml:"killSwitch"`
}

type EndpointConfig struct {
	Ssh            *EndpointSshConfig `yaml:"ssh"`
	Url            string             `yaml:"url"`
	Name           string             `yaml:"name"`
	Archive        bool               `yaml:"archive"`
	SkipValidators bool               `yaml:"skipValidators"`
	Priority       int                `yaml:"priority"`
	Headers        map[string]string  `yaml:"headers"`
}

type EndpointSshConfig struct {
	Host     string `yaml:"host"`
	Port     string `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Keyfile  string `yaml:"keyfile"`
}

type MevRelayConfig struct {
	Index      uint8  `yaml:"index"`
	Name       string `yaml:"name"`
	Url        string `yaml:"url"`
	BlockLimit int    `yaml:"blockLimit"`
}

type SqliteDatabaseConfig struct {
	File         string
	MaxOpenConns int
	MaxIdleConns int
}

type PgsqlDatabaseConfig struct {
	Username     string
	Password     string
	Name         string
	Host         string
	Port         string
	MaxOpenConns int
	MaxIdleConns int
}
