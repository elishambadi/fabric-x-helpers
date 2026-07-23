// This is the config used in sample_fabric_x_runner.go. It is a sample config for a 4-party Fabric X network with PostgreSQL committer.
package bootstrap

import "time"

// Config holds parameters for bootstrap.
type Config struct {
	// Namespace to deploy resources into (default: "fabric-x").
	Namespace string

	// CAName is the CA resource name (default: "bootstrap-ca").
	CAName string

	// ChannelID for the genesis block (default: "arma").
	ChannelID string

	// Parties is the number of orderer groups to create (1 or 4, default: 1).
	Parties int

	// SkipCommitter skips committer creation (requires PostgreSQL/CNPG).
	SkipCommitter bool

	// Timeout per phase (default: 10 minutes).
	PhaseTimeout time.Duration

	// PostgreSQL configuration for committer validator and query-service.
	PostgresHost                    string
	PostgresPort                    int32
	PostgresDatabase                string
	PostgresUser                    string
	PostgresPasswordSecretName      string
	PostgresPasswordSecretNamespace string

	// Connection pool settings for PostgreSQL.
	MaxConnections      int
	MinConnections      int
	MaxElapsedRetryTime string

	// TopologyFilePath points to a YAML topology config.
	// When set, all other Config fields are ignored and the topology file
	// drives the entire bootstrap.
	TopologyFilePath string

	// Image overrides.
	CAImage          string
	CAVersion        string
	OrdererImage     string
	OrdererVersion   string
	CommitterImage   string
	CommitterVersion string

	// Storage class for PVCs.
	StorageClass string

	// Storage size for assembler PVCs.
	StorageSize string

	// Storage size for consenter PVCs.
	ConsenterStorageSize string

	// Storage size for batcher PVCs.
	BatcherStorageSize string

	// Number of batcher shards per party.
	ShardsPerParty int

	// Orderer component replica counts.
	OrdererRouterReplicas    int
	OrdererConsenterReplicas int
	OrdererAssemblerReplicas int
	OrdererBatcherReplicas   int

	// Committer component replica counts.
	CommitterCoordinatorReplicas  int
	CommitterSidecarReplicas      int
	CommitterValidatorReplicas    int
	CommitterVerifierReplicas     int
	CommitterQueryServiceReplicas int

	// MSPID for the organization (default: "Org1MSP").
	MSPID string

	// OrgName for the organization (default: "Org1").
	OrgName string
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Namespace:                       "fabric-x",
		CAName:                          "bootstrap-ca",
		ChannelID:                       "arma",
		Parties:                         4,
		SkipCommitter:                   false,
		PhaseTimeout:                    10 * time.Minute,
		PostgresPort:                    5432,
		PostgresDatabase:                "fabricx",
		PostgresUser:                    "fabricx",
		PostgresPasswordSecretName:      "fabric-x-postgres-app",
		PostgresPasswordSecretNamespace: "fabric-x",
		MaxConnections:                  80,
		MinConnections:                  10,
		MaxElapsedRetryTime:             "1h",
		CAImage:                         "hyperledger/fabric-ca",
		CAVersion:                       "1.5.15",
		OrdererImage:                    "hyperledger/fabric-x-orderer",
		OrdererVersion:                  "1.0.0",
		CommitterImage:                  "hyperledger/fabric-x-committer",
		CommitterVersion:                "1.0.0",
		StorageClass:                    "local-path",
		StorageSize:                     "10Gi",
		ConsenterStorageSize:            "10Gi",
		BatcherStorageSize:              "10Gi",
		ShardsPerParty:                  1,
		OrdererRouterReplicas:           1,
		OrdererConsenterReplicas:        1,
		OrdererAssemblerReplicas:        1,
		OrdererBatcherReplicas:          1,
		CommitterCoordinatorReplicas:    1,
		CommitterSidecarReplicas:        1,
		CommitterValidatorReplicas:      1,
		CommitterVerifierReplicas:       1,
		CommitterQueryServiceReplicas:   1,
		MSPID:                           "Org1MSP",
		OrgName:                         "Org1",
	}
}
