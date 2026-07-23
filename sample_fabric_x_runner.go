package bootstrap

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/kfsoftware/fabric-x-operator/api/v1alpha1"
	"github.com/kfsoftware/fabric-x-operator/internal/controller/certs"
	"github.com/rs/zerolog/log"
)

// Runner applies a Fabric X network bootstrap sequence.
// It is idempotent — existing resources are skipped.
type Runner struct {
	client client.Client
	config Config
}

// NewRunner creates a bootstrap Runner.
func NewRunner(c client.Client, cfg Config) *Runner {
	return &Runner{client: c, config: cfg}
}

// Start runs the bootstrap sequence. Implements manager.Runnable.
func (r *Runner) Start(ctx context.Context) error {
	if r.config.TopologyFilePath != "" {
		// This LoadTopology function reads a YAML topology file and converts it into a Config struct. It allows users to define the entire bootstrap configuration in a single file, overriding any other Config fields.

		// You can read the sample sampel_topology.yaml in the project for an example of how to structure the topology file. The LoadTopology function will parse that file and populate the Config struct accordingly.
		tc, err := LoadTopology(r.config.TopologyFilePath)
		if err != nil {
			return fmt.Errorf("load topology: %w", err)
		}
		r.config = tc.ToConfig()
		log.Info("loaded topology config", "path", r.config.TopologyFilePath)
	}

	log.Info("starting network bootstrap",
		"namespace", r.config.Namespace,
		"ca", r.config.CAName,
		"parties", r.config.Parties,
		"channel", r.config.ChannelID,
	)

	if err := r.phaseDeployCA(ctx); err != nil {
		return fmt.Errorf("CA: %w", err)
	}
	if err := r.phaseOrdererConfigure(ctx); err != nil {
		return fmt.Errorf("orderer-configure: %w", err)
	}
	if err := r.phaseGenesis(ctx); err != nil {
		return fmt.Errorf("genesis: %w", err)
	}
	if err := r.phaseOrdererDeploy(ctx); err != nil {
		return fmt.Errorf("orderer-deploy: %w", err)
	}
	if !r.config.SkipCommitter {
		if err := r.phaseCommitter(ctx); err != nil {
			return fmt.Errorf("committer: %w", err)
		}
	}
	if err := r.phaseIdentityAndNamespace(ctx); err != nil {
		return fmt.Errorf("identity/namespace: %w", err)
	}

	log.Info("bootstrap complete")
	return nil
}

// --- Phase: CA ---

func (r *Runner) phaseDeployCA(ctx context.Context) error {
	log.Info("deploying CA", "name", r.config.CAName)

	ca := &v1alpha1.CA{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.config.CAName,
			Namespace: r.config.Namespace,
		},
		Spec: v1alpha1.CASpec{
			Image:   r.config.CAImage,
			Version: r.config.CAVersion,
			CA: v1alpha1.FabricCAItemConf{
				Name: "ca",
				Registry: v1alpha1.FabricCAItemRegistry{
					MaxEnrollments: -1,
					Identities: []v1alpha1.FabricCAIdentity{
						{
							Name:        "admin",
							Pass:        "adminpw",
							Type:        "client",
							Affiliation: "",
							Attrs: v1alpha1.FabricCAIdentityAttrs{
								RegistrarRoles: "*",
								DelegateRoles:  "*",
								Attributes:     "*",
								Revoker:        true,
								IntermediateCA: true,
								GenCRL:         true,
								AffiliationMgr: true,
							},
						},
					},
				},
			},
			TLSCA: v1alpha1.FabricCAItemConf{
				Name: "tlsca",
				Registry: v1alpha1.FabricCAItemRegistry{
					MaxEnrollments: -1,
					Identities: []v1alpha1.FabricCAIdentity{
						{
							Name:        "admin",
							Pass:        "adminpw",
							Type:        "client",
							Affiliation: "",
							Attrs: v1alpha1.FabricCAIdentityAttrs{
								RegistrarRoles: "*",
								DelegateRoles:  "*",
								Attributes:     "*",
								Revoker:        true,
								IntermediateCA: true,
								GenCRL:         true,
								AffiliationMgr: true,
							},
						},
					},
				},
			},
			TLS: v1alpha1.FabricCATLS{
				Domains: []string{
					r.config.CAName,
					r.config.CAName + "." + r.config.Namespace,
					r.config.CAName + "." + r.config.Namespace + ".svc.cluster.local",
					"localhost",
				},
			},
			Service: v1alpha1.FabricCASpecService{
				ServiceType: corev1.ServiceTypeClusterIP,
			},
		},
	}

	if err := r.applyCA(ctx, ca); err != nil {
		return err
	}

	log.Info("waiting for CA RUNNING")
	if err := r.waitForCAStatus(ctx, r.config.CAName, "RUNNING", r.config.PhaseTimeout); err != nil {
		return err
	}

	log.Info("verifying CA crypto secrets")
	return r.waitForSecrets(ctx, []string{
		r.config.CAName + "-tls-crypto",
		r.config.CAName + "-msp-crypto",
		r.config.CAName + "-tlsca-crypto",
	}, r.config.PhaseTimeout)
}

// --- Phase: Orderer Configure ---

func (r *Runner) phaseOrdererConfigure(ctx context.Context) error {
	genesisSecretName := r.config.CAName + "-genesis-block"

	for partyID := 1; partyID <= r.config.Parties; partyID++ {
		name := fmt.Sprintf("orderergroup-party%d", partyID)
		log.Info("deploying orderer group (configure)", "name", name)

		var batchers []v1alpha1.BatcherInstance
		for shardID := 1; shardID <= r.config.ShardsPerParty; shardID++ {
			batchers = append(batchers, v1alpha1.BatcherInstance{
				ShardID:               int32(shardID),
				CommonComponentConfig: v1alpha1.CommonComponentConfig{Replicas: int32(r.config.OrdererBatcherReplicas)},
				PVCStorageSize:        r.config.BatcherStorageSize,
				SANS: &v1alpha1.SANSConfig{
					DNSNames: []string{
						fmt.Sprintf("%s-batcher-%d.%s.svc.cluster.local", name, shardID-1, r.config.Namespace),
					},
				},
			})
		}

		og := &v1alpha1.OrdererGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: r.config.Namespace,
			},
			Spec: v1alpha1.OrdererGroupSpec{
				BootstrapMode: "configure",
				MSPID:         r.config.MSPID,
				PartyID:       int32(partyID),
				Image:         r.config.OrdererImage,
				ImageTag:      r.config.OrdererVersion,
				Common: &v1alpha1.CommonComponentConfig{
					Replicas: 1,
					PodLabels: map[string]string{
						"app.kubernetes.io/component": "fabric-x",
					},
				},
				Components: v1alpha1.OrdererComponents{
					Assembler: &v1alpha1.ComponentConfig{
						CommonComponentConfig: v1alpha1.CommonComponentConfig{
							Replicas: int32(r.config.OrdererAssemblerReplicas),
							Storage: &v1alpha1.StorageConfig{
								Size:         r.config.StorageSize,
								AccessMode:   "ReadWriteOnce",
								StorageClass: r.config.StorageClass,
							},
						},
						SANS: &v1alpha1.SANSConfig{
							DNSNames: []string{
								name + "-assembler." + r.config.Namespace + ".svc.cluster.local",
							},
						},
					},
					Batchers: batchers,
					Consenter: &v1alpha1.ConsenterInstance{
						ConsenterID:           int32(partyID),
						CommonComponentConfig: v1alpha1.CommonComponentConfig{Replicas: int32(r.config.OrdererConsenterReplicas)},
						PVCStorageSize:        r.config.ConsenterStorageSize,
						SANS: &v1alpha1.SANSConfig{
							DNSNames: []string{
								name + "-consenter." + r.config.Namespace + ".svc.cluster.local",
							},
						},
						Endpoints: []string{
							name + "-consenter." + r.config.Namespace + ".svc.cluster.local:7050",
						},
					},
					Router: &v1alpha1.ComponentConfig{
						CommonComponentConfig: v1alpha1.CommonComponentConfig{Replicas: int32(r.config.OrdererRouterReplicas)},
						SANS: &v1alpha1.SANSConfig{
							DNSNames: []string{
								name + "-router-service." + r.config.Namespace + ".svc.cluster.local",
							},
						},
					},
				},
				Enrollment: &v1alpha1.EnrollmentConfig{
					Sign: r.buildSignCertConfig(),
					TLS:  r.buildTLSCertConfig(),
				},
				Genesis: v1alpha1.GenesisConfig{
					SecretName:      genesisSecretName,
					SecretKey:       "genesis.block",
					SecretNamespace: r.config.Namespace,
				},
			},
		}

		if err := r.applyOrdererGroup(ctx, og); err != nil {
			return err
		}

		log.Info("waiting for orderer group RUNNING", "name", name)
		if err := r.waitForOrdererGroupStatus(ctx, name, "RUNNING", r.config.PhaseTimeout); err != nil {
			return err
		}
	}

	log.Info("verifying certificate secrets")
	comp := []string{"router", "consenter", "assembler"}
	// Add batcher secrets for each shard
	for shardID := 0; shardID < r.config.ShardsPerParty; shardID++ {
		comp = append(comp, fmt.Sprintf("batcher-%d", shardID))
	}
	var secrets []string
	for _, c := range comp {
		secrets = append(secrets,
			fmt.Sprintf("orderergroup-party1-%s-sign-cert", c),
			fmt.Sprintf("orderergroup-party1-%s-tls-cert", c),
		)
	}
	return r.waitForSecrets(ctx, secrets, r.config.PhaseTimeout)
}

// --- Phase: Genesis ---

func (r *Runner) phaseGenesis(ctx context.Context) error {
	genesisName := r.config.CAName + "-genesis"
	genesisSecretName := r.config.CAName + "-genesis-block"

	log.Info("deploying genesis block", "name", genesisName)

	genesis := &v1alpha1.Genesis{
		ObjectMeta: metav1.ObjectMeta{
			Name:      genesisName,
			Namespace: r.config.Namespace,
		},
		Spec: v1alpha1.GenesisSpec{
			ChannelID: r.config.ChannelID,
			OrdererOrganizations: []v1alpha1.OrdererOrganization{
				{
					Name:  r.config.MSPID,
					MSPID: r.config.MSPID,
					SignCACertRef: v1alpha1.SecretKeyNSSelector{
						Name:      r.config.CAName + "-msp-crypto",
						Namespace: r.config.Namespace,
						Key:       "certfile",
					},
					TLSCACertRef: v1alpha1.SecretKeyNSSelector{
						Name:      r.config.CAName + "-tlsca-crypto",
						Namespace: r.config.Namespace,
						Key:       "certfile",
					},
					Endpoints: r.buildEndpoints(),
					Router: &v1alpha1.RouterConfig{
						Host:    "orderergroup-party1-router-service." + r.config.Namespace + ".svc.cluster.local",
						Port:    7150,
						PartyID: 1,
						SignCertRef: v1alpha1.SecretKeyNSSelector{
							Name:      "orderergroup-party1-router-sign-cert",
							Namespace: r.config.Namespace,
							Key:       "cert.pem",
						},
						TLSCertRef: v1alpha1.SecretKeyNSSelector{
							Name:      "orderergroup-party1-router-tls-cert",
							Namespace: r.config.Namespace,
							Key:       "cert.pem",
						},
					},
					Assembler: &v1alpha1.AssemblerConfig{
						Host: "orderergroup-party1-assembler." + r.config.Namespace + ".svc.cluster.local",
						Port: 7050,
						TLSCertRef: v1alpha1.SecretKeyNSSelector{
							Name:      "orderergroup-party1-assembler-tls-cert",
							Namespace: r.config.Namespace,
							Key:       "cert.pem",
						},
					},
				},
			},
			ApplicationOrgs: []v1alpha1.ApplicationOrganization{
				{
					Name:  r.config.MSPID,
					MSPID: r.config.MSPID,
					SignCACertRef: v1alpha1.SecretKeyNSSelector{
						Name:      r.config.CAName + "-msp-crypto",
						Namespace: r.config.Namespace,
						Key:       "certfile",
					},
					TLSCACertRef: v1alpha1.SecretKeyNSSelector{
						Name:      r.config.CAName + "-tlsca-crypto",
						Namespace: r.config.Namespace,
						Key:       "certfile",
					},
				},
			},
			Consenters: r.buildConsenters(),
			Parties:    r.buildParties(),
			Output: v1alpha1.GenesisOutput{
				SecretName: genesisSecretName,
				BlockKey:   "genesis.block",
			},
		},
	}

	if err := r.applyGenesis(ctx, genesis); err != nil {
		return err
	}

	log.Info("waiting for genesis RUNNING")
	if err := r.waitForGenesisStatus(ctx, genesisName, "RUNNING", r.config.PhaseTimeout); err != nil {
		return err
	}

	return r.waitForSecrets(ctx, []string{genesisSecretName}, r.config.PhaseTimeout)
}

func (r *Runner) buildEndpoints() []string {
	var eps []string
	for i := 1; i <= r.config.Parties; i++ {
		prefix := fmt.Sprintf("orderergroup-party%d", i)
		eps = append(eps,
			fmt.Sprintf("id=%d,broadcast,%s-router-service.%s.svc.cluster.local:7150", i, prefix, r.config.Namespace),
			fmt.Sprintf("id=%d,deliver,%s-assembler.%s.svc.cluster.local:7050", i, prefix, r.config.Namespace),
		)
	}
	return eps
}

func (r *Runner) buildConsenters() []v1alpha1.OrdererNode {
	nodes := make([]v1alpha1.OrdererNode, 0, r.config.Parties)
	for i := 1; i <= r.config.Parties; i++ {
		prefix := fmt.Sprintf("orderergroup-party%d", i)
		nodes = append(nodes, v1alpha1.OrdererNode{
			ID:    i,
			MSPID: r.config.MSPID,
			Host:  prefix + "-consenter." + r.config.Namespace + ".svc.cluster.local",
			Port:  7052,
			IdentityRef: v1alpha1.SecretKeyNSSelector{
				Name:      prefix + "-consenter-sign-cert",
				Namespace: r.config.Namespace,
				Key:       "cert.pem",
			},
			ClientTLSCertRef: v1alpha1.SecretKeyNSSelector{
				Name:      prefix + "-consenter-tls-cert",
				Namespace: r.config.Namespace,
				Key:       "cert.pem",
			},
			ServerTLSCertRef: v1alpha1.SecretKeyNSSelector{
				Name:      prefix + "-consenter-tls-cert",
				Namespace: r.config.Namespace,
				Key:       "cert.pem",
			},
		})
	}
	return nodes
}

func (r *Runner) buildParties() []v1alpha1.PartyConfig {
	parties := make([]v1alpha1.PartyConfig, 0, r.config.Parties)
	for i := 1; i <= r.config.Parties; i++ {
		prefix := fmt.Sprintf("orderergroup-party%d", i)
		var batchers []v1alpha1.PartyBatcherConfig
		for shardID := 1; shardID <= r.config.ShardsPerParty; shardID++ {
			batcherSuffix := fmt.Sprintf("batcher-%d", shardID-1)
			batchers = append(batchers, v1alpha1.PartyBatcherConfig{
				ShardID: int32(shardID),
				Host:    fmt.Sprintf("%s-%s.%s.svc.cluster.local", prefix, batcherSuffix, r.config.Namespace),
				Port:    7151,
				SignCert: v1alpha1.SecretKeyNSSelector{
					Name:      fmt.Sprintf("%s-%s-sign-cert", prefix, batcherSuffix),
					Namespace: r.config.Namespace,
					Key:       "cert.pem",
				},
				TLSCert: v1alpha1.SecretKeyNSSelector{
					Name:      fmt.Sprintf("%s-%s-tls-cert", prefix, batcherSuffix),
					Namespace: r.config.Namespace,
					Key:       "cert.pem",
				},
			})
		}

		parties = append(parties, v1alpha1.PartyConfig{
			PartyID: int32(i),
			CACerts: []v1alpha1.SecretKeyNSSelector{
				{Name: r.config.CAName + "-msp-crypto", Namespace: r.config.Namespace, Key: "certfile"},
			},
			TLSCACerts: []v1alpha1.SecretKeyNSSelector{
				{Name: r.config.CAName + "-tlsca-crypto", Namespace: r.config.Namespace, Key: "certfile"},
			},
			RouterConfig: &v1alpha1.PartyRouterConfig{
				Host: prefix + "-router-service." + r.config.Namespace + ".svc.cluster.local",
				Port: 7150,
				TLSCert: v1alpha1.SecretKeyNSSelector{
					Name:      prefix + "-router-tls-cert",
					Namespace: r.config.Namespace,
					Key:       "cert.pem",
				},
			},
			BatchersConfig: batchers,
			ConsenterConfig: &v1alpha1.PartyConsenterConfig{
				Host: prefix + "-consenter." + r.config.Namespace + ".svc.cluster.local",
				Port: 7052,
				SignCert: v1alpha1.SecretKeyNSSelector{
					Name:      prefix + "-consenter-sign-cert",
					Namespace: r.config.Namespace,
					Key:       "cert.pem",
				},
				TLSCert: v1alpha1.SecretKeyNSSelector{
					Name:      prefix + "-consenter-tls-cert",
					Namespace: r.config.Namespace,
					Key:       "cert.pem",
				},
			},
			AssemblerConfig: &v1alpha1.PartyAssemblerConfig{
				Host: prefix + "-assembler." + r.config.Namespace + ".svc.cluster.local",
				Port: 7050,
				TLSCert: v1alpha1.SecretKeyNSSelector{
					Name:      prefix + "-assembler-tls-cert",
					Namespace: r.config.Namespace,
					Key:       "cert.pem",
				},
			},
		})
	}
	return parties
}

// Shared enrollment helpers

func (r *Runner) buildSignCertConfig() *v1alpha1.CertificateConfig {
	return &v1alpha1.CertificateConfig{
		CA: &v1alpha1.CACertificateConfig{
			CAName: "ca",
			CAHost: r.config.CAName + "." + r.config.Namespace,
			CAPort: 7054,
			CATLS: &v1alpha1.CATLSConfig{
				SecretRef: &v1alpha1.SecretRef{
					Name:      r.config.CAName + "-tls-crypto",
					Namespace: r.config.Namespace,
					Key:       "tls.crt",
				},
			},
			EnrollID:     "admin",
			EnrollSecret: "adminpw",
		},
	}
}

func (r *Runner) buildTLSCertConfig() *v1alpha1.CertificateConfig {
	return &v1alpha1.CertificateConfig{
		CA: &v1alpha1.CACertificateConfig{
			CAName: "tlsca",
			CAHost: r.config.CAName + "." + r.config.Namespace,
			CAPort: 7054,
			CATLS: &v1alpha1.CATLSConfig{
				SecretRef: &v1alpha1.SecretRef{
					Name:      r.config.CAName + "-tls-crypto",
					Namespace: r.config.Namespace,
					Key:       "tls.crt",
				},
			},
			EnrollID:     "admin",
			EnrollSecret: "adminpw",
		},
	}
}

// --- Phase: Orderer Deploy ---

func (r *Runner) phaseOrdererDeploy(ctx context.Context) error {
	for partyID := 1; partyID <= r.config.Parties; partyID++ {
		name := fmt.Sprintf("orderergroup-party%d", partyID)
		log.Info("switching orderer group to deploy", "name", name)

		og := &v1alpha1.OrdererGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: r.config.Namespace,
			},
		}
		patch := []byte(`{"spec":{"bootstrapMode":"deploy"}}`)
		if err := r.client.Patch(ctx, og, client.RawPatch(types.MergePatchType, patch)); err != nil {
			return fmt.Errorf("patch %s to deploy: %w", name, err)
		}
	}

	// Wait for orderer pods
	for i := 1; i <= r.config.Parties; i++ {
		prefix := fmt.Sprintf("orderergroup-party%d", i)
		log.Info("waiting for orderer pods", "party", i)

		if err := r.waitForPod(ctx, "ordererrouter", prefix+"-router"); err != nil {
			return err
		}
		if err := r.waitForPod(ctx, "release", prefix+"-consenter"); err != nil {
			return err
		}
		for shardID := 0; shardID < r.config.ShardsPerParty; shardID++ {
			if err := r.waitForPod(ctx, "release", fmt.Sprintf("%s-batcher-%d", prefix, shardID)); err != nil {
				return err
			}
		}
		if err := r.waitForPod(ctx, "ordererassembler", prefix+"-assembler"); err != nil {
			return err
		}
	}
	return nil
}

// --- Phase: Committer ---

func (r *Runner) phaseCommitter(ctx context.Context) error {
	committerName := r.config.CAName + "-committer"
	genesisSecretName := r.config.CAName + "-genesis-block"

	var endpoints []string
	for i := 1; i <= r.config.Parties; i++ {
		endpoints = append(endpoints,
			fmt.Sprintf("orderergroup-party%d-assembler.%s.svc.cluster.local:7050", i, r.config.Namespace))
	}

	postgresql := r.buildPostgreSQLConfig()

	log.Info("deploying committer", "name", committerName)

	committer := &v1alpha1.Committer{
		ObjectMeta: metav1.ObjectMeta{
			Name:      committerName,
			Namespace: r.config.Namespace,
		},
		Spec: v1alpha1.CommitterSpec{
			BootstrapMode: "deploy",
			MSPID:         r.config.MSPID,
			Image:         r.config.CommitterImage,
			ImageTag:      r.config.CommitterVersion,
			Common: &v1alpha1.CommonComponentConfig{
				Replicas: 1,
			},
			Genesis: v1alpha1.GenesisConfig{
				SecretName:      genesisSecretName,
				SecretKey:       "genesis.block",
				SecretNamespace: r.config.Namespace,
			},
			Components: v1alpha1.CommitterComponents{
				OrdererEndpoints: endpoints,
				CommitterHost:    committerName + "-coordinator-service." + r.config.Namespace + ".svc.cluster.local",
				CommitterPort:    9001,
				CoordinatorVerifierEndpoints: []string{
					committerName + "-verifier-service." + r.config.Namespace + ".svc.cluster.local:5001",
				},
				CoordinatorValidatorCommitterEndpoints: []string{
					committerName + "-validator-service." + r.config.Namespace + ".svc.cluster.local:6001",
				},
				Coordinator: &v1alpha1.ComponentConfig{
					CommonComponentConfig: v1alpha1.CommonComponentConfig{Replicas: int32(r.config.CommitterCoordinatorReplicas)},
				},
				Sidecar: &v1alpha1.ComponentConfig{
					CommonComponentConfig: v1alpha1.CommonComponentConfig{Replicas: int32(r.config.CommitterSidecarReplicas)},
					Env: []corev1.EnvVar{
						{Name: "SC_SIDECAR_ORDERER_CHANNEL_ID", Value: r.config.ChannelID},
					},
				},
				Validator: &v1alpha1.ValidatorComponentConfig{
					CommonComponentConfig: v1alpha1.CommonComponentConfig{Replicas: int32(r.config.CommitterValidatorReplicas)},
					PostgreSQL:            postgresql,
				},
				VerifierService: &v1alpha1.ComponentConfig{
					CommonComponentConfig: v1alpha1.CommonComponentConfig{Replicas: int32(r.config.CommitterVerifierReplicas)},
				},
				QueryService: &v1alpha1.ComponentConfig{
					CommonComponentConfig: v1alpha1.CommonComponentConfig{Replicas: int32(r.config.CommitterQueryServiceReplicas)},
					Command:               []string{"committer"},
					Args:                  []string{"start-query", "--config=/config/config.yaml"},
					PostgreSQL:            postgresql,
				},
			},
			Enrollment: &v1alpha1.EnrollmentConfig{
				Sign: r.buildSignCertConfig(),
				TLS:  r.buildTLSCertConfig(),
			},
		},
	}

	if err := r.applyCommitter(ctx, committer); err != nil {
		return err
	}

	log.Info("waiting for committer RUNNING")
	return r.waitForCommitterStatus(ctx, committerName, "RUNNING", r.config.PhaseTimeout)
}

func (r *Runner) buildPostgreSQLConfig() *v1alpha1.PostgreSQLConfig {
	if r.config.PostgresHost == "" {
		return nil
	}
	return &v1alpha1.PostgreSQLConfig{
		Host:     r.config.PostgresHost,
		Port:     r.config.PostgresPort,
		Database: r.config.PostgresDatabase,
		Username: r.config.PostgresUser,
		PasswordSecret: &v1alpha1.SecretRef{
			Name:      r.config.PostgresPasswordSecretName,
			Namespace: r.config.PostgresPasswordSecretNamespace,
			Key:       "password",
		},
		MaxConnections: int32(r.config.MaxConnections),
		MinConnections: int32(r.config.MinConnections),
		Retry: &v1alpha1.PostgreSQLRetryConfig{
			MaxElapsedTime: r.config.MaxElapsedRetryTime,
		},
	}
}

// --- Phase: Identity + ChainNamespace ---

func (r *Runner) phaseIdentityAndNamespace(ctx context.Context) error {
	adminName := r.config.CAName + "-admin"
	enrollSecretName := adminName + "-enroll"

	log.Info("creating admin enrollment secret", "name", enrollSecretName)
	if err := r.ensureSecret(ctx, enrollSecretName, map[string]string{
		"password": "adminpw",
	}); err != nil {
		return err
	}

	log.Info("creating admin identity", "name", adminName)
	identity := &v1alpha1.Identity{
		ObjectMeta: metav1.ObjectMeta{
			Name:      adminName,
			Namespace: r.config.Namespace,
		},
		Spec: v1alpha1.IdentitySpec{
			Type:  "admin",
			MspID: r.config.MSPID,
			Enrollment: &v1alpha1.IdentityEnrollment{
				CARef: v1alpha1.IdentityCARef{
					Name:      r.config.CAName,
					Namespace: r.config.Namespace,
				},
				EnrollID: "admin",
				EnrollSecretRef: v1alpha1.SecretKeyNSSelector{
					Name:      enrollSecretName,
					Namespace: r.config.Namespace,
					Key:       "password",
				},
			},
			Output: v1alpha1.IdentityOutput{
				SecretName: adminName + "-cert",
			},
		},
	}

	if err := r.applyIdentity(ctx, identity); err != nil {
		return err
	}

	log.Info("waiting for identity READY")
	if err := r.waitForIdentityStatus(ctx, adminName, "READY", r.config.PhaseTimeout); err != nil {
		return err
	}

	// Use a dedicated peer identity for ChainNamespace deployment so its
	// certificate has OU=peer and satisfies the genesis LifecycleEndorsement
	// policy under NodeOUs.
	peerName := r.config.CAName + "-peer"
	log.Info("registering peer identity for namespace deployment", "name", peerName)
	if err := r.registerPeerIdentity(ctx); err != nil {
		return err
	}

	log.Info("creating peer identity", "name", peerName)
	peerIdentity := &v1alpha1.Identity{
		ObjectMeta: metav1.ObjectMeta{
			Name:      peerName,
			Namespace: r.config.Namespace,
		},
		Spec: v1alpha1.IdentitySpec{
			Type:  "peer",
			MspID: r.config.MSPID,
			Enrollment: &v1alpha1.IdentityEnrollment{
				CARef: v1alpha1.IdentityCARef{
					Name:      r.config.CAName,
					Namespace: r.config.Namespace,
				},
				EnrollID: "peer",
				EnrollSecretRef: v1alpha1.SecretKeyNSSelector{
					Name:      peerName + "-enroll",
					Namespace: r.config.Namespace,
					Key:       "password",
				},
			},
			Output: v1alpha1.IdentityOutput{
				SecretName: peerName + "-cert",
			},
		},
	}
	if err := r.applyIdentity(ctx, peerIdentity); err != nil {
		return err
	}

	log.Info("waiting for peer identity READY")
	if err := r.waitForIdentityStatus(ctx, peerName, "READY", r.config.PhaseTimeout); err != nil {
		return err
	}

	ordererEndpoint := fmt.Sprintf("orderergroup-party1-router-service.%s.svc.cluster.local:7150", r.config.Namespace)
	log.Info("creating ChainNamespace", "name", r.config.CAName+"-ns")

	ns := &v1alpha1.ChainNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      r.config.CAName + "-ns",
			Namespace: r.config.Namespace,
		},
		Spec: v1alpha1.NamespaceSpec{
			Name:    "testnamespace",
			Orderer: ordererEndpoint,
			TLS: &v1alpha1.NamespaceTLS{
				Enabled: false,
				// Enabled: true,
				// Disable TLS for now
				// CACert: &v1alpha1.SecretKeyRef{
				// 	Name:      r.config.CAName + "-tls-crypto",
				// 	Namespace: r.config.Namespace,
				// 	Key:       "tls.crt",
				// },
			},
			MSPID: r.config.MSPID,
			Identity: v1alpha1.SecretKeyRef{
				Name:      peerName,
				Namespace: r.config.Namespace,
				Key:       "identity.pem",
			},
			Channel: r.config.ChannelID,
			Version: -1,
		},
	}
	return r.applyChainNamespace(ctx, ns)
}

// registerPeerIdentity registers a peer user with the Fabric CA using the
// bootstrap admin registrar credential. It is idempotent: if the peer user is
// already registered the error is ignored so operator restarts are safe.
func (r *Runner) registerPeerIdentity(ctx context.Context) error {
	caName := r.config.CAName
	peerEnrollSecretName := caName + "-peer-enroll"
	peerPassword := "peerpw"

	log.Info("creating peer enrollment secret", "name", peerEnrollSecretName)
	if err := r.ensureSecret(ctx, peerEnrollSecretName, map[string]string{
		"password": peerPassword,
	}); err != nil {
		return fmt.Errorf("peer enroll secret: %w", err)
	}

	// Fetch CA TLS certificate to trust the CA endpoint
	tlsCertSecret := &corev1.Secret{}
	if err := r.client.Get(ctx, client.ObjectKey{
		Name:      caName + "-tls-crypto",
		Namespace: r.config.Namespace,
	}, tlsCertSecret); err != nil {
		return fmt.Errorf("get CA TLS secret: %w", err)
	}
	tlsCert, ok := tlsCertSecret.Data["tls.crt"]
	if !ok {
		return fmt.Errorf("tls.crt not found in CA TLS secret %s/%s", r.config.Namespace, caName+"-tls-crypto")
	}

	log.Info("registering peer user with Fabric CA")
	_, err := certs.RegisterUser(ctx, r.client, certs.RegisterUserRequest{
		TLSCert:      string(tlsCert),
		URL:          fmt.Sprintf("https://%s.%s:7054", caName, r.config.Namespace),
		Name:         "ca",
		MSPID:        r.config.MSPID,
		EnrollID:     "admin",
		EnrollSecret: "adminpw",
		User:         "peer",
		Secret:       peerPassword,
		Type:         "peer",
		Attributes:   nil,
	})
	if err != nil && !strings.Contains(err.Error(), "already registered") {
		return fmt.Errorf("register peer user: %w", err)
	}
	if err != nil {
		log.Info("peer user already registered, continuing")
	}

	return nil
}

// --- Idempotent apply helpers ---

func (r *Runner) applyCA(ctx context.Context, ca *v1alpha1.CA) error {
	return r.apply(ctx, ca, &v1alpha1.CA{})
}

func (r *Runner) applyOrdererGroup(ctx context.Context, og *v1alpha1.OrdererGroup) error {
	return r.apply(ctx, og, &v1alpha1.OrdererGroup{})
}

func (r *Runner) applyGenesis(ctx context.Context, g *v1alpha1.Genesis) error {
	return r.apply(ctx, g, &v1alpha1.Genesis{})
}

func (r *Runner) applyCommitter(ctx context.Context, c *v1alpha1.Committer) error {
	return r.apply(ctx, c, &v1alpha1.Committer{})
}

func (r *Runner) applyIdentity(ctx context.Context, id *v1alpha1.Identity) error {
	return r.apply(ctx, id, &v1alpha1.Identity{})
}

func (r *Runner) applyChainNamespace(ctx context.Context, ns *v1alpha1.ChainNamespace) error {
	return r.apply(ctx, ns, &v1alpha1.ChainNamespace{})
}

func (r *Runner) apply(ctx context.Context, obj, existing client.Object) error {
	key := client.ObjectKeyFromObject(obj)
	err := r.client.Get(ctx, key, existing)
	if err == nil {
		log.Info("resource already exists, skipping", "name", key.Name)
		return nil
	}
	if !errors.IsNotFound(err) {
		return fmt.Errorf("get %s: %w", key.Name, err)
	}
	log.Info("creating resource", "name", key.Name)
	return r.client.Create(ctx, obj)
}

func (r *Runner) ensureConfigMap(ctx context.Context, name, key, data string) error {
	existing := &corev1.ConfigMap{}
	err := r.client.Get(ctx, client.ObjectKey{Name: name, Namespace: r.config.Namespace}, existing)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: r.config.Namespace,
		},
		Data: map[string]string{key: data},
	}
	return r.client.Create(ctx, cm)
}

func (r *Runner) ensureSecret(ctx context.Context, name string, data map[string]string) error {
	existing := &corev1.Secret{}
	err := r.client.Get(ctx, client.ObjectKey{Name: name, Namespace: r.config.Namespace}, existing)
	if err == nil {
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: r.config.Namespace,
		},
		StringData: data,
		Type:       corev1.SecretTypeOpaque,
	}
	return r.client.Create(ctx, secret)
}

// --- Status wait helpers ---

func (r *Runner) waitForCAStatus(ctx context.Context, name, expected string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		ca := &v1alpha1.CA{}
		if err := r.client.Get(ctx, client.ObjectKey{Name: name, Namespace: r.config.Namespace}, ca); err != nil {
			return false, nil
		}
		ok := string(ca.Status.Status) == expected
		if ok {
			log.Info("CA status OK", "status", string(ca.Status.Status))
		}
		return ok, nil
	})
}

func (r *Runner) waitForOrdererGroupStatus(ctx context.Context, name, expected string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		og := &v1alpha1.OrdererGroup{}
		if err := r.client.Get(ctx, client.ObjectKey{Name: name, Namespace: r.config.Namespace}, og); err != nil {
			return false, nil
		}
		ok := string(og.Status.Status) == expected
		if ok {
			log.Info("OrdererGroup status OK", "status", string(og.Status.Status))
		}
		return ok, nil
	})
}

func (r *Runner) waitForGenesisStatus(ctx context.Context, name, expected string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		g := &v1alpha1.Genesis{}
		if err := r.client.Get(ctx, client.ObjectKey{Name: name, Namespace: r.config.Namespace}, g); err != nil {
			return false, nil
		}
		ok := string(g.Status.Status) == expected
		if ok {
			log.Info("Genesis status OK", "status", string(g.Status.Status))
		}
		return ok, nil
	})
}

func (r *Runner) waitForCommitterStatus(ctx context.Context, name, expected string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		c := &v1alpha1.Committer{}
		if err := r.client.Get(ctx, client.ObjectKey{Name: name, Namespace: r.config.Namespace}, c); err != nil {
			return false, nil
		}
		ok := string(c.Status.Status) == expected
		if ok {
			log.Info("Committer status OK", "status", string(c.Status.Status))
		}
		return ok, nil
	})
}

func (r *Runner) waitForIdentityStatus(ctx context.Context, name, expected string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		id := &v1alpha1.Identity{}
		if err := r.client.Get(ctx, client.ObjectKey{Name: name, Namespace: r.config.Namespace}, id); err != nil {
			return false, nil
		}
		ok := id.Status.Status == expected
		if ok {
			log.Info("Identity status OK", "status", id.Status.Status)
		}
		return ok, nil
	})
}

func (r *Runner) waitForSecrets(ctx context.Context, names []string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, timeout, true, func(ctx context.Context) (bool, error) {
		for _, name := range names {
			secret := &corev1.Secret{}
			if err := r.client.Get(ctx, client.ObjectKey{Name: name, Namespace: r.config.Namespace}, secret); err != nil {
				return false, nil
			}
		}
		log.Info("all secrets ready", "count", len(names))
		return true, nil
	})
}

func (r *Runner) waitForPod(ctx context.Context, labelKey, labelValue string) error {
	return wait.PollUntilContextTimeout(ctx, 5*time.Second, r.config.PhaseTimeout, true, func(ctx context.Context) (bool, error) {
		pods := &corev1.PodList{}
		if err := r.client.List(ctx, pods,
			client.InNamespace(r.config.Namespace),
			client.MatchingLabels{labelKey: labelValue},
		); err != nil {
			return false, nil
		}
		if len(pods.Items) == 0 {
			return false, nil
		}
		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodRunning {
				return false, nil
			}
		}
		log.Info("pod running", "label", fmt.Sprintf("%s=%s", labelKey, labelValue))
		return true, nil
	})
}
