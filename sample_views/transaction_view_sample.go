//go:build fabricx

package localapps

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hyperledger-labs/fabric-smart-client/platform/common/utils/assert"
	"github.com/hyperledger-labs/fabric-smart-client/platform/fabric"
	"github.com/hyperledger-labs/fabric-smart-client/platform/fabric/services/state"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/services/id"
	svcview "github.com/hyperledger-labs/fabric-smart-client/platform/view/services/view"
	"github.com/hyperledger-labs/fabric-smart-client/platform/view/view"
)

// ChainNSConfig holds the chain namespace metadata loaded from a configmap.
// Mount the configmap file at chainNSConfigPath (e.g. via k8s ConfigMap volume)
// with shape: { "namespace": "test-namespace", "channel": "arma" }
type ChainNSConfig struct {
	Namespace string `json:"namespace"`
	Channel   string `json:"channel"`
}

// chainNSConfigPath is the on-disk location of the chainNS configmap payload.
// Override at build time with -ldflags "-X ...chainNSConfigPath=/path".
const chainNSConfigPath = "/conf/chainNS.json"

// loadChainNSConfig reads and parses the chainNS configmap file.
func loadChainNSConfig() (*ChainNSConfig, error) {
	data, err := os.ReadFile(chainNSConfigPath)
	if err != nil {
		return nil, fmt.Errorf("read chainNS config %s: %w", chainNSConfigPath, err)
	}
	cfg := &ChainNSConfig{}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse chainNS config %s: %w", chainNSConfigPath, err)
	}
	if cfg.Namespace == "" {
		return nil, fmt.Errorf("chainNS config %s: namespace is empty", chainNSConfigPath)
	}
	return cfg, nil
}

// Record is a simple key-value state stored on the Fabric-X ledger.
type Record struct {
	LinearID string        `json:"linear_id"`
	Key      string        `json:"key"`
	Value    string        `json:"value"`
	Owner    view.Identity `json:"owner"`
}

func (r *Record) SetLinearID(id string) string {
	if len(r.LinearID) == 0 {
		r.LinearID = id
	}
	return r.LinearID
}

func (r *Record) Owners() state.Identities {
	return []view.Identity{r.Owner}
}

// StoreRecordView submits a key-value record to the Fabric-X ledger.
// A single party (this node) self-endorses. No multi-party orchestration.
type StoreRecordView struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (v *StoreRecordView) Call(ctx view.Context) (any, error) {
	fns, err := fabric.GetDefaultFNS(ctx)
	assert.NoError(err, "failed getting default fabric network service")

	me := fns.IdentityProvider().DefaultIdentity()

	nsCfg, err := loadChainNSConfig()
	assert.NoError(err, "failed loading chainNS config")
	log.Printf("[StoreRecord] chainNS config loaded: path=%s namespace=%s channel=%s",
		chainNSConfigPath, nsCfg.Namespace, nsCfg.Channel)

	record := &Record{
		Key:   v.Key,
		Value: v.Value,
		Owner: me,
	}

	tx, err := state.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed creating transaction: %w", err)
	}

	tx.SetNamespace(nsCfg.Namespace)
	tx.AddCommand("store_record", me)
	tx.AddOutput(record)

	result, err := ctx.RunView(state.NewCollectEndorsementsView(tx, me))
	fmt.Println(result, "endorsements results")
	if err != nil {
		return nil, fmt.Errorf("failed collecting endorsements: %w", err)
	}

	_, err = ctx.RunView(state.NewOrderingAndFinalityWithTimeoutView(tx, 5*time.Minute))
	if err != nil {
		return map[string]string{
			"linear_id": record.LinearID,
			"key":       record.Key,
			"value":     record.Value,
			"status":    "unknown",
			"error":     err.Error(),
		}, nil
	}

	return map[string]string{
		"linear_id": record.LinearID,
		"key":       record.Key,
		"value":     record.Value,
		// "owner":     string(record.Owner),
	}, nil
}

type StoreRecordViewFactory struct{}

func (f *StoreRecordViewFactory) NewView(in []byte) (view.View, error) {
	v := &StoreRecordView{}
	if err := json.Unmarshal(in, v); err != nil {
		return nil, fmt.Errorf("failed to unmarshal StoreRecordView: %w", err)
	}
	return v, nil
}

// QueryRecordView reads a record from the ledger by its linear ID.
type QueryRecordView struct {
	LinearID string `json:"linear_id"`
}

func (v *QueryRecordView) Call(ctx view.Context) (any, error) {
	vault, err := state.GetVault(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed getting vault: %w", err)
	}

	nsCfg, err := loadChainNSConfig()
	if err != nil {
		return nil, fmt.Errorf("failed loading chainNS config: %w", err)
	}
	log.Printf("[QueryRecord] chainNS config loaded: path=%s namespace=%s channel=%s",
		chainNSConfigPath, nsCfg.Namespace, nsCfg.Channel)

	record := &Record{}
	if err := vault.GetState(ctx.Context(), nsCfg.Namespace, v.LinearID, record); err != nil {
		return nil, fmt.Errorf("record %s not found: %w", v.LinearID, err)
	}
	return record, nil
}

type QueryRecordViewFactory struct{}

func (f *QueryRecordViewFactory) NewView(in []byte) (view.View, error) {
	v := &QueryRecordView{}
	if err := json.Unmarshal(in, v); err != nil {
		return nil, fmt.Errorf("failed to unmarshal QueryRecordView: %w", err)
	}
	return v, nil
}

// PingView sends a "ping" message to another FSC node and waits for a "pong".
// This tests P2P session connectivity without touching the ledger.
type PingView struct {
	Target string `json:"target"`
}

func (v *PingView) Call(ctx view.Context) (any, error) {
	idProvider, err := id.GetProvider(ctx)
	assert.NoError(err, "failed getting identity provider")

	target := idProvider.Identity(v.Target)

	session, err := ctx.GetSession(ctx.Initiator(), target)
	assert.NoError(err, "failed opening session to %s", v.Target)

	err = session.Send([]byte("ping"))
	assert.NoError(err, "failed sending ping")

	ch := session.Receive()
	select {
	case msg := <-ch:
		if msg.Status == view.ERROR {
			return nil, fmt.Errorf("ping failed: %s", string(msg.Payload))
		}
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("no pong response from %s within timeout", v.Target)
	}

	return map[string]string{"target": v.Target, "result": "pong"}, nil
}

type PingViewFactory struct{}

func (f *PingViewFactory) NewView(in []byte) (view.View, error) {
	v := &PingView{}
	if err := json.Unmarshal(in, v); err != nil {
		return nil, fmt.Errorf("failed to unmarshal PingView: %w", err)
	}
	return v, nil
}

// PongResponderView responds to an incoming PingView from another FSC node.
// Register this as a responder so other nodes can ping this one.
type PongResponderView struct{}

func (v *PongResponderView) Call(ctx view.Context) (any, error) {
	session := ctx.Session()

	ch := session.Receive()
	select {
	case msg := <-ch:
		if msg.Status == view.ERROR {
			return nil, fmt.Errorf("received error: %s", string(msg.Payload))
		}
		if string(msg.Payload) != "ping" {
			err := session.SendError([]byte(fmt.Sprintf("expected ping, got %s", string(msg.Payload))))
			assert.NoError(err, "failed sending error")
			return nil, fmt.Errorf("expected ping, got %s", string(msg.Payload))
		}
		err := session.Send([]byte("pong"))
		assert.NoError(err, "failed sending pong")
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("no ping received within timeout")
	}

	return "OK", nil
}

type PongResponderViewFactory struct{}

func (f *PongResponderViewFactory) NewView(in []byte) (view.View, error) {
	return &PongResponderView{}, nil
}

// GetLedgerInfoView queries the Fabric-X ledger for block height and current block hash.
// This tests ledger connectivity without writing any transactions.
type GetLedgerInfoView struct{}

func (v *GetLedgerInfoView) Call(ctx view.Context) (any, error) {
	_, ch, err := fabric.GetDefaultChannel(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed getting default channel: %w", err)
	}

	info, err := ch.Ledger().GetLedgerInfo()
	if err != nil {
		return nil, fmt.Errorf("failed getting ledger info: %w", err)
	}

	return map[string]any{
		"height":              info.Height,
		"current_block_hash":  fmt.Sprintf("%x", info.CurrentBlockHash),
		"previous_block_hash": fmt.Sprintf("%x", info.PreviousBlockHash),
	}, nil
}

type GetLedgerInfoViewFactory struct{}

func (f *GetLedgerInfoViewFactory) NewView(in []byte) (view.View, error) {
	return &GetLedgerInfoView{}, nil
}

// GetTxStatusView checks the commit status of a previously submitted transaction.
// This tests that the committer and finality service are reachable.
type GetTxStatusView struct {
	TxID string `json:"tx_id"`
}

func (v *GetTxStatusView) Call(ctx view.Context) (any, error) {
	_, ch, err := fabric.GetDefaultChannel(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed getting default channel: %w", err)
	}

	status, message, err := ch.Committer().Status(ctx.Context(), v.TxID)
	if err != nil {
		return nil, fmt.Errorf("failed getting tx status: %w", err)
	}

	return map[string]any{
		"tx_id":   v.TxID,
		"status":  status,
		"message": message,
	}, nil
}

type GetTxStatusViewFactory struct{}

func (f *GetTxStatusViewFactory) NewView(in []byte) (view.View, error) {
	v := &GetTxStatusView{}
	if err := json.Unmarshal(in, v); err != nil {
		return nil, fmt.Errorf("failed to unmarshal GetTxStatusView: %w", err)
	}
	return v, nil
}

// RecordInput is a single record within a batch submission.
type RecordInput struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// StoreBatchView submits multiple key-value records in a single transaction.
// Creates one transaction with N outputs, one endorsement, one ordering.
type StoreBatchView struct {
	Records []RecordInput `json:"records"`
}

func (v *StoreBatchView) Call(ctx view.Context) (any, error) {
	fns, err := fabric.GetDefaultFNS(ctx)
	assert.NoError(err, "failed getting default fabric network service")

	me := fns.IdentityProvider().DefaultIdentity()

	nsCfg, err := loadChainNSConfig()
	assert.NoError(err, "failed loading chainNS config")
	log.Printf("[StoreBatch] chainNS config loaded: path=%s namespace=%s channel=%s batch_size=%d",
		chainNSConfigPath, nsCfg.Namespace, nsCfg.Channel, len(v.Records))

	tx, err := state.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed creating transaction: %w", err)
	}

	tx.SetNamespace(nsCfg.Namespace)
	tx.AddCommand("store_records", me)

	records := make([]*Record, len(v.Records))
	for i, r := range v.Records {
		records[i] = &Record{
			Key:   r.Key,
			Value: r.Value,
			Owner: me,
		}
		tx.AddOutput(records[i])
	}

	result, err := ctx.RunView(state.NewCollectEndorsementsView(tx, me))
	log.Printf("[StoreBatch] endorsements result: %v", result)
	if err != nil {
		return nil, fmt.Errorf("failed collecting endorsements: %w", err)
	}

	_, err = ctx.RunView(state.NewOrderingAndFinalityWithTimeoutView(tx, 5*time.Minute))
	if err != nil {
		return nil, fmt.Errorf("failed ordering and finality: %w", err)
	}

	type recordResult struct {
		Key      string `json:"key"`
		LinearID string `json:"linear_id"`
	}
	results := make([]recordResult, len(records))
	for i, rec := range records {
		results[i] = recordResult{
			Key:      rec.Key,
			LinearID: rec.LinearID,
		}
	}
	return map[string]any{
		"status":  "committed",
		"count":   len(results),
		"records": results,
	}, nil
}

type StoreBatchViewFactory struct{}

func (f *StoreBatchViewFactory) NewView(in []byte) (view.View, error) {
	v := &StoreBatchView{}
	if err := json.Unmarshal(in, v); err != nil {
		return nil, fmt.Errorf("failed to unmarshal StoreBatchView: %w", err)
	}
	return v, nil
}

// RegisterTransactionViews registers all simple transaction views.
func RegisterTransactionViews(sp *svcview.Registry) error {
	if err := sp.RegisterFactory("StoreRecord", &StoreRecordViewFactory{}); err != nil {
		return fmt.Errorf("failed to register StoreRecordView: %w", err)
	}
	if err := sp.RegisterFactory("StoreBatch", &StoreBatchViewFactory{}); err != nil {
		return fmt.Errorf("failed to register StoreBatchView: %w", err)
	}
	if err := sp.RegisterFactory("QueryRecord", &QueryRecordViewFactory{}); err != nil {
		return fmt.Errorf("failed to register QueryRecordView: %w", err)
	}
	if err := sp.RegisterFactory("Ping", &PingViewFactory{}); err != nil {
		return fmt.Errorf("failed to register PingView: %w", err)
	}
	if err := sp.RegisterResponderFactory(&PongResponderViewFactory{}, "Ping"); err != nil {
		return fmt.Errorf("failed to register PongResponderView: %w", err)
	}
	if err := sp.RegisterFactory("GetLedgerInfo", &GetLedgerInfoViewFactory{}); err != nil {
		return fmt.Errorf("failed to register GetLedgerInfoView: %w", err)
	}
	if err := sp.RegisterFactory("GetTxStatus", &GetTxStatusViewFactory{}); err != nil {
		return fmt.Errorf("failed to register GetTxStatusView: %w", err)
	}
	return nil
}
