package etcd

import (
	"os"
	"testing"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/testutils"
	"github.com/stretchr/testify/assert"
)

const (
	defaultEndpoint = "localhost:4001"
	endpointEnvVar  = "LIBKV_TEST_ETCD_ENDPOINT"
)

// makeEtcdClient makes etcd client for use in tests. It uses
// defaultEndpoint, unless LIBKV_TEST_ETCD_ENDPOINT environment
// variable is specified, in which case it is used.
func makeEtcdClient(t *testing.T) store.Store {
	endpoint := os.Getenv(endpointEnvVar)
	if endpoint == "" {
		endpoint = defaultEndpoint
	}

	t.Logf("Using ETCD endpoint %s", endpoint)
	kv, err := New(
		[]string{endpoint},
		&store.Config{
			ConnectionTimeout: 3 * time.Second,
			Username:          "test",
			Password:          "very-secure",
		},
	)

	if err != nil {
		t.Fatalf("cannot create store: %v", err)
	}

	return kv
}

func TestRegister(t *testing.T) {
	Register()
	endpoint := os.Getenv(endpointEnvVar)
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	t.Logf("Using ETCD endpoint %s", endpoint)
	kv, err := libkv.NewStore(store.ETCD, []string{endpoint}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, kv)

	if _, ok := kv.(*Etcd); !ok {
		t.Fatal("Error registering and initializing etcd")
	}
}

func TestEtcdStoreConcurrentLock(t *testing.T) {
	kv := makeEtcdClient(t)
	testutils.RunTestLockConcurrent(t, kv)
	testutils.RunCleanup(t, kv)
}

func TestEtcdStore(t *testing.T) {
	kv := makeEtcdClient(t)
	lockKV := makeEtcdClient(t)
	ttlKV := makeEtcdClient(t)

	testutils.RunTestCommon(t, kv)
	testutils.RunTestAtomic(t, kv)
	testutils.RunTestWatch(t, kv)
	testutils.RunTestLock(t, kv)
	testutils.RunTestLockTTL(t, kv, lockKV)
	testutils.RunTestTTL(t, kv, ttlKV)
	testutils.RunCleanup(t, kv)
}
