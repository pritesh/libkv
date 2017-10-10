package etcd

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/context"

	etcd "github.com/coreos/etcd/client"
	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
)

var (
	// ErrAbortTryLock is thrown when a user stops trying to seek the lock
	// by sending a signal to the stop chan, this is used to verify if the
	// operation succeeded
	ErrAbortTryLock = errors.New("lock operation aborted")
)

// Etcd is the receiver type for the
// Store interface
type Etcd struct {
	client etcd.KeysAPI
}

type etcdLock struct {
	client        etcd.KeysAPI
	stopLock      chan struct{}
	stopRenew     chan struct{}
	key           string
	value         string
	last          *etcd.Response
	ttl           time.Duration
	waitLockDelay time.Duration
}

const (
	periodicSync      = 5 * time.Minute
	defaultLockTTL    = 20 * time.Minute
	defaultUpdateTime = 5 * time.Second
)

// Register registers etcd to libkv
func Register() {
	libkv.AddStore(store.ETCD, New)
}

// New creates a new Etcd client given a list
// of endpoints and an optional tls config
func New(addrs []string, options *store.Config) (store.Store, error) {
	s := &Etcd{}

	var (
		entries []string
		err     error
	)

	entries = store.CreateEndpoints(addrs, "http")
	cfg := &etcd.Config{
		Endpoints:               entries,
		Transport:               etcd.DefaultTransport,
		HeaderTimeoutPerRequest: 3 * time.Second,
	}

	// Set options
	if options != nil {
		if options.TLS != nil {
			setTLS(cfg, options.TLS, addrs)
		}
		if options.ConnectionTimeout != 0 {
			setTimeout(cfg, options.ConnectionTimeout)
		}
		if options.Username != "" {
			setCredentials(cfg, options.Username, options.Password)
		}
	}

	c, err := etcd.New(*cfg)
	if err != nil {
		log.Fatal(err)
	}

	s.client = etcd.NewKeysAPI(c)

	// Periodic Cluster Sync
	go func() {
		for {
			if err := c.AutoSync(context.Background(), periodicSync); err != nil {
				return
			}
		}
	}()

	return s, nil
}

// SetTLS sets the tls configuration given a tls.Config scheme
func setTLS(cfg *etcd.Config, tls *tls.Config, addrs []string) {
	entries := store.CreateEndpoints(addrs, "https")
	cfg.Endpoints = entries

	// Set transport
	t := http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tls,
	}

	cfg.Transport = &t
}

// setTimeout sets the timeout used for connecting to the store
func setTimeout(cfg *etcd.Config, time time.Duration) {
	cfg.HeaderTimeoutPerRequest = time
}

// setCredentials sets the username/password credentials for connecting to Etcd
func setCredentials(cfg *etcd.Config, username, password string) {
	cfg.Username = username
	cfg.Password = password
}

// Normalize the key for usage in Etcd
func (s *Etcd) normalize(key string) string {
	key = store.Normalize(key)
	return strings.TrimPrefix(key, "/")
}

// keyNotFound checks on the error returned by the KeysAPI
// to verify if the key exists in the store or not
func keyNotFound(err error) bool {
	if err != nil {
		if etcdError, ok := err.(etcd.Error); ok {
			if etcdError.Code == etcd.ErrorCodeKeyNotFound ||
				etcdError.Code == etcd.ErrorCodeNotFile ||
				etcdError.Code == etcd.ErrorCodeNotDir {
				return true
			}
		}
	}
	return false
}

// Get the value at "key", returns the last modified
// index to use in conjunction to Atomic calls
func (s *Etcd) Get(key string) (pair *store.KVPair, err error) {
	getOpts := &etcd.GetOptions{
		Quorum: true,
	}

	result, err := s.client.Get(context.Background(), s.normalize(key), getOpts)
	if err != nil {
		if keyNotFound(err) {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}

	pair = &store.KVPair{
		Key:       key,
		Value:     []byte(result.Node.Value),
		LastIndex: result.Node.ModifiedIndex,
	}

	return pair, nil
}

// GetExt gets the value at "key", returns the last modified
// index to use in conjunction to Atomic calls
func (s *Etcd) GetExt(key string, options store.GetOptions) (pair *store.KVPairExt, err error) {
	getOpts := &etcd.GetOptions{
		Recursive: options.Recursive,
		Sort:      options.Sort,
		Quorum:    options.Quorum,
	}

	result, err := s.client.Get(context.Background(), s.normalize(key), getOpts)
	if err != nil {
		if keyNotFound(err) {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}

	prevVal := ""
	if result.PrevNode != nil {
		prevVal = result.PrevNode.Value
	}

	kv := &store.KVPairExt{
		Key:       result.Node.Key,
		Value:     result.Node.Value,
		PrevValue: prevVal,
		LastIndex: result.Node.ModifiedIndex,
		Action:    result.Action,
		Dir:       result.Node.Dir,
		Response:  result,
	}

	return kv, nil
}

// Put a value at "key"
func (s *Etcd) Put(key string, value []byte, opts *store.WriteOptions) error {
	setOpts := &etcd.SetOptions{}

	// Set options
	if opts != nil {
		setOpts.Dir = opts.IsDir
		setOpts.TTL = opts.TTL
	}

	_, err := s.client.Set(context.Background(), s.normalize(key), string(value), setOpts)
	return err
}

// Delete a value at "key"
func (s *Etcd) Delete(key string) error {
	opts := &etcd.DeleteOptions{
		Recursive: false,
	}

	_, err := s.client.Delete(context.Background(), s.normalize(key), opts)
	if keyNotFound(err) {
		return store.ErrKeyNotFound
	}
	return err
}

// Exists checks if the key exists inside the store
func (s *Etcd) Exists(key string) (bool, error) {
	_, err := s.Get(key)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Watch for changes on a "key"
// It returns a channel that will receive changes or pass
// on errors. Upon creation, the current value will first
// be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) Watch(key string, stopCh <-chan struct{}) (<-chan *store.KVPair, error) {
	opts := &etcd.WatcherOptions{Recursive: false}
	watcher := s.client.Watcher(s.normalize(key), opts)

	// watchCh is sending back events to the caller
	watchCh := make(chan *store.KVPair)

	go func() {
		defer close(watchCh)

		// Get the current value
		pair, err := s.Get(key)
		if err != nil {
			return
		}

		// Push the current value through the channel.
		watchCh <- pair

		for {
			// Check if the watch was stopped by the caller
			select {
			case <-stopCh:
				return
			default:
			}

			result, err := watcher.Next(context.Background())

			if err != nil {
				return
			}

			watchCh <- &store.KVPair{
				Key:       key,
				Value:     []byte(result.Node.Value),
				LastIndex: result.Node.ModifiedIndex,
			}
		}
	}()

	return watchCh, nil
}

// WatchExt watches for changes on a "key"
// It returns a channel that will receive changes or pass
// on errors. Upon creation, the current value will first
// be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) WatchExt(key string, options store.WatcherOptions, stopCh <-chan struct{}) (<-chan *store.KVPairExt, error) {
	opts := &etcd.WatcherOptions{AfterIndex: options.AfterIndex, Recursive: options.Recursive}
	watcher := s.client.Watcher(s.normalize(key), opts)

	// watchCh is sending back events to the caller
	watchCh := make(chan *store.KVPairExt)

	go func() {
		defer close(watchCh)

		// Get the current value
		if !options.NoList {
			pair, err := s.GetExt(key, store.GetOptions{Quorum: true})
			if err != nil {
				return
			}

			// Push the current value through the channel.
			watchCh <- &store.KVPairExt{
				Key:       pair.Key,
				Value:     pair.Value,
				LastIndex: pair.LastIndex,
				Action:    pair.Action,
				Dir:       pair.Dir,
				Response:  pair.Response,
			}
		}

		for {
			// Check if the watch was stopped by the caller
			select {
			case <-stopCh:
				return
			default:
			}

			result, err := watcher.Next(context.Background())

			if err != nil {
				return
			}

			prevVal := ""
			if result.PrevNode != nil {
				prevVal = result.PrevNode.Value
			}

			watchCh <- &store.KVPairExt{
				Key:       result.Node.Key,
				Value:     result.Node.Value,
				PrevValue: prevVal,
				LastIndex: result.Node.ModifiedIndex,
				Action:    result.Action,
				Dir:       result.Node.Dir,
				Response:  result,
			}
		}
	}()

	return watchCh, nil
}

// WatchTree watches for changes on a "directory"
// It returns a channel that will receive changes or pass
// on errors. Upon creating a watch, the current childs values
// will be sent to the channel. Providing a non-nil stopCh can
// be used to stop watching.
func (s *Etcd) WatchTree(directory string, stopCh <-chan struct{}) (<-chan []*store.KVPair, error) {
	watchOpts := &etcd.WatcherOptions{Recursive: true}
	watcher := s.client.Watcher(s.normalize(directory), watchOpts)

	// watchCh is sending back events to the caller
	watchCh := make(chan []*store.KVPair)

	go func() {
		defer close(watchCh)

		// Get child values
		list, err := s.List(directory)
		if err != nil {
			return
		}

		// Push the current value through the channel.
		watchCh <- list

		for {
			// Check if the watch was stopped by the caller
			select {
			case <-stopCh:
				return
			default:
			}

			_, err := watcher.Next(context.Background())

			if err != nil {
				return
			}

			list, err = s.List(directory)
			if err != nil {
				return
			}

			watchCh <- list
		}
	}()

	return watchCh, nil
}

// WatchTreeExt watches for changes on a "directory",
// extended edition. It provides some extra information
// along with normal key and value change.
// It returns a channel that will receive changes or pass
// on errors. Providing a non-nil stopCh can be used to
// stop watching.
func (s *Etcd) WatchTreeExt(directory string, stopCh <-chan struct{}) (<-chan *store.KVPairExt, error) {
	watchOpts := &etcd.WatcherOptions{Recursive: true}
	watcher := s.client.Watcher(s.normalize(directory), watchOpts)

	watchCh := make(chan *store.KVPairExt)

	go func() {
		defer close(watchCh)

		for {
			select {
			case <-stopCh:
				return
			default:
			}

			result, err := watcher.Next(context.Background())

			if err != nil {
				return
			}

			prevVal := ""
			if result.PrevNode != nil {
				prevVal = result.PrevNode.Value
			}

			watchCh <- &store.KVPairExt{
				Key:       result.Node.Key,
				Value:     result.Node.Value,
				PrevValue: prevVal,
				LastIndex: result.Node.ModifiedIndex,
				Action:    result.Action,
				Dir:       result.Node.Dir,
				Response:  result,
			}
		}
	}()

	return watchCh, nil
}

// AtomicPut puts a value at "key" if the key has not been
// modified in the meantime, throws an error if this is the case
func (s *Etcd) AtomicPut(key string, value []byte, previous *store.KVPair, opts *store.WriteOptions) (bool, *store.KVPair, error) {
	var (
		meta *etcd.Response
		err  error
	)

	setOpts := &etcd.SetOptions{}

	if previous != nil {
		setOpts.PrevExist = etcd.PrevExist
		setOpts.PrevIndex = previous.LastIndex
		if previous.Value != nil {
			setOpts.PrevValue = string(previous.Value)
		}
	} else {
		setOpts.PrevExist = etcd.PrevNoExist
	}

	if opts != nil {
		if opts.TTL > 0 {
			setOpts.TTL = opts.TTL
		}
	}

	meta, err = s.client.Set(context.Background(), s.normalize(key), string(value), setOpts)
	if err != nil {
		if etcdError, ok := err.(etcd.Error); ok {
			// Compare failed
			if etcdError.Code == etcd.ErrorCodeTestFailed {
				return false, nil, store.ErrKeyModified
			}
			// Node exists error (when PrevNoExist)
			if etcdError.Code == etcd.ErrorCodeNodeExist {
				return false, nil, store.ErrKeyExists
			}
		}
		return false, nil, err
	}

	updated := &store.KVPair{
		Key:       key,
		Value:     value,
		LastIndex: meta.Node.ModifiedIndex,
	}

	return true, updated, nil
}

// AtomicDelete deletes a value at "key" if the key
// has not been modified in the meantime, throws an
// error if this is the case
func (s *Etcd) AtomicDelete(key string, previous *store.KVPair) (bool, error) {
	if previous == nil {
		return false, store.ErrPreviousNotSpecified
	}

	delOpts := &etcd.DeleteOptions{}

	if previous != nil {
		delOpts.PrevIndex = previous.LastIndex
		if previous.Value != nil {
			delOpts.PrevValue = string(previous.Value)
		}
	}

	_, err := s.client.Delete(context.Background(), s.normalize(key), delOpts)
	if err != nil {
		if etcdError, ok := err.(etcd.Error); ok {
			// Key Not Found
			if etcdError.Code == etcd.ErrorCodeKeyNotFound {
				return false, store.ErrKeyNotFound
			}
			// Compare failed
			if etcdError.Code == etcd.ErrorCodeTestFailed {
				return false, store.ErrKeyModified
			}
		}
		return false, err
	}

	return true, nil
}

// List child nodes of a given directory
func (s *Etcd) List(directory string) ([]*store.KVPair, error) {
	getOpts := &etcd.GetOptions{
		Quorum:    true,
		Recursive: true,
		Sort:      true,
	}

	resp, err := s.client.Get(context.Background(), s.normalize(directory), getOpts)
	if err != nil {
		if keyNotFound(err) {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}

	kv := []*store.KVPair{}
	for _, n := range resp.Node.Nodes {
		kv = append(kv, &store.KVPair{
			Key:       n.Key,
			Value:     []byte(n.Value),
			LastIndex: n.ModifiedIndex,
		})
	}
	return kv, nil
}

// ListExt lists child nodes of a given directory, etcd native extension.
func (s *Etcd) ListExt(directory string) ([]*store.KVPairExt, error) {
	getOpts := &etcd.GetOptions{
		Quorum:    true,
		Recursive: true,
		Sort:      true,
	}

	result, err := s.client.Get(context.Background(), s.normalize(directory), getOpts)
	if err != nil {
		if keyNotFound(err) {
			return nil, store.ErrKeyNotFound
		}
		return nil, err
	}

	kv := []*store.KVPairExt{}
	for _, n := range result.Node.Nodes {
		kv = append(kv, &store.KVPairExt{
			Key:       n.Key,
			Value:     n.Value,
			LastIndex: n.ModifiedIndex,
			Dir:       n.Dir,
			Response:  result,
		})
	}

	return kv, nil
}

// DeleteTree deletes a range of keys under a given directory
func (s *Etcd) DeleteTree(directory string) error {
	delOpts := &etcd.DeleteOptions{
		Recursive: true,
	}

	_, err := s.client.Delete(context.Background(), s.normalize(directory), delOpts)
	if keyNotFound(err) {
		return store.ErrKeyNotFound
	}
	return err
}

// NewLock returns a handle to a lock struct which can
// be used to provide mutual exclusion on a key
func (s *Etcd) NewLock(key string, options *store.LockOptions) (lock store.Locker, err error) {
	var value string
	ttl := defaultLockTTL
	renewCh := make(chan struct{})

	// Apply options on Lock
	if options != nil {
		if options.Value != nil {
			value = string(options.Value)
		}
		if options.TTL != 0 {
			ttl = options.TTL
		}
		if options.RenewLock != nil {
			renewCh = options.RenewLock
		}
	}

	// Create lock object
	lock = &etcdLock{
		client:    s.client,
		stopRenew: renewCh,
		key:       s.normalize(key),
		value:     value,
		ttl:       ttl,
	}

	return lock, nil
}

// Lock attempts to acquire the lock and blocks while
// doing so. It returns a channel that is closed if our
// lock is lost or if an error occurs
func (l *etcdLock) Lock(stopChan <-chan struct{}) (<-chan struct{}, error) {
	fmt.Printf("=====> %d: etcd.Lock() entering\n", getGID())
	// Lock holder channel
	lockHeld := make(chan struct{})
	stopLocking := l.stopRenew

	setOpts := &etcd.SetOptions{
		TTL: l.ttl,
	}

	for {
		setOpts.PrevExist = etcd.PrevNoExist
		fmt.Printf("=====> %d: etcd.Lock(): trying to set %s to %s\n", getGID(), l.key, l.value)
		resp, err := l.client.Set(context.Background(), l.key, l.value, setOpts)
		if err != nil {
			fmt.Printf("=====> %d: Etcd.Lock() error %s %T\n", getGID(), err, err)
			if etcdError, ok := err.(etcd.Error); ok {
				if etcdError.Code != etcd.ErrorCodeNodeExist {
					fmt.Printf("=====> %d: etcd.Lock(): Got etcd error %s\n", getGID(), err)
					return nil, err
				}
				fmt.Printf("=====> %d: etcd.Lock(): Got etcd.ErrorCodeNodeExist, setting prevIndex to MaxInt\n", getGID())
				setOpts.PrevIndex = ^uint64(0)
			} else {
				fmt.Printf("=====> %d: etcd.Lock(): Got UNHANDLED etcd error %s %T\n", getGID(), err, err)
			}

		} else {
			setOpts.PrevIndex = resp.Node.ModifiedIndex
			fmt.Printf("=====> %d: etcd.Lock(): set %s to %s (%d)\n", getGID(), l.key, l.value, setOpts.PrevIndex)
		}

		setOpts.PrevExist = etcd.PrevExist
		var lastResp *etcd.Response
		lastResp, err = l.client.Set(context.Background(), l.key, l.value, setOpts)
		if lastResp != nil {
			l.last = lastResp
		}
		if err == nil {
			// Leader section
			fmt.Printf("=====> %d: etcd.Lock(): Leader selection session: set %s to %s, with %d\n", getGID(), l.key, l.value, setOpts.PrevIndex)

			r, err := l.client.Get(context.Background(), l.key, nil)
			if err != nil {
				fmt.Printf("=====> %d: etcd.Lock(): ERROR getting lock after set: %s %T\n", getGID(), err, err)
				return nil, err
			} else {
				fmt.Printf("=====> %d: etcd.Lock(): After a Set with %d got %s at %s\n", getGID(), l.last.Node.ModifiedIndex, r, l.key)
			}

			l.stopLock = stopLocking
			go l.holdLock(l.key, lockHeld, stopLocking)
			break
		} else {

			// If this is a legitimate error, return
			if etcdError, ok := err.(etcd.Error); ok {
				if etcdError.Code != etcd.ErrorCodeTestFailed {
					fmt.Printf("=====> %d: etcd.Lock() got error %s %T\n", getGID(), err, err)
					return nil, err
				}
			}
			fmt.Printf("=====> %d: etcd.Lock() got %s %T, gonna wait\n", getGID(), err, err)
			// Seeker section
			errorCh := make(chan error)
			chWStop := make(chan bool)
			free := make(chan bool)

			go l.waitLock(l.key, errorCh, chWStop, free)

			// Wait for the key to be available or for
			// a signal to stop trying to lock the key
			fmt.Printf("=====> %d: etcd.Lock() Entering select\n", getGID())

			select {
			case <-free:
				fmt.Printf("=====> %d: etcd.Lock() Got free\n", getGID())
				break
			case err := <-errorCh:
				fmt.Printf("=====> %d: etcd.Lock() Got error %s %T\n", getGID(), err, err)
				return nil, err
			case <-stopChan:
				fmt.Printf("=====> %d: etcd.Lock() Got stop.", getGID())
				return nil, ErrAbortTryLock
			}

			// Delete or Expire event occurred
			// Retry
		}
	}

	return lockHeld, nil
}

// Hold the lock as long as we can
// Updates the key ttl periodically until we receive
// an explicit stop signal from the Unlock method
func (l *etcdLock) holdLock(key string, lockHeld chan struct{}, stopLocking <-chan struct{}) {
	fmt.Printf("Entering goroutine holdLock: %d\n", getGID())
	defer close(lockHeld)

	update := time.NewTicker(l.ttl / 3)
	defer update.Stop()

	var err error
	setOpts := &etcd.SetOptions{TTL: l.ttl}

	for {
		select {
		case <-update.C:
			fmt.Printf("=====> %d: etcd.holdLock(): got update, setting %s to %s with prevIndex %d\n", getGID(), key, l.value, setOpts.PrevIndex)
			setOpts.PrevIndex = l.last.Node.ModifiedIndex
			l.last, err = l.client.Set(context.Background(), key, l.value, setOpts)
			if err != nil {
				fmt.Printf("=====> %d: etcd.holdLock(): got error %s on setting %s to %s\n", getGID(), err, key, l.value)
				return
			}

		case msg := <-stopLocking:
			fmt.Printf("=====> %d: etcd.holdLock(): got stopLocking: %p\n", getGID(), &msg)
			return
		}
	}
}

// WaitLock simply waits for the key to be available for creation
func (l *etcdLock) waitLock(key string, errorCh chan error, stopWatchCh chan bool, free chan<- bool) {

	opts := &etcd.WatcherOptions{Recursive: false}
	watcher := l.client.Watcher(key, opts)
	if l.waitLockDelay == 0 {
		l.waitLockDelay = 1 * time.Microsecond
	}
	retryLock := true
	ctx, cancelFunc := context.WithTimeout(context.Background(), l.waitLockDelay)
	defer cancelFunc()
	for {
		fmt.Printf("=====> %d: In the waitLock loop, waiting for event\n", getGID())
		event, err := watcher.Next(ctx)
		if err != nil {
			if err == context.DeadlineExceeded {
				fmt.Printf("=====> %d: In the waitLock loop, got DeadlineExceeded\n", getGID())
				// First, see if maybe the key is not there.
				fmt.Printf("=====> %d: In the waitLock loop, checking for %s\n", getGID(), key)
				kp, err2 := l.client.Get(ctx, key, nil)
				if err2 != nil {
					if err2 == store.ErrKeyNotFound {
						fmt.Printf("=====> %d: In the waitLock loop, %s not found \n", getGID(), key)
						retryLock = false
					} else {
						fmt.Printf("=====> %d: In the waitLock loop, checking on %s: %s (%T)", getGID(), key, err2, err2)
					}
				} else {
					fmt.Printf("=====> %d: In the waitLock loop, got %v", getGID(), kp)
				}
				if !retryLock {
					free <- true
					l.waitLockDelay = 0
					return
				}

				fmt.Printf("=====> %d: In the waitLock loop, got DeadlineExceeded, will retry for %s\n", getGID(), l.waitLockDelay)
				l.waitLockDelay *= 2
				ctx, cancelFunc = context.WithTimeout(context.Background(), l.waitLockDelay)
				defer cancelFunc()
				continue
			}
			fmt.Printf("=====> %d: In the waitLock loop, got error %s %T \n", getGID(), err, err)
			errorCh <- err
			return
		}
		fmt.Printf("=====> %d: In the waitLock loop, event is %d (%s)\n", event, event.Action, getGID())
		if event.Action == "delete" || event.Action == "expire" || event.Action == "compareAndDelete" {
			free <- true
			l.waitLockDelay = 0
			return
		}
	}
}

// Unlock the "key". Calling unlock while
// not holding the lock will throw an error
func (l *etcdLock) Unlock() error {
	fmt.Printf("=====> %d: entering etcd.Unlock()\n", getGID())
	if l.stopLock != nil {
		s := struct{}{}
		fmt.Printf("=====> %d: etcd.Unlock() Sending message to stop lock: %p\n", getGID(), &s)
		l.stopLock <- s
		fmt.Printf("=====> %d: etcd.Unlock() Sent message to stop lock: %p\n", getGID(), &s)
	}
	fmt.Printf("=====> %d: etcd.Unlock(): l.last is nil: %t\n", getGID(), l.last == nil)
	if l.last != nil {
		delOpts := &etcd.DeleteOptions{
			PrevIndex: l.last.Node.ModifiedIndex,
		}
		fmt.Printf("=====> %d: etcd.Unlock(): Deleting %s\n", getGID(), l.key)
		_, err := l.client.Delete(context.Background(), l.key, delOpts)
		if err != nil {
			fmt.Printf("=====> %d: etcd.Unlock() ERROR %s\n", getGID(), err)
			return err
		}
		r, err := l.client.Get(context.Background(), l.key, nil)
		if err != nil {
			fmt.Printf("=====> %d: etcd.Unlock() Successfully deleted: %s\n", getGID(), err)
		} else {
			fmt.Printf("=====> %d: etcd.Unlock(): After a Delete with %d got %s at %s\n", getGID(), l.last.Node.ModifiedIndex, r, l.key)
		}
	}
	return nil
}

// Close closes the client connection
func (s *Etcd) Close() {
	return
}

// TODO remove this and move it into logging
func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
