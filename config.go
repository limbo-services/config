package config

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/context"
	"limbo.services/proc"
)

// Map holds all configuration entries
type Map struct {
	source Source
	ready  chan struct{}
	bus    eventBus

	entriesMtx sync.RWMutex
	entries    map[string]string
}

// Source must be implemented by configuration sources
type Source interface {
	Read() (map[string]string, error)
}

// New returns a new Map.
// Map periodically loads entries from the provided sources
func New(sources ...Source) (*Map, proc.Runner) {
	if len(sources) == 0 {
		sources = append(sources, Database())
	}

	m := &Map{
		source:  Multi(sources...),
		ready:   make(chan struct{}),
		entries: make(map[string]string),
	}

	return m, m.runner
}

// Get value for key
func (m *Map) Get(key string) string {
	m.entriesMtx.RLock()
	defer m.entriesMtx.RUnlock()

	if m.entries == nil {
		return ""
	}
	return m.entries[key]
}

func (m *Map) runner(ctx context.Context) <-chan error {
	out := make(chan error)
	go func() {
		defer close(out)

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		err := m.update()
		if err != nil {
			out <- err
			return
		}

		// trigger Ready
		close(m.ready)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := m.update()
				if err != nil {
					out <- err
					return
				}
			}
		}
	}()
	return out
}

func (m *Map) update() error {
	var (
		oldMap  = m.entries
		changes []string
	)

	newMap, err := m.source.Read()
	if err != nil {
		return err
	}

	for k, a := range oldMap {
		b := newMap[k]
		if a != b {
			changes = append(changes, k)
		}
	}

	for k, a := range newMap {
		b := oldMap[k]
		if a != b {
			changes = append(changes, k)
		}
	}

	changes = dedeupStrings(changes)

	m.entriesMtx.Lock()
	m.entries = newMap
	m.entriesMtx.Unlock()

	for _, key := range changes {
		m.bus.send(key)
	}
	return nil
}

// Ready returns a channel which is unblocked when the Map is ready.
func (m *Map) Ready() <-chan struct{} {
	return m.ready
}

// Notify causes Map to change events to c. If no keys are provided,
// all changes will be relayed to c. Otherwise, just the provided changes will.
//
// Map will not block sending to c: the caller must ensure that c has sufficient
// buffer space to keep up with the expected change rate. For a channel used for
// notification of just one change value, a buffer of size 1 is sufficient.
//
// It is allowed to call Notify multiple times with the same channel: each call
// expands the set of changes sent to that channel. The only way to remove keys
// from the set is to call Stop.
//
// It is allowed to call Notify multiple times with different channels and the
// same keys: each channel receives copies of incoming changes independently.
func (m *Map) Notify(c chan<- string, keys ...string) {
	m.bus.Notify(c, keys)
}

// Stop causes Map to stop relaying key changes to c. It undoes
// the effect of all prior calls to Notify using c. When Stop returns,
// it is guaranteed that c will receive no more signals.
func (m *Map) Stop(c chan<- string) {
	m.bus.Stop(c)
}

type eventBus struct {
	mtx           sync.Mutex
	subscriptions map[string]map[chan<- string]struct{}
}

func (b *eventBus) send(key string) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.subscriptions == nil {
		return
	}

	if m := b.subscriptions["*"]; m != nil {
		for c := range m {
			select {
			case c <- key:
			default:
			}
		}
	}

	if m := b.subscriptions[key]; m != nil {
		for c := range m {
			select {
			case c <- key:
			default:
			}
		}
	}
}

func (b *eventBus) Notify(c chan<- string, keys []string) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.subscriptions == nil {
		b.subscriptions = make(map[string]map[chan<- string]struct{})
	}
	if len(keys) == 0 {
		for k, m := range b.subscriptions {
			if k == "*" {
				continue
			}
			delete(m, c)
		}
		m := b.subscriptions["*"]
		if m == nil {
			m = make(map[chan<- string]struct{})
			b.subscriptions["*"] = m
		}
		m[c] = struct{}{}
	} else {
		delete(b.subscriptions["*"], c)
		for _, key := range keys {
			m := b.subscriptions[key]
			if m == nil {
				m = make(map[chan<- string]struct{})
				b.subscriptions[key] = m
			}
			m[c] = struct{}{}
		}
	}
}

func (b *eventBus) Stop(c chan<- string) {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if b.subscriptions == nil {
		return
	}
	for _, m := range b.subscriptions {
		delete(m, c)
	}
}

func dedeupStrings(s []string) []string {
	sort.Strings(s)

	o := s[:0]
	l := ""
	for i, v := range s {
		if i == 0 || v != l {
			l = v
			o = append(o, v)
		}
	}

	return o
}

// Prefix all entry keys with a given prefix
func Prefix(prefix string, s Source) Source {
	return SourceFunc(func() (map[string]string, error) {
		m, err := s.Read()
		if err != nil {
			return nil, err
		}
		if m == nil {
			return nil, nil
		}

		o := make(map[string]string, len(m))
		for k, v := range m {
			o[prefix+k] = v
		}
		return o, nil
	})
}

// Static provides static configuration. This can be used for default and
// override layers.
func Static(v map[string]string) Source {
	return SourceFunc(func() (map[string]string, error) {
		return v, nil
	})
}

var dirSourceEntryNameRE = regexp.MustCompile(`^\.?[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

// Dir loads entries from a directory.
// One file per entry is expected.
func Dir(dirname string) Source {
	return SourceFunc(func() (map[string]string, error) {
		entries, err := ioutil.ReadDir(dirname)
		if os.IsNotExist(err) || os.IsPermission(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		m := make(map[string]string, len(entries))

		for _, entry := range entries {
			entryPath := filepath.Join(dirname, entry.Name())
			entryName := filepath.Base(entry.Name())
			fi, err := os.Stat(entryPath)
			if os.IsNotExist(err) || os.IsPermission(err) {
				continue
			}
			if !fi.Mode().IsRegular() {
				continue
			}

			// ignore system files: .DS_Store
			if entryName == ".DS_Store" {
				continue
			}
			if len(entryName) > 253 || !dirSourceEntryNameRE.MatchString(entryName) {
				continue
			}

			data, err := ioutil.ReadFile(entryPath)
			if err != nil {
				return nil, err
			}

			m[entryName] = strings.TrimSpace(string(data))
		}

		return m, nil
	})
}

type SourceFunc func() (map[string]string, error)

func (f SourceFunc) Read() (map[string]string, error) {
	return f()
}

// Multi loads entries from multiple sources
func Multi(sources ...Source) Source {
	return SourceFunc(func() (map[string]string, error) {
		var m = make(map[string]string)

		for _, source := range sources {
			layer, err := source.Read()
			if err != nil {
				return nil, err
			}

			if layer == nil {
				continue
			}

			for k, v := range layer {
				m[k] = v
			}
		}

		return m, nil
	})
}

// Database loads entries from .config and /etc/database
func Database() Source {
	return SourceFunc(func() (map[string]string, error) {
		pwd, err := os.Getwd()
		if err != nil {
			pwd = "/"
		}

		var sources []Source

		{
			configDir := filepath.Join("/etc/database")
			fi, err := os.Stat(configDir)
			if err == nil && fi.IsDir() {
				sources = append([]Source{databaseDir(configDir)}, sources...)
			}
			if os.IsNotExist(err) || os.IsPermission(err) {
				err = nil
			}
			if err != nil {
				return nil, err
			}
		}

		for {
			configDir := filepath.Join(pwd, ".config")
			fi, err := os.Stat(configDir)
			if err == nil && fi.IsDir() {
				sources = append([]Source{databaseDir(configDir)}, sources...)
			}
			if os.IsNotExist(err) || os.IsPermission(err) {
				err = nil
			}
			if err != nil {
				return nil, err
			}
			if pwd == "/" {
				break
			}
			pwd = filepath.Dir(pwd)
		}

		return Multi(sources...).Read()
	})
}

func databaseDir(dirname string) Source {
	return SourceFunc(func() (map[string]string, error) {
		entries, err := ioutil.ReadDir(dirname)
		if os.IsNotExist(err) || os.IsPermission(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		var sources = make([]Source, 0, len(entries))

		for _, entry := range entries {
			entryPath := filepath.Join(dirname, entry.Name())
			entryName := filepath.Base(entry.Name())
			fi, err := os.Stat(entryPath)
			if os.IsNotExist(err) || os.IsPermission(err) {
				continue
			}
			if err != nil {
				return nil, err
			}

			// ignore system files: .DS_Store
			if entryName == ".DS_Store" {
				continue
			}
			if len(entryName) > 253 || !dirSourceEntryNameRE.MatchString(entryName) {
				continue
			}

			if fi.Mode().IsDir() {
				sources = append(sources, Prefix(entryName, Dir(entryPath)))
			}
			if fi.Mode().IsRegular() {
				sources = append(sources, Prefix(entryName, File(entryPath)))
			}
		}

		return Multi(sources...).Read()
	})
}

func File(filename string) Source {
	return SourceFunc(func() (map[string]string, error) {
		data, err := ioutil.ReadFile(filename)
		if os.IsNotExist(err) || os.IsPermission(err) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}

		var m = make(map[string]string)

		for len(data) > 0 {
			line := data
			if idx := bytes.IndexByte(data, '\n'); idx >= 0 {
				line = data[:idx]
				data = data[idx+1:]
			} else {
				data = nil
			}
			if len(line) == 0 {
				continue
			}

			var (
				key   = line
				value []byte
			)

			if idx := bytes.IndexByte(line, '='); idx >= 0 {
				key = line[:idx]
				value = line[idx+1:]
			}
			if len(key) == 0 {
				continue
			}

			m[string(key)] = string(value)
		}

		return m, nil
	})
}
