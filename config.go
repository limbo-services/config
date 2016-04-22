package config

import (
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
	sources []Source
	ready   chan struct{}
	bus     eventBus

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
	m := &Map{
		sources: sources,
		ready:   make(chan struct{}),
		entries: make(map[string]string),
	}

	return m, m.runner
}

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
		newMap  = make(map[string]string, len(oldMap))
		changes []string
	)

	for _, source := range m.sources {
		layer, err := source.Read()
		if err != nil {
			return err
		}

		for k, v := range layer {
			newMap[k] = v
		}
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
	return &prefixSource{
		prefix: prefix,
		s:      s,
	}
}

type prefixSource struct {
	prefix string
	s      Source
}

func (s *prefixSource) Read() (map[string]string, error) {
	m, err := s.s.Read()
	if err != nil {
		return nil, err
	}

	o := make(map[string]string, len(m))
	for k, v := range m {
		o[s.prefix+k] = v
	}
	return o, nil
}

// Static provides static configuration. This can be used for default and
// override layers.
func Static(v map[string]string) Source {
	if v == nil {
		v = make(map[string]string)
	}
	return &staticSource{e: v}
}

type staticSource struct {
	e map[string]string
}

func (s *staticSource) Read() (map[string]string, error) {
	return s.e, nil
}

// Dir loads entries from a directory.
// One file per entry is expected.
func Dir(name string) Source {
	return &dirSource{name: name}
}

var dirSourceEntryNameRE = regexp.MustCompile(`^\.?[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`)

type dirSource struct {
	name string
}

func (s *dirSource) Read() (map[string]string, error) {
	fis, err := ioutil.ReadDir(s.name)
	if os.IsNotExist(err) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}

	m := make(map[string]string, len(fis))

	for _, fi := range fis {
		if !fi.Mode().IsRegular() {
			if fi.Mode()&os.ModeSymlink == 0 {
				continue
			}
		}

		path := filepath.Join(s.name, fi.Name())
		name := filepath.Base(fi.Name())
		// ignore system files: .DS_Store
		if name == ".DS_Store" {
			continue
		}
		if len(name) > 253 || !dirSourceEntryNameRE.MatchString(name) {
			continue
		}

		data, err := ioutil.ReadFile(path)
		if err != nil {
			return nil, err
		}

		m[name] = strings.TrimSpace(string(data))
	}

	return m, nil
}
