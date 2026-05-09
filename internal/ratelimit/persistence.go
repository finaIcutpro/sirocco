package ratelimit

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type StateStatus struct {
	Path        string    `json:"path,omitempty"`
	Loaded      bool      `json:"loaded"`
	LoadedAt    time.Time `json:"loaded_at,omitempty"`
	SavedAt     time.Time `json:"saved_at,omitempty"`
	Buckets     int       `json:"buckets"`
	LastError   string    `json:"last_error,omitempty"`
	Persistence bool      `json:"persistence"`
}

type stateFile struct {
	Version int           `json:"version"`
	SavedAt time.Time     `json:"saved_at"`
	Buckets []bucketState `json:"buckets"`
}

type bucketState struct {
	Key             string `json:"key"`
	Route           string `json:"route"`
	BucketID        string `json:"bucket_id,omitempty"`
	Cap             int    `json:"cap"`
	Remaining       int    `json:"remaining"`
	WindowMillis    int64  `json:"window_ms"`
	ResetUnixMillis int64  `json:"reset_unix_ms,omitempty"`
	BlockUnixMillis int64  `json:"block_unix_ms,omitempty"`
	UpdatedUnixMS   int64  `json:"updated_unix_ms,omitempty"`
}

func (m *Manager) LoadState(path string, maxAge time.Duration) error {
	if path == "" {
		m.setState(StateStatus{Persistence: false})
		return nil
	}
	status := StateStatus{Path: path, Persistence: true}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			m.setState(status)
			return nil
		}
		status.LastError = err.Error()
		m.setState(status)
		return err
	}

	var file stateFile
	if err := json.Unmarshal(data, &file); err != nil {
		status.LastError = err.Error()
		m.setState(status)
		return err
	}
	if file.Version != 1 {
		err := fmt.Errorf("unsupported state version %d", file.Version)
		status.LastError = err.Error()
		m.setState(status)
		return err
	}
	if maxAge > 0 && time.Since(file.SavedAt) > maxAge {
		status.LastError = "state expired"
		m.setState(status)
		return nil
	}

	loaded := 0
	m.mu.Lock()
	for _, item := range file.Buckets {
		if item.Key == "" || item.Route == "" || item.Cap <= 0 || item.WindowMillis <= 0 {
			continue
		}
		m.routes[item.Key] = bucketFromState(item)
		loaded++
	}
	m.mu.Unlock()

	status.Loaded = loaded > 0
	status.LoadedAt = time.Now()
	status.SavedAt = file.SavedAt
	status.Buckets = loaded
	m.setState(status)
	return nil
}

func (m *Manager) SaveState(path string) error {
	if path == "" {
		return nil
	}

	file := stateFile{Version: 1, SavedAt: time.Now()}
	m.mu.RLock()
	for key, bucket := range m.routes {
		if item, ok := bucket.state(key); ok {
			file.Buckets = append(file.Buckets, item)
		}
	}
	m.mu.RUnlock()

	data, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		m.stateError(path, err)
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		m.stateError(path, err)
		return err
	}
	if err := atomicWrite(path, data, 0o644); err != nil {
		m.stateError(path, err)
		return err
	}

	m.stateMu.Lock()
	m.state.Path = path
	m.state.Persistence = true
	m.state.SavedAt = file.SavedAt
	m.state.Buckets = len(file.Buckets)
	m.state.LastError = ""
	m.stateMu.Unlock()
	m.dirty.Store(false)
	return nil
}

func (b *routeBucket) state(key string) (bucketState, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cap <= 0 || b.window <= 0 {
		return bucketState{}, false
	}
	return bucketState{Key: key, Route: b.route, BucketID: b.bucketID, Cap: b.cap, Remaining: b.remaining, WindowMillis: b.window.Milliseconds(), ResetUnixMillis: unixMillis(b.reset), BlockUnixMillis: unixMillis(b.blockUntil), UpdatedUnixMS: unixMillis(b.updated)}, true
}

func bucketFromState(item bucketState) *routeBucket {
	return &routeBucket{route: item.Route, bucketID: item.BucketID, cap: item.Cap, remaining: item.Remaining, window: time.Duration(item.WindowMillis) * time.Millisecond, reset: fromUnixMillis(item.ResetUnixMillis), blockUntil: fromUnixMillis(item.BlockUnixMillis), updated: fromUnixMillis(item.UpdatedUnixMS)}
}

func (m *Manager) setState(status StateStatus) {
	m.stateMu.Lock()
	m.state = status
	m.stateMu.Unlock()
}

func (m *Manager) stateError(path string, err error) {
	m.stateMu.Lock()
	m.state.Path = path
	m.state.Persistence = path != ""
	m.state.LastError = err.Error()
	m.stateMu.Unlock()
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func unixMillis(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func fromUnixMillis(v int64) time.Time {
	if v <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(v)
}
