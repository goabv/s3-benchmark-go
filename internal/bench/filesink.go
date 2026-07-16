package bench

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// fileSink writes downloaded parts to on-disk files (one per object) using
// positional writes, so parts can be written concurrently and out of order — the
// Go analogue of the JS runner's "file" delivery mode.
type fileSink struct {
	mu    sync.Mutex
	dir   string
	files map[string]*os.File
}

func newFileSink(dir string) *fileSink {
	return &fileSink{dir: dir, files: map[string]*os.File{}}
}

// fileFor lazily opens (creating/truncating) the backing file for an object key.
func (fs *fileSink) fileFor(key string) (*os.File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if f, ok := fs.files[key]; ok {
		return f, nil
	}
	path := filepath.Join(fs.dir, safeFileName(key))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	fs.files[key] = f
	return f, nil
}

// writeAt writes a part's bytes at its byte offset within the object's file.
func (fs *fileSink) writeAt(key string, off int64, data []byte) error {
	f, err := fs.fileFor(key)
	if err != nil {
		return err
	}
	_, err = f.WriteAt(data, off)
	return err
}

// closeAll closes every open file.
func (fs *fileSink) closeAll() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for _, f := range fs.files {
		_ = f.Close()
	}
	fs.files = map[string]*os.File{}
}

// safeFileName flattens an object key into a single filesystem-safe name.
func safeFileName(key string) string {
	return strings.NewReplacer("/", "_", "\\", "_", ":", "_").Replace(key)
}
