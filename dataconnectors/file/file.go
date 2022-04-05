package file

import (
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/logrusorgru/aurora"
	"golang.org/x/sync/errgroup"
)

const (
	FileConnectorName string = "file"
)

type FileConnector struct {
	path         string
	noWatch      bool
	readHandlers []*func(data []byte, metadata map[string]string) ([]byte, error)

	dataMutex sync.RWMutex
	fileInfo  fs.FileInfo
	data      []byte
}

func NewFileConnector() *FileConnector {
	return &FileConnector{}
}

func (c *FileConnector) Init(epoch time.Time, period time.Duration, interval time.Duration, params map[string]string) error {
	c.dataMutex = sync.RWMutex{}

	path := params["path"]
	appDir := params["appDirectory"]
	if !filepath.IsAbs(path) {
		path = filepath.Clean(filepath.Join(appDir, path))
	}

	c.path = path
	c.noWatch = params["watch"] != "true"

	if newFileInfo, err := os.Stat(c.path); err == nil {
		if _, err := c.loadFileData(newFileInfo); err != nil {
			return err
		}
		if err = c.sendData(); err != nil {
			return err
		}
	}

	if !c.noWatch {
		c.watchPath()
	}

	return nil
}

func (c *FileConnector) Read(handler func(data []byte, metadata map[string]string) ([]byte, error)) error {
	c.readHandlers = append(c.readHandlers, &handler)
	return nil
}

func (c *FileConnector) loadFileData(newFileInfo fs.FileInfo) ([]byte, error) {
	log.Printf("loading file '%s' ...", c.path)

	loadStartTime := time.Now()

	fileData, err := ioutil.ReadFile(c.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %w", c.path, err)
	}

	c.data = fileData
	c.fileInfo = newFileInfo

	duration := time.Since(loadStartTime)

	log.Println(aurora.Green(fmt.Sprintf("loaded file '%s' in %.2f seconds ...", filepath.Base(c.path), duration.Seconds())))

	return fileData, nil
}

func (c *FileConnector) watchPath() {
	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Println(fmt.Errorf("error starting '%s' watcher: %w", c.path, err))
		}
		defer watcher.Close()

		if err := watcher.Add(c.path); err != nil {
			log.Println(fmt.Errorf("error starting '%s' watcher: %w", c.path, err))
		}

		log.Println(fmt.Sprintf("watching '%s' for updates", c.path))

		for {
			select {
			case event := <-watcher.Events:
				err := c.processWatchNotifyEvent(event, c.path)
				if err != nil {
					log.Println(fmt.Errorf("error processing '%s' event %s: %w", c.path, event, err))
				}
			case err := <-watcher.Errors:
				log.Println(fmt.Errorf("error processing '%s': %w", c.path, err))
			}
		}
	}()
}

func (c *FileConnector) processWatchNotifyEvent(event fsnotify.Event, path string) error {
	switch event.Op {
	case fsnotify.Create:
		fallthrough
	case fsnotify.Write:
		file := filepath.Join(path, event.Name)
		newFileInfo, err := os.Stat(file)
		if err != nil {
			return fmt.Errorf("failed to open file '%s': %w", file, err)
		}
		c.dataMutex.Lock()
		defer c.dataMutex.Unlock()
		if c.fileInfo == nil || newFileInfo.ModTime().After(c.fileInfo.ModTime()) {
			// Only load file if it's changed since last read
			_, err = c.loadFileData(newFileInfo)
			if err != nil {
				return err
			}
			err = c.sendData()
			if err != nil {
				return err
			}
		}
	case fsnotify.Remove:
		c.dataMutex.Lock()
		defer c.dataMutex.Unlock()
		c.fileInfo = nil
		c.data = nil
	}

	return nil
}

func (c *FileConnector) sendData() error {
	if len(c.readHandlers) == 0 || c.fileInfo == nil || c.data == nil {
		// Nothing to read
		return nil
	}

	metadata := map[string]string{}
	metadata["mod_time"] = c.fileInfo.ModTime().Format(time.RFC3339Nano)
	metadata["size"] = fmt.Sprintf("%d", c.fileInfo.Size())

	c.dataMutex.RLock()
	defer c.dataMutex.RUnlock()

	if len(c.readHandlers) == 0 {
		return nil
	}

	errGroup, _ := errgroup.WithContext(context.Background())
	for _, handler := range c.readHandlers {
		readHandler := *handler
		errGroup.Go(func() error {
			_, err := readHandler(c.data, metadata)
			return err
		})
	}

	return errGroup.Wait()
}
