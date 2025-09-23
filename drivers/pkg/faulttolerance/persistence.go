package faulttolerance

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type MessageBuffer struct {
	Messages  [][]byte               `json:"messages"`
	Metadata  map[string]interface{} `json:"metadata"`
	Timestamp time.Time              `json:"timestamp"`
}

type PersistenceManager struct {
	dataDir       string
	bufferSize    int
	flushInterval time.Duration
	logger        *logrus.Logger
	buffer        [][]byte
	bufferMutex   sync.RWMutex
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
	flushChan     chan struct{}
}

func NewPersistenceManager(dataDir string, bufferSize int, flushInterval time.Duration, logger *logrus.Logger) (*PersistenceManager, error) {
	if bufferSize <= 0 {
		bufferSize = 1000
	}
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	pm := &PersistenceManager{
		dataDir:       dataDir,
		bufferSize:    bufferSize,
		flushInterval: flushInterval,
		logger:        logger,
		buffer:        make([][]byte, 0, bufferSize),
		ctx:           ctx,
		cancel:        cancel,
		flushChan:     make(chan struct{}, 1),
	}

	return pm, nil
}

func (pm *PersistenceManager) Start() {
	pm.wg.Add(1)
	go pm.flushLoop()
	pm.logger.Info("Persistence manager started")
}

func (pm *PersistenceManager) Stop() {
	pm.cancel()
	pm.wg.Wait()

	pm.flushBuffer()
	pm.logger.Info("Persistence manager stopped")
}

func (pm *PersistenceManager) StoreMessage(message []byte) {
	pm.bufferMutex.Lock()
	defer pm.bufferMutex.Unlock()

	pm.buffer = append(pm.buffer, message)

	if len(pm.buffer) >= pm.bufferSize {
		select {
		case pm.flushChan <- struct{}{}:
		default:
		}
	}
}

func (pm *PersistenceManager) flushLoop() {
	defer pm.wg.Done()

	ticker := time.NewTicker(pm.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-pm.ctx.Done():
			return
		case <-ticker.C:
			pm.flushBuffer()
		case <-pm.flushChan:
			pm.flushBuffer()
		}
	}
}

func (pm *PersistenceManager) flushBuffer() {
	pm.bufferMutex.Lock()
	if len(pm.buffer) == 0 {
		pm.bufferMutex.Unlock()
		return
	}

	messages := make([][]byte, len(pm.buffer))
	copy(messages, pm.buffer)
	pm.buffer = pm.buffer[:0]
	pm.bufferMutex.Unlock()

	if err := pm.writeMessages(messages); err != nil {
		pm.logger.Errorf("Failed to flush messages to disk: %v", err)

		pm.bufferMutex.Lock()
		pm.buffer = append(messages, pm.buffer...)
		pm.bufferMutex.Unlock()
	} else {
		pm.logger.Debugf("Flushed %d messages to disk", len(messages))
	}
}

func (pm *PersistenceManager) writeMessages(messages [][]byte) error {
	filename := fmt.Sprintf("messages_%d.jsonl", time.Now().Unix())
	filepath := filepath.Join(pm.dataDir, filename)

	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, message := range messages {
		entry := map[string]interface{}{
			"timestamp": time.Now().Format(time.RFC3339),
			"data":      string(message),
		}

		if data, err := json.Marshal(entry); err == nil {
			writer.Write(data)
			writer.WriteByte('\n')
		}
	}

	return nil
}

func (pm *PersistenceManager) RecoverMessages(maxAge time.Duration) ([][]byte, error) {
	files, err := os.ReadDir(pm.dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read data directory: %w", err)
	}

	var allMessages [][]byte
	cutoffTime := time.Now().Add(-maxAge)

	for _, file := range files {
		if file.IsDir() || !file.Type().IsRegular() {
			continue
		}

		info, err := file.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoffTime) {
			continue
		}

		filepath := filepath.Join(pm.dataDir, file.Name())
		messages, err := pm.readMessagesFromFile(filepath)
		if err != nil {
			pm.logger.Warnf("Failed to read messages from %s: %v", file.Name(), err)
			continue
		}

		allMessages = append(allMessages, messages...)
	}

	pm.logger.Infof("Recovered %d messages from disk", len(allMessages))
	return allMessages, nil
}

func (pm *PersistenceManager) readMessagesFromFile(filepath string) ([][]byte, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var messages [][]byte
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var entry map[string]interface{}
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}

		if data, ok := entry["data"].(string); ok {
			messages = append(messages, []byte(data))
		}
	}

	return messages, scanner.Err()
}

func (pm *PersistenceManager) CleanupOldFiles(maxAge time.Duration) error {
	files, err := os.ReadDir(pm.dataDir)
	if err != nil {
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	cutoffTime := time.Now().Add(-maxAge)
	cleanedCount := 0

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		info, err := file.Info()
		if err != nil {
			continue
		}

		if info.ModTime().Before(cutoffTime) {
			filepath := filepath.Join(pm.dataDir, file.Name())
			if err := os.Remove(filepath); err != nil {
				pm.logger.Warnf("Failed to remove old file %s: %v", file.Name(), err)
			} else {
				cleanedCount++
			}
		}
	}

	if cleanedCount > 0 {
		pm.logger.Infof("Cleaned up %d old persistence files", cleanedCount)
	}

	return nil
}

func (pm *PersistenceManager) GetStats() map[string]interface{} {
	pm.bufferMutex.RLock()
	bufferSize := len(pm.buffer)
	pm.bufferMutex.RUnlock()

	files, _ := os.ReadDir(pm.dataDir)
	fileCount := 0
	for _, file := range files {
		if !file.IsDir() {
			fileCount++
		}
	}

	return map[string]interface{}{
		"buffer_size":     bufferSize,
		"max_buffer_size": pm.bufferSize,
		"file_count":      fileCount,
		"data_directory":  pm.dataDir,
		"flush_interval":  pm.flushInterval.String(),
	}
}
