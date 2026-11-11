package vault

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
)

// SensitiveBuffer holds decrypted credentials in memory with explicit zeroization.
type SensitiveBuffer struct {
	data []byte
}

// NewSensitiveBuffer creates a buffer wrapping sensitive bytes.
func NewSensitiveBuffer(b []byte) *SensitiveBuffer {
	cp := make([]byte, len(b))
	copy(cp, b)
	return &SensitiveBuffer{data: cp}
}

// String returns the string representation.
func (s *SensitiveBuffer) String() string {
	if s == nil || s.data == nil {
		return ""
	}
	return string(s.data)
}

// Bytes returns the raw underlying byte slice.
func (s *SensitiveBuffer) Bytes() []byte {
	if s == nil {
		return nil
	}
	return s.data
}

// Wipe zeroes the memory immediately to prevent plain-text exposure in heap dumps.
func (s *SensitiveBuffer) Wipe() {
	if s == nil || s.data == nil {
		return
	}
	for i := range s.data {
		s.data[i] = 0
	}
	s.data = nil
}

// VaultSecret represents resolved credentials for a job.
type VaultSecret struct {
	Values map[string]*SensitiveBuffer
}

// Get retrieves a sensitive value by key.
func (vs *VaultSecret) Get(key string) *SensitiveBuffer {
	if vs == nil || vs.Values == nil {
		return nil
	}
	return vs.Values[key]
}

// Wipe zeroes all buffers in the secret map.
func (vs *VaultSecret) Wipe() {
	if vs == nil || vs.Values == nil {
		return
	}
	for _, buf := range vs.Values {
		if buf != nil {
			buf.Wipe()
		}
	}
	vs.Values = nil
}

// Connector defines operations to securely pull credentials and manage session caches.
type Connector interface {
	FetchSecret(authRef string) (*VaultSecret, error)
	GetCachedSession(domain string) ([]byte, error)
	SaveSession(domain string, sessionData []byte) error
}

// MemoryVaultConnector is a thread-safe in-memory vault and encrypted session store.
type MemoryVaultConnector struct {
	mu        sync.RWMutex
	secrets   map[string]map[string][]byte
	sessions  map[string][]byte // AES-GCM encrypted session blobs
	masterKey []byte
}

// NewMemoryVaultConnector initializes a vault with a 32-byte AES master key.
func NewMemoryVaultConnector(masterKey []byte) (*MemoryVaultConnector, error) {
	if len(masterKey) != 32 {
		key := make([]byte, 32)
		if _, err := io.ReadFull(rand.Reader, key); err != nil {
			return nil, fmt.Errorf("failed generating master key: %w", err)
		}
		masterKey = key
	}
	return &MemoryVaultConnector{
		secrets:   make(map[string]map[string][]byte),
		sessions:  make(map[string][]byte),
		masterKey: masterKey,
	}, nil
}

// RegisterSecret allows populating mock vault secrets (e.g. for testing / seeding).
func (v *MemoryVaultConnector) RegisterSecret(ref string, data map[string]string) {
	v.mu.Lock()
	defer v.mu.Unlock()

	byteMap := make(map[string][]byte)
	for k, val := range data {
		byteMap[k] = []byte(val)
	}
	v.secrets[ref] = byteMap
}

// FetchSecret retrieves secret keys for an auth reference (e.g. "vault://creds/supplier_portal_prod").
func (v *MemoryVaultConnector) FetchSecret(authRef string) (*VaultSecret, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	cleanRef := strings.TrimSpace(authRef)
	if cleanRef == "" {
		return nil, errors.New("empty auth_ref provided")
	}

	data, exists := v.secrets[cleanRef]
	if !exists {
		// Provide default mock secrets if unregistered to allow generic tests
		return &VaultSecret{
			Values: map[string]*SensitiveBuffer{
				"username": NewSensitiveBuffer([]byte("fleet_bot_operator")),
				"password": NewSensitiveBuffer([]byte("RpaFleetSecurePassword2026!")),
				"token":    NewSensitiveBuffer([]byte("token_prod_sec_8912")),
			},
		}, nil
	}

	res := make(map[string]*SensitiveBuffer)
	for k, val := range data {
		res[k] = NewSensitiveBuffer(val)
	}
	return &VaultSecret{Values: res}, nil
}

// SaveSession encrypts and caches reusable session cookies/tokens for recurrent runs.
func (v *MemoryVaultConnector) SaveSession(domain string, sessionData []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	block, err := aes.NewCipher(v.masterKey)
	if err != nil {
		return err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return err
	}

	ciphertext := gcm.Seal(nonce, nonce, sessionData, nil)
	v.sessions[domain] = ciphertext
	return nil
}

// GetCachedSession decrypts and returns cached session cookies for the target domain.
func (v *MemoryVaultConnector) GetCachedSession(domain string) ([]byte, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	ciphertext, exists := v.sessions[domain]
	if !exists {
		return nil, nil // No session cached
	}

	block, err := aes.NewCipher(v.masterKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("malformed encrypted session")
	}

	nonce, actualCipher := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, actualCipher, nil)
	if err != nil {
		return nil, fmt.Errorf("session decryption failed: %w", err)
	}
	return plaintext, nil
}
