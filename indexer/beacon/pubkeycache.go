package beacon

import (
	"strconv"
	"sync"

	"github.com/attestantio/go-eth2-client/spec/phase0"
	"github.com/syndtr/goleveldb/leveldb"
)

type pubkeyCache struct {
	pubkeyDb    *leveldb.DB
	pubkeyMap   map[phase0.BLSPubKey]phase0.ValidatorIndex
	pubkeyMutex sync.RWMutex // mutex to protect pubkeyMap for concurrent access
}

// newPubkeyCache creates a new cache for validator public keys.
func newPubkeyCache(indexer *Indexer, cacheFile string) *pubkeyCache {
	cache := &pubkeyCache{}

	if cacheFile != "" {
		db, err := leveldb.OpenFile(cacheFile, nil)
		if err != nil {
			indexer.logger.WithError(err).Error("failed to open pubkey cache")
		} else {
			cache.pubkeyDb = db
		}
	}

	if cache.pubkeyDb == nil {
		cache.pubkeyMap = make(map[phase0.BLSPubKey]phase0.ValidatorIndex)
	}

	return cache
}

func (c *pubkeyCache) Add(pubkey phase0.BLSPubKey, index phase0.ValidatorIndex) error {
	c.pubkeyMutex.Lock()
	defer c.pubkeyMutex.Unlock()

	if c.pubkeyDb != nil {
		indexStr := strconv.FormatUint(uint64(index), 10)
		err := c.pubkeyDb.Put(pubkey[:], []byte(indexStr), nil)
		if err != nil {
			return err
		}
	} else {
		c.pubkeyMap[pubkey] = index
	}

	return nil
}

func (c *pubkeyCache) Get(pubkey phase0.BLSPubKey) (phase0.ValidatorIndex, bool) {
	if c.pubkeyDb != nil {
		data, err := c.pubkeyDb.Get(pubkey[:], nil)
		if err != nil {
			return 0, false
		}

		index, err := strconv.ParseUint(string(data), 10, 64)
		if err != nil {
			return 0, false
		}

		return phase0.ValidatorIndex(index), true
	} else {
		c.pubkeyMutex.RLock()
		defer c.pubkeyMutex.RUnlock()

		index, exists := c.pubkeyMap[pubkey]
		return index, exists
	}
}
