// Copyright (c) 2018-2022, The Decred developers
// Copyright (c) 2017, The dcrdata developers
// See LICENSE for details.

package dcrpg

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcd/btcjson"
	"github.com/btcsuite/btcd/btcutil"
	btc_chaincfg "github.com/btcsuite/btcd/chaincfg"
	btc_chainhash "github.com/btcsuite/btcd/chaincfg/chainhash"
	btcClient "github.com/btcsuite/btcd/rpcclient"
	btcwire "github.com/btcsuite/btcd/wire"
	"github.com/decred/dcrd/blockchain/stake/v5"
	"github.com/decred/dcrd/blockchain/standalone/v2"
	"github.com/decred/dcrd/chaincfg/chainhash"
	"github.com/decred/dcrd/chaincfg/v3"
	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"github.com/decred/dcrd/dcrutil/v4"
	chainjson "github.com/decred/dcrd/rpc/jsonrpc/types/v4"
	"github.com/decred/dcrd/rpcclient/v8"
	"github.com/decred/dcrd/txscript/v4"
	"github.com/decred/dcrd/txscript/v4/stdaddr"
	"github.com/decred/dcrd/txscript/v4/stdscript"
	"github.com/decred/dcrd/wire"
	apitypes "github.com/decred/dcrdata/v8/api/types"
	"github.com/decred/dcrdata/v8/blockdata"
	"github.com/decred/dcrdata/v8/blockdata/blockdatabtc"
	"github.com/decred/dcrdata/v8/blockdata/blockdataltc"
	"github.com/decred/dcrdata/v8/db/cache"
	"github.com/decred/dcrdata/v8/db/dbtypes"
	exptypes "github.com/decred/dcrdata/v8/explorer/types"
	"github.com/decred/dcrdata/v8/mempool"
	"github.com/decred/dcrdata/v8/mempool/mempoolbtc"
	"github.com/decred/dcrdata/v8/mempool/mempoolltc"
	"github.com/decred/dcrdata/v8/mutilchain"
	"github.com/decred/dcrdata/v8/mutilchain/btcrpcutils"
	"github.com/decred/dcrdata/v8/mutilchain/externalapi"
	"github.com/decred/dcrdata/v8/mutilchain/ltcrpcutils"
	"github.com/decred/dcrdata/v8/rpcutils"
	"github.com/decred/dcrdata/v8/stakedb"
	"github.com/decred/dcrdata/v8/trylock"
	"github.com/decred/dcrdata/v8/txhelpers"
	"github.com/decred/dcrdata/v8/txhelpers/btctxhelper"
	"github.com/decred/dcrdata/v8/txhelpers/ltctxhelper"
	humanize "github.com/dustin/go-humanize"
	"github.com/lib/pq"
	ltcjson "github.com/ltcsuite/ltcd/btcjson"
	ltc_chaincfg "github.com/ltcsuite/ltcd/chaincfg"
	ltc_chainhash "github.com/ltcsuite/ltcd/chaincfg/chainhash"
	"github.com/ltcsuite/ltcd/ltcutil"
	ltcClient "github.com/ltcsuite/ltcd/rpcclient"
	ltcwire "github.com/ltcsuite/ltcd/wire"

	"github.com/decred/dcrdata/db/dcrpg/v8/internal"
	"github.com/decred/dcrdata/db/dcrpg/v8/internal/mutilchainquery"
	"github.com/decred/dcrdata/v8/utils"
)

var (
	zeroHash            = chainhash.Hash{}
	zeroHashStringBytes = []byte(chainhash.Hash{}.String())
)

const dcrToAtoms = 1e8

type retryError struct{}

// Error implements Stringer for retryError.
func (s retryError) Error() string {
	return "retry"
}

// IsRetryError checks if an error is a retryError type.
func IsRetryError(err error) bool {
	_, isRetryErr := err.(retryError)
	return isRetryErr
}

// storedAgendas holds the current state of agenda data already in the db.
// This helps track changes in the lockedIn and activated heights when they
// happen without making too many db accesses everytime we are updating the
// agenda_votes table.
var storedAgendas map[string]dbtypes.MileStone

// ticketPoolDataCache stores the most recent ticketpool graphs information
// fetched to minimize the possibility of making multiple queries to the db
// fetching the same information.
type ticketPoolDataCache struct {
	sync.RWMutex
	Height          map[dbtypes.TimeBasedGrouping]int64
	TimeGraphCache  map[dbtypes.TimeBasedGrouping]*dbtypes.PoolTicketsData
	PriceGraphCache map[dbtypes.TimeBasedGrouping]*dbtypes.PoolTicketsData
	// DonutGraphCache persist data for the Number of tickets outputs pie chart.
	DonutGraphCache map[dbtypes.TimeBasedGrouping]*dbtypes.PoolTicketsData
}

// ticketPoolGraphsCache persists the latest ticketpool data queried from the db.
var ticketPoolGraphsCache = &ticketPoolDataCache{
	Height:          make(map[dbtypes.TimeBasedGrouping]int64),
	TimeGraphCache:  make(map[dbtypes.TimeBasedGrouping]*dbtypes.PoolTicketsData),
	PriceGraphCache: make(map[dbtypes.TimeBasedGrouping]*dbtypes.PoolTicketsData),
	DonutGraphCache: make(map[dbtypes.TimeBasedGrouping]*dbtypes.PoolTicketsData),
}

// TicketPoolData is a thread-safe way to access the ticketpool graphs data
// stored in the cache.
func TicketPoolData(interval dbtypes.TimeBasedGrouping, height int64) (timeGraph *dbtypes.PoolTicketsData,
	priceGraph *dbtypes.PoolTicketsData, donutChart *dbtypes.PoolTicketsData, actualHeight int64, intervalFound, isStale bool) {
	ticketPoolGraphsCache.RLock()
	defer ticketPoolGraphsCache.RUnlock()

	var tFound, pFound, dFound bool
	timeGraph, tFound = ticketPoolGraphsCache.TimeGraphCache[interval]
	priceGraph, pFound = ticketPoolGraphsCache.PriceGraphCache[interval]
	donutChart, dFound = ticketPoolGraphsCache.DonutGraphCache[interval]
	intervalFound = tFound && pFound && dFound

	actualHeight = ticketPoolGraphsCache.Height[interval]
	isStale = actualHeight != height

	return
}

// UpdateTicketPoolData updates the ticket pool cache with the latest data fetched.
// This is a thread-safe way to update ticket pool cache data. TryLock helps avoid
// stacking calls to update the cache.
func UpdateTicketPoolData(interval dbtypes.TimeBasedGrouping, timeGraph *dbtypes.PoolTicketsData,
	priceGraph *dbtypes.PoolTicketsData, donutcharts *dbtypes.PoolTicketsData, height int64) {
	if interval >= dbtypes.NumIntervals {
		return
	}

	ticketPoolGraphsCache.Lock()
	defer ticketPoolGraphsCache.Unlock()

	ticketPoolGraphsCache.Height[interval] = height
	ticketPoolGraphsCache.TimeGraphCache[interval] = timeGraph
	ticketPoolGraphsCache.PriceGraphCache[interval] = priceGraph
	ticketPoolGraphsCache.DonutGraphCache[interval] = donutcharts
}

// utxoStore provides a UTXOData cache with thread-safe get/set methods.
type utxoStore struct {
	sync.Mutex
	c map[string]map[uint32]*dbtypes.UTXOData
}

// newUtxoStore constructs a new utxoStore.
func newUtxoStore(prealloc int) utxoStore {
	return utxoStore{
		c: make(map[string]map[uint32]*dbtypes.UTXOData, prealloc),
	}
}

// Get attempts to locate UTXOData for the specified outpoint. If the data is
// not in the cache, a nil pointer and false are returned. If the data is
// located, the data and true are returned, and the data is evicted from cache.
func (u *utxoStore) Get(txHash string, txIndex uint32) (*dbtypes.UTXOData, bool) {
	u.Lock()
	defer u.Unlock()
	utxoData, ok := u.c[txHash][txIndex]
	if ok {
		u.c[txHash][txIndex] = nil
		delete(u.c[txHash], txIndex)
		if len(u.c[txHash]) == 0 {
			delete(u.c, txHash)
		}
	}
	return utxoData, ok
}

func (u *utxoStore) Peek(txHash string, txIndex uint32) *dbtypes.UTXOData {
	u.Lock()
	defer u.Unlock()
	txVals, ok := u.c[txHash]
	if !ok {
		return nil
	}
	return txVals[txIndex]
}

func (u *utxoStore) set(txHash string, txIndex uint32, voutDbID int64, addrs []string, val int64, mixed bool) {
	txUTXOVals, ok := u.c[txHash]
	if !ok {
		u.c[txHash] = map[uint32]*dbtypes.UTXOData{
			txIndex: {
				Addresses: addrs,
				Value:     val,
				Mixed:     mixed,
				VoutDbID:  voutDbID,
			},
		}
	} else {
		txUTXOVals[txIndex] = &dbtypes.UTXOData{
			Addresses: addrs,
			Value:     val,
			Mixed:     mixed,
			VoutDbID:  voutDbID,
		}
	}
}

// Set stores the addresses and amount in a UTXOData entry in the cache for the
// given outpoint.
func (u *utxoStore) Set(txHash string, txIndex uint32, voutDbID int64, addrs []string, val int64, mixed bool) {
	u.Lock()
	defer u.Unlock()
	u.set(txHash, txIndex, voutDbID, addrs, val, mixed)
}

// Reinit re-initializes the utxoStore with the given UTXOs.
func (u *utxoStore) Reinit(utxos []dbtypes.UTXO) {
	if len(utxos) == 0 {
		return
	}
	u.Lock()
	defer u.Unlock()
	// Pre-allocate the transaction hash map assuming the number of unique
	// transaction hashes in input is roughly 2/3 of the number of UTXOs.
	prealloc := 2 * len(utxos) / 3
	u.c = make(map[string]map[uint32]*dbtypes.UTXOData, prealloc)
	for i := range utxos {
		u.set(utxos[i].TxHash, utxos[i].TxIndex, utxos[i].VoutDbID, utxos[i].Addresses, utxos[i].Value, utxos[i].Mixed)
	}
}

// Size returns the size of the utxo cache in number of UTXOs.
func (u *utxoStore) Size() (sz int) {
	u.Lock()
	defer u.Unlock()
	for _, m := range u.c {
		sz += len(m)
	}
	return
}

type cacheLocks struct {
	bal        *cache.CacheLock
	rows       *cache.CacheLock
	rowsMerged *cache.CacheLock
	utxo       *cache.CacheLock
}

// BlockGetter implements a few basic blockchain data retrieval functions. It is
// like rpcutils.BlockFetcher except that it must also implement
// GetBlockChainInfo.
type BlockGetter interface {
	// rpcutils.BlockFetcher implements GetBestBlock, GetBlock, GetBlockHash,
	// and GetBlockHeaderVerbose.
	rpcutils.BlockFetcher

	// GetBlockChainInfo is required for a legacy upgrade involving agendas.
	GetBlockChainInfo(ctx context.Context) (*chainjson.GetBlockChainInfoResult, error)
	GetRawTransactionVerbose(ctx context.Context, txHash *chainhash.Hash) (*chainjson.TxRawResult, error)
}

// ChainDB provides an interface for storing and manipulating extracted
// blockchain data in a PostgreSQL database.
type ChainDB struct {
	ctx                context.Context
	queryTimeout       time.Duration
	db                 *sql.DB
	mp                 rpcutils.MempoolAddressChecker
	ltcMp              ltcrpcutils.MempoolAddressChecker
	btcMp              btcrpcutils.MempoolAddressChecker
	chainParams        *chaincfg.Params
	ltcChainParams     *ltc_chaincfg.Params
	btcChainParams     *btc_chaincfg.Params
	devAddress         string
	dupChecks          bool
	ltcDupChecks       bool
	btcDupChecks       bool
	bestBlock          *BestBlock
	LtcBestBlock       *MutilchainBestBlock
	BtcBestBlock       *MutilchainBestBlock
	lastBlock          map[chainhash.Hash]uint64
	ltcLastBlock       map[ltc_chainhash.Hash]uint64
	btcLastBlock       map[btc_chainhash.Hash]uint64
	ChainDisabledMap   map[string]bool
	stakeDB            *stakedb.StakeDatabase
	unspentTicketCache *TicketTxnIDGetter
	AddressCache       *cache.AddressCache
	CacheLocks         cacheLocks
	devPrefetch        bool
	InBatchSync        bool
	InReorg            bool
	tpUpdatePermission map[dbtypes.TimeBasedGrouping]*trylock.Mutex
	utxoCache          utxoStore
	ltcUtxoCache       utxoStore
	btcUtxoCache       utxoStore
	mixSetDiffsMtx     sync.Mutex
	mixSetDiffs        map[uint32]int64 // height to value diff
	deployments        *ChainDeployments
	MPC                *mempool.DataCache
	LTCMPC             *mempoolltc.DataCache
	BTCMPC             *mempoolbtc.DataCache
	// BlockCache stores apitypes.BlockDataBasic and apitypes.StakeInfoExtended
	// in StoreBlock for quick retrieval without a DB query.
	BlockCache             *apitypes.APICache
	heightClients          []chan uint32
	ltcHeightClients       []chan uint32
	btcHeightClients       []chan uint32
	shutdownDcrdata        func()
	Client                 *rpcclient.Client
	LtcClient              *ltcClient.Client
	BtcClient              *btcClient.Client
	tipMtx                 sync.Mutex
	tipSummary             *apitypes.BlockDataBasic
	ChainDBDisabled        bool
	OkLinkAPIKey           string
	BTC20BlocksSyncing     bool
	LTC20BlocksSyncing     bool
	AddressSummarySyncing  bool
	TreasurySummarySyncing bool
	lastExplorerBlock      struct {
		sync.Mutex
		hash      string
		blockInfo *exptypes.BlockInfo
		// Somewhat unrelated, difficulties is a map of timestamps to mining
		// difficulties. It is in this cache struct since these values are
		// commonly retrieved when the explorer block is updated.
		difficulties map[int64]float64
	}
	btcLastExplorerBlock struct {
		sync.Mutex
		hash      string
		blockInfo *exptypes.BlockInfo
		// Somewhat unrelated, difficulties is a map of timestamps to mining
		// difficulties. It is in this cache struct since these values are
		// commonly retrieved when the explorer block is updated.
		difficulties map[int64]float64
	}
	ltcLastExplorerBlock struct {
		sync.Mutex
		hash      string
		blockInfo *exptypes.BlockInfo
		// Somewhat unrelated, difficulties is a map of timestamps to mining
		// difficulties. It is in this cache struct since these values are
		// commonly retrieved when the explorer block is updated.
		difficulties map[int64]float64
	}
}

// ChainDeployments is mutex-protected blockchain deployment data.
type ChainDeployments struct {
	mtx          sync.RWMutex
	chainInfo    *dbtypes.BlockChainData
	btcChainInfo *dbtypes.BlockChainData
	ltcChainInfo *dbtypes.BlockChainData
}

// BestBlock is mutex-protected block hash and height.
type BestBlock struct {
	mtx    sync.RWMutex
	height int64
	hash   string
}

type MutilchainBestBlock struct {
	Mtx    sync.RWMutex
	Height int64
	Hash   string
	Time   int64
}

func (pgb *ChainDB) timeoutError() string {
	return fmt.Sprintf("%s after %v", dbtypes.TimeoutPrefix, pgb.queryTimeout)
}

// replaceCancelError will replace the generic error strings that can occur when
// a PG query is canceled (dbtypes.PGCancelError) or a context deadline is
// exceeded (dbtypes.CtxDeadlineExceeded from context.DeadlineExceeded). It also
// replaces a sql.ErrNoRows with a dbtypes.ErrNoResult.
func (pgb *ChainDB) replaceCancelError(err error) error {
	if err == nil {
		return err
	}

	if errors.Is(err, sql.ErrNoRows) {
		return dbtypes.ErrNoResult
	}

	patched := err.Error()
	if strings.Contains(patched, dbtypes.PGCancelError) {
		patched = strings.Replace(patched, dbtypes.PGCancelError,
			pgb.timeoutError(), -1)
	} else if strings.Contains(patched, dbtypes.CtxDeadlineExceeded) {
		patched = strings.Replace(patched, dbtypes.CtxDeadlineExceeded,
			pgb.timeoutError(), -1)
	} else {
		return err
	}
	return errors.New(patched)
}

// MissingSideChainBlocks identifies side chain blocks that are missing from the
// DB. Side chains known to dcrd are listed via the getchaintips RPC. Each block
// presence in the postgres DB is checked, and any missing block is returned in
// a SideChain along with a count of the total number of missing blocks.
func (pgb *ChainDB) MissingSideChainBlocks() ([]dbtypes.SideChain, int, error) {
	// First get the side chain tips (head blocks).
	tips, err := rpcutils.SideChains(pgb.Client)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to get chain tips from node: %w", err)
	}
	nSideChains := len(tips)

	// Build a list of all the blocks in each side chain that are not
	// already in the database.
	blocksToStore := make([]dbtypes.SideChain, nSideChains)
	var nSideChainBlocks int
	for it := range tips {
		sideHeight := tips[it].Height
		log.Tracef("Getting full side chain with tip %s at %d.", tips[it].Hash, sideHeight)

		sideChain, err := rpcutils.SideChainFull(pgb.Client, tips[it].Hash)
		if err != nil {
			return nil, 0, fmt.Errorf("unable to get side chain blocks for chain tip %s: %w",
				tips[it].Hash, err)
		}
		// Starting height is the lowest block in the side chain.
		sideHeight -= int64(len(sideChain)) - 1

		// For each block in the side chain, check if it already stored.
		for is := range sideChain {
			// Check for the block hash in the DB.
			sideHeightDB, err := pgb.BlockHeight(sideChain[is])
			if errors.Is(err, dbtypes.ErrNoResult) {
				// This block is NOT already in the DB.
				blocksToStore[it].Hashes = append(blocksToStore[it].Hashes, sideChain[is])
				blocksToStore[it].Heights = append(blocksToStore[it].Heights, sideHeight)
				nSideChainBlocks++
			} else if err == nil {
				// This block is already in the DB.
				log.Tracef("Found block %s in postgres at height %d.",
					sideChain[is], sideHeightDB)
				if sideHeight != sideHeightDB {
					log.Errorf("Side chain block height %d, expected %d.",
						sideHeightDB, sideHeight)
				}
			} else /* err != nil && err != sql.ErrNoRows */ {
				// Unexpected error
				log.Errorf("Failed to retrieve block %s: %v", sideChain[is], err)
			}

			// Next block
			sideHeight++
		}
	}

	return blocksToStore, nSideChainBlocks, nil
}

// TicketTxnIDGetter provides a cache for DB row IDs of tickets.
type TicketTxnIDGetter struct {
	mtx     sync.RWMutex
	idCache map[string]uint64
	db      *sql.DB
}

// TxnDbID fetches DB row ID for the ticket specified by the input transaction
// hash. A cache is checked first. In the event of a cache hit, the DB ID is
// returned and deleted from the internal cache. In the event of a cache miss,
// the database is queried. If the database query fails, the error is non-nil.
func (t *TicketTxnIDGetter) TxnDbID(txid string, expire bool) (uint64, error) {
	if t == nil {
		panic("You're using an uninitialized TicketTxnIDGetter")
	}
	t.mtx.RLock()
	dbID, ok := t.idCache[txid]
	t.mtx.RUnlock()
	if ok {
		if expire {
			t.mtx.Lock()
			delete(t.idCache, txid)
			t.mtx.Unlock()
		}
		return dbID, nil
	}
	// Cache miss. Get the row id by hash from the tickets table.
	log.Tracef("Cache miss for %s.", txid)
	return RetrieveTicketIDByHashNoCancel(t.db, txid)
}

// Set stores the (transaction hash, DB row ID) pair a map for future access.
func (t *TicketTxnIDGetter) Set(txid string, txDbID uint64) {
	if t == nil {
		return
	}
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.idCache[txid] = txDbID
}

// SetN stores several (transaction hash, DB row ID) pairs in the map.
func (t *TicketTxnIDGetter) SetN(txid []string, txDbID []uint64) {
	if t == nil {
		return
	}
	t.mtx.Lock()
	defer t.mtx.Unlock()
	for i := range txid {
		t.idCache[txid[i]] = txDbID[i]
	}
}

// NewTicketTxnIDGetter constructs a new TicketTxnIDGetter with an empty cache.
func NewTicketTxnIDGetter(db *sql.DB) *TicketTxnIDGetter {
	return &TicketTxnIDGetter{
		db:      db,
		idCache: make(map[string]uint64),
	}
}

// DBInfo holds the PostgreSQL database connection information.
type DBInfo struct {
	Host, Port, User, Pass, DBName string
	QueryTimeout                   time.Duration
}

type ChainDBCfg struct {
	DBi                               *DBInfo
	Params                            *chaincfg.Params
	LTCParams                         *ltc_chaincfg.Params
	BTCParams                         *btc_chaincfg.Params
	DevPrefetch, HidePGConfig         bool
	AddrCacheRowCap, AddrCacheAddrCap int
	AddrCacheUTXOByteCap              int
	ChainDBDisabled                   bool
	OkLinkAPIKey                      string
}

// The minimum required PostgreSQL version in integer format as returned by
// "SHOW server_version_num;" or "SELECT current_setting('server_version_num')".
// This is currently 11.0, as described in README.md.
const pgVerNumMin = 11_0000

// NewChainDB constructs a cancellation-capable ChainDB for the given connection
// and Decred network parameters. By default, duplicate row checks on insertion
// are enabled. See EnableDuplicateCheckOnInsert to change this behavior.
func NewChainDB(ctx context.Context, cfg *ChainDBCfg, stakeDB *stakedb.StakeDatabase,
	mp rpcutils.MempoolAddressChecker, client *rpcclient.Client, shutdown func()) (*ChainDB, error) {
	// Connect to the PostgreSQL daemon and return the *sql.DB.
	dbi := cfg.DBi
	db, err := Connect(dbi.Host, dbi.Port, dbi.User, dbi.Pass, dbi.DBName)
	if err != nil {
		return nil, err
	}

	// Put the PostgreSQL time zone in UTC.
	var initTZ string
	initTZ, err = CheckCurrentTimeZone(db)
	if err != nil {
		return nil, err
	}
	if initTZ != "UTC" {
		log.Infof("Switching PostgreSQL time zone to UTC for this session.")
		if _, err = db.Exec(`SET TIME ZONE UTC`); err != nil {
			return nil, fmt.Errorf("Failed to set time zone to UTC: %w", err)
		}
	}

	pgVersion, pgVerNum, err := RetrievePGVersion(db)
	if err != nil {
		return nil, err
	}
	log.Info(pgVersion)
	if pgVerNum < pgVerNumMin {
		return nil, fmt.Errorf("PostgreSQL version %d.%d or greater is required; got %d.%d",
			pgVerNumMin/10_000, pgVerNumMin%10_000, pgVerNum/10_000, pgVerNum%10_000)
	}

	// Optionally logs the PostgreSQL configuration.
	if !cfg.HidePGConfig {
		perfSettings, err := RetrieveSysSettingsPerformance(db)
		if err != nil {
			return nil, err
		}
		log.Infof("postgres configuration settings:\n%v", perfSettings)

		servSettings, err := RetrieveSysSettingsServer(db)
		if err != nil {
			return nil, err
		}
		log.Infof("postgres server settings:\n%v", servSettings)
	}

	// Check the synchronous_commit setting.
	syncCommit, err := RetrieveSysSettingSyncCommit(db)
	if err != nil {
		return nil, err
	}
	if syncCommit != "off" {
		log.Warnf(`PERFORMANCE ISSUE! The synchronous_commit setting is "%s". `+
			`Changing it to "off".`, syncCommit)
		// Turn off synchronous_commit.
		if err = SetSynchronousCommit(db, "off"); err != nil {
			return nil, fmt.Errorf("failed to set synchronous_commit: %w", err)
		}
		// Verify that the setting was changed.
		if syncCommit, err = RetrieveSysSettingSyncCommit(db); err != nil {
			return nil, err
		}
		if syncCommit != "off" {
			log.Errorf(`Failed to set synchronous_commit="off". Check PostgreSQL user permissions.`)
		}
	}

	params := cfg.Params
	ltcParams := cfg.LTCParams
	btcParams := cfg.BTCParams

	// Perform any necessary database schema upgrades.
	dbVer, compatAction, err := versionCheck(db)
	switch err {
	case nil:
		if compatAction == OK {
			// meta table present and no upgrades required
			log.Infof("DB schema version %v", dbVer)
			break
		}

		// Upgrades required
		if client == nil {
			return nil, fmt.Errorf("a rpcclient.Client is required for database upgrades")
		}
		// Do upgrades required by meta table versioning.
		log.Infof("DB schema version %v upgrading to version %v", dbVer, targetDatabaseVersion)
		upgrader := NewUpgrader(ctx, params, db, client, stakeDB)
		success, err := upgrader.UpgradeDatabase()
		if err != nil {
			return nil, fmt.Errorf("failed to upgrade database: %w", err)
		}
		if !success {
			return nil, fmt.Errorf("failed to upgrade database (upgrade not supported?)")
		}
	case tablesNotFoundErr:
		// Empty database (no blocks table). Proceed to setupTables.
		log.Infof(`Empty database "%s". Creating tables...`, dbi.DBName)
		if err = CreateTables(db); err != nil {
			return nil, fmt.Errorf("failed to create tables: %w", err)
		}
		//insert DCR meta data
		err = insertMetaData(db, &metaData{
			netName:         params.Name,
			currencyNet:     uint32(params.Net),
			bestBlockHeight: -1,
			dbVer:           *targetDatabaseVersion,
		})
		if err != nil {
			return nil, fmt.Errorf("insertMetaData failed: %w", err)
		}
	case metaNotFoundErr:
		log.Errorf("Legacy DB versioning found. No upgrade supported. Wipe all data and start fresh.")
	default:
		return nil, err
	}

	// Get the best block height from the blocks table.
	bestHeight, bestHash, err := RetrieveBestBlock(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("RetrieveBestBlock: %w", err)
	}
	// NOTE: Once legacy versioned tables are no longer in use, use the height
	// and hash from DBBestBlock instead.

	// Verify that the best
	// block in the meta table is the same as in the blocks table. If the blocks
	// table is ahead of the meta table, it is likely that the data for the best
	// block was not fully inserted into all tables. Purge data back to the meta
	// table's best block height. Also purge if the hashes do not match.
	dbHash, dbHeightInit, err := DBBestBlock(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("DBBestBlock: %w", err)
	}

	// Best block height in the transactions table (written to even before
	// the blocks table).
	bestTxsBlockHeight, bestTxsBlockHash, err :=
		RetrieveTxsBestBlockMainchain(ctx, db)
	if err != nil {
		return nil, err
	}

	if bestTxsBlockHeight > bestHeight {
		bestHeight = bestTxsBlockHeight
		bestHash = bestTxsBlockHash
	}

	// The meta table's best block height should never end up larger than
	// the blocks table's best block height, but purge a block anyway since
	// something went awry. This will update the best block in the meta
	// table to match the blocks table, allowing dcrdata to start.
	if dbHeightInit > bestHeight {
		log.Warnf("Best block height in meta table (%d) "+
			"greater than best height in blocks table (%d)!",
			dbHeightInit, bestHeight)
		_, bestHeight, bestHash, err = DeleteBestBlock(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("DeleteBestBlock: %w", err)
		}
		dbHash, dbHeightInit, err = DBBestBlock(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("DBBestBlock: %w", err)
		}
	}

	// Purge blocks if the best block hashes do not match, and until the
	// best block height in the data tables is less than or equal to the
	// starting height in the meta table.
	log.Debugf("meta height %d / blocks height %d", dbHeightInit, bestHeight)
	for dbHash != bestHash || dbHeightInit < bestHeight {
		log.Warnf("Purging best block %s (%d).", bestHash, bestHeight)

		// Delete the best block across all tables, updating the best block
		// in the meta table.
		_, bestHeight, bestHash, err = DeleteBestBlock(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("DeleteBestBlock: %w", err)
		}
		if bestHeight == -1 {
			break
		}

		// Now dbHash must equal bestHash. If not, DeleteBestBlock failed to
		// update the meta table.
		dbHash, _, err = DBBestBlock(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("DBBestBlock: %w", err)
		}
		if dbHash != bestHash {
			return nil, fmt.Errorf("best block hash in meta and blocks tables do not match: "+
				"%s != %s", dbHash, bestHash)
		}
	}

	// Project fund address of the current network
	projectFundAddress, err := dbtypes.DevSubsidyAddress(params)
	if err != nil {
		log.Warnf("ChainDB.NewChainDB: %v", err)
	}

	log.Infof("Pre-loading unspent ticket info for InsertVote optimization.")
	unspentTicketCache := NewTicketTxnIDGetter(db)
	unspentTicketDbIDs, unspentTicketHashes, err := RetrieveUnspentTickets(ctx, db)
	if err != nil && !errors.Is(err, sql.ErrNoRows) && !strings.HasSuffix(err.Error(), "does not exist") {
		return nil, err
	}
	if len(unspentTicketDbIDs) != 0 {
		log.Infof("Storing data for %d unspent tickets in cache.", len(unspentTicketDbIDs))
		unspentTicketCache.SetN(unspentTicketHashes, unspentTicketDbIDs)
	}

	// For each chart grouping type create a non-blocking updater mutex.
	tpUpdatePermissions := make(map[dbtypes.TimeBasedGrouping]*trylock.Mutex)
	for g := range dbtypes.TimeBasedGroupings {
		tpUpdatePermissions[g] = new(trylock.Mutex)
	}

	// If a query timeout is not set (i.e. zero), default to 24 hrs for
	// essentially no timeout.
	queryTimeout := dbi.QueryTimeout
	if queryTimeout <= 0 {
		queryTimeout = time.Hour
	}

	log.Infof("Setting PostgreSQL DB statement timeout to %v.", queryTimeout)

	bestBlock := &BestBlock{
		height: bestHeight,
		hash:   bestHash,
	}

	// Create the address cache with the given capacity. The project fund
	// address is set to prevent purging its data when cache reaches capacity.
	addrCache := cache.NewAddressCache(cfg.AddrCacheRowCap, cfg.AddrCacheAddrCap,
		cfg.AddrCacheUTXOByteCap)
	addrCache.ProjectAddress = projectFundAddress

	//init ltc address cache
	chainDB := &ChainDB{
		ctx:                ctx,
		queryTimeout:       queryTimeout,
		db:                 db,
		mp:                 mp,
		chainParams:        params,
		ltcChainParams:     ltcParams,
		btcChainParams:     btcParams,
		devAddress:         projectFundAddress,
		dupChecks:          true,
		ltcDupChecks:       true,
		btcDupChecks:       true,
		bestBlock:          bestBlock,
		lastBlock:          make(map[chainhash.Hash]uint64),
		ltcLastBlock:       make(map[ltc_chainhash.Hash]uint64),
		btcLastBlock:       make(map[btc_chainhash.Hash]uint64),
		ChainDisabledMap:   make(map[string]bool, 0),
		stakeDB:            stakeDB,
		unspentTicketCache: unspentTicketCache,
		AddressCache:       addrCache,
		CacheLocks:         cacheLocks{cache.NewCacheLock(), cache.NewCacheLock(), cache.NewCacheLock(), cache.NewCacheLock()},
		devPrefetch:        cfg.DevPrefetch,
		tpUpdatePermission: tpUpdatePermissions,
		utxoCache:          newUtxoStore(5e4),
		ltcUtxoCache:       newUtxoStore(5e4),
		btcUtxoCache:       newUtxoStore(5e4),
		mixSetDiffs:        make(map[uint32]int64),
		deployments:        new(ChainDeployments),
		MPC:                new(mempool.DataCache),
		LTCMPC:             new(mempoolltc.DataCache),
		BTCMPC:             new(mempoolbtc.DataCache),
		BlockCache:         apitypes.NewAPICache(1e4),
		heightClients:      make([]chan uint32, 0),
		shutdownDcrdata:    shutdown,
		Client:             client,
		ChainDBDisabled:    cfg.ChainDBDisabled,
		OkLinkAPIKey:       cfg.OkLinkAPIKey,
	}
	chainDB.lastExplorerBlock.difficulties = make(map[int64]float64)
	// Update the current chain state in the ChainDB
	if client != nil {
		bci, err := chainDB.BlockchainInfo()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch the latest blockchain info")
		}
		chainDB.UpdateChainState(bci)
	}

	return chainDB, nil
}

// Close closes the underlying sql.DB connection to the database.
func (pgb *ChainDB) Close() error {
	return pgb.db.Close()
}

// SqlDB returns the underlying sql.DB, which should not be used directly unless
// you know what you are doing (if you have to ask...).
func (pgb *ChainDB) SqlDB() *sql.DB {
	return pgb.db
}

// InitUtxoCache resets the UTXO cache with the given slice of UTXO data.
func (pgb *ChainDB) InitUtxoCache(utxos []dbtypes.UTXO) {
	pgb.utxoCache.Reinit(utxos)
}

// UseStakeDB is used to assign a stakedb.StakeDatabase for ticket tracking.
// This may be useful when it is necessary to construct a ChainDB prior to
// creating or loading a StakeDatabase, such as when dropping tables.
func (pgb *ChainDB) UseStakeDB(stakeDB *stakedb.StakeDatabase) {
	pgb.stakeDB = stakeDB
}

// UseMempoolChecker assigns a MempoolAddressChecker for searching mempool for
// transactions involving a certain address.
func (pgb *ChainDB) UseMempoolChecker(mp rpcutils.MempoolAddressChecker) {
	pgb.mp = mp
}

func (pgb *ChainDB) UseLTCMempoolChecker(mp ltcrpcutils.MempoolAddressChecker) {
	pgb.ltcMp = mp
}

func (pgb *ChainDB) UseBTCMempoolChecker(mp btcrpcutils.MempoolAddressChecker) {
	pgb.btcMp = mp
}

// EnableDuplicateCheckOnInsert specifies whether SQL insertions should check
// for row conflicts (duplicates), and avoid adding or updating.
func (pgb *ChainDB) EnableDuplicateCheckOnInsert(dupCheck bool) {
	if pgb == nil {
		return
	}
	pgb.dupChecks = dupCheck
}

func (pgb *ChainDB) GetMutilchainBestDBBlock(ctx context.Context, chainType string) (height int64, hash string, err error) {
	return RetrieveMutilchainBestBlock(ctx, pgb.db, chainType)
}

// EnableDuplicateCheckOnInsert specifies whether SQL insertions should check
// for row conflicts (duplicates), and avoid adding or updating.
func (pgb *ChainDB) MutilchainEnableDuplicateCheckOnInsert(dupCheck bool, chainType string) {
	if pgb == nil {
		return
	}
	switch chainType {
	case mutilchain.TYPELTC:
		pgb.ltcDupChecks = dupCheck
	case mutilchain.TYPEBTC:
		pgb.btcDupChecks = dupCheck
	default:
		pgb.dupChecks = dupCheck
	}
}

var (
	// metaNotFoundErr is the error from versionCheck when the meta table does
	// not exist.
	metaNotFoundErr = errors.New("meta table not found")

	// tablesNotFoundErr is the error from versionCheck when any of the tables
	// do not exist.
	tablesNotFoundErr = errors.New("tables not found")
)

// versionCheck attempts to retrieve the database version from the meta table,
// along with a CompatAction upgrade plan. If any of the regular data tables do
// not exist, a tablesNotFoundErr error is returned to indicated that the tables
// do not exist (or are partially created.) If the data tables exist but the
// meta table does not exist, a metaNotFoundErr error is returned to indicate
// that the legacy table versioning system is in use.
func versionCheck(db *sql.DB) (*DatabaseVersion, CompatAction, error) {
	// Detect an empty database, only checking for the "blocks" table since some
	// of the tables are created by schema upgrades.
	exists, err := TableExists(db, "blocks")
	if err != nil {
		return nil, Unknown, err
	}
	if !exists {
		return nil, Unknown, tablesNotFoundErr
	}

	// The meta table stores the database schema version.
	exists, err = TableExists(db, "meta")
	if err != nil {
		return nil, Unknown, err
	}
	// If there is no meta table, this could indicate the legacy table
	// versioning system is still in used. Return the MetaNotFoundErr error.
	if !exists {
		return nil, Unknown, metaNotFoundErr
	}

	// Retrieve the database version from the meta table.
	dbVer, err := DBVersion(db)
	if err != nil {
		return nil, Unknown, fmt.Errorf("DBVersion failure: %w", err)
	}

	// Return the version, and an upgrade plan to reach targetDatabaseVersion.
	return &dbVer, dbVer.NeededToReach(targetDatabaseVersion), nil
}

func (pgb *ChainDB) CheckTableExist(table string) (bool, error) {
	exists, err := TableExists(pgb.db, table)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (pgb *ChainDB) MutilchainCheckAndCreateTable(chainType string) error {
	exists, err := TableExists(pgb.db, fmt.Sprintf("%sblocks", chainType))
	if err != nil {
		return err
	}
	if !exists {
		//Create type
		if err := CreateMutilchainTypes(pgb.db, chainType); err != nil {
			return err
		}
		// Empty database (no blocks table). Proceed to setupTables.
		log.Infof(`tables of %s empty. Creating tables...`, chainType)
		if err = CreateMutilchainTables(pgb.db, chainType); err != nil {
			return fmt.Errorf("failed to create tables: %w", err)
		}
	}
	return nil
}

func (pgb *ChainDB) CheckAndCreateCoinAgeTable() error {
	exists, err := TableExists(pgb.db, "coin_age")
	if err != nil {
		return err
	}
	if !exists {
		// Empty database (no blocks table). Proceed to setupTables.
		log.Infof(`tables of %s empty. Creating tables...`, "coin_age")
		if err = createTable(pgb.db, "coin_age", internal.CreateCoinAgeTable); err != nil {
			return fmt.Errorf("failed to create tables: %w", err)
		}
	}
	return nil
}

func (pgb *ChainDB) CheckAndCreateUtxoHistoryTable() error {
	exists, err := TableExists(pgb.db, "utxo_history")
	if err != nil {
		return err
	}
	if !exists {
		// Empty database (no blocks table). Proceed to setupTables.
		log.Infof(`tables of %s empty. Creating tables...`, "utxo_history")
		if err = createTable(pgb.db, "utxo_history", internal.CreateUtxoHistoryTable); err != nil {
			return fmt.Errorf("failed to create tables: %w", err)
		}
	}
	return nil
}

func (pgb *ChainDB) CheckAndCreateCoinAgeBandsTable() error {
	exists, err := TableExists(pgb.db, "coin_age_bands")
	if err != nil {
		return err
	}
	if !exists {
		// Empty database (no blocks table). Proceed to setupTables.
		log.Infof(`tables of %s empty. Creating tables...`, "coin_age_bands")
		if err = createTable(pgb.db, "coin_age_bands", internal.CreateCoinAgeBandsTable); err != nil {
			return fmt.Errorf("failed to create tables: %w", err)
		}
	}
	return nil
}

// DropTables drops (deletes) all of the known dcrdata tables.
func (pgb *ChainDB) DropTables() {
	DropTables(pgb.db)
}

// SideChainBlocks retrieves all known side chain blocks.
func (pgb *ChainDB) SideChainBlocks() ([]*dbtypes.BlockStatus, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	scb, err := RetrieveSideChainBlocks(ctx, pgb.db)
	return scb, pgb.replaceCancelError(err)
}

// SideChainTips retrieves the tip/head block for all known side chains.
func (pgb *ChainDB) SideChainTips() ([]*dbtypes.BlockStatus, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	sct, err := RetrieveSideChainTips(ctx, pgb.db)
	return sct, pgb.replaceCancelError(err)
}

// DisapprovedBlocks retrieves all blocks disapproved by stakeholder votes.
func (pgb *ChainDB) DisapprovedBlocks() ([]*dbtypes.BlockStatus, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	disb, err := RetrieveDisapprovedBlocks(ctx, pgb.db)
	return disb, pgb.replaceCancelError(err)
}

// BlockStatus retrieves the block chain status of the specified block.
func (pgb *ChainDB) BlockStatus(hash string) (dbtypes.BlockStatus, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	bs, err := RetrieveBlockStatus(ctx, pgb.db, hash)
	return bs, pgb.replaceCancelError(err)
}

// BlockStatuses retrieves the block chain statuses of all blocks at the given
// height.
func (pgb *ChainDB) BlockStatuses(height int64) ([]*dbtypes.BlockStatus, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	blocks, err := RetrieveBlockStatuses(ctx, pgb.db, height)
	return blocks, pgb.replaceCancelError(err)
}

// blockFlags retrieves the block's isValid and isMainchain flags.
func (pgb *ChainDB) blockFlags(ctx context.Context, hash string) (bool, bool, error) {
	iv, im, err := RetrieveBlockFlags(ctx, pgb.db, hash)
	return iv, im, pgb.replaceCancelError(err)
}

// BlockFlags retrieves the block's isValid and isMainchain flags.
func (pgb *ChainDB) BlockFlags(hash string) (bool, bool, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	return pgb.blockFlags(ctx, hash)
}

// BlockFlagsNoCancel retrieves the block's isValid and isMainchain flags.
func (pgb *ChainDB) BlockFlagsNoCancel(hash string) (bool, bool, error) {
	return pgb.blockFlags(context.Background(), hash)
}

// blockChainDbID gets the row ID of the given block hash in the block_chain
// table. The cancellation context is used without timeout.
func (pgb *ChainDB) blockChainDbID(ctx context.Context, hash string) (dbID uint64, err error) {
	err = pgb.db.QueryRowContext(ctx, internal.SelectBlockChainRowIDByHash, hash).Scan(&dbID)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) RetrieveLegacyAddressCreditMonthRowIndex(year, month int) (index int64, err error) {
	err = pgb.db.QueryRowContext(pgb.ctx, internal.SelectCreditRowIndexByMonth, year, month).Scan(&index)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) RetrieveLegacyAddressCreditYearRowIndex(year int) (index int64, err error) {
	err = pgb.db.QueryRowContext(pgb.ctx, internal.SelectCreditRowIndexByYear, year).Scan(&index)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) RetrieveLegacyAddressDebitMonthRowIndex(year, month int) (index int64, err error) {
	err = pgb.db.QueryRowContext(pgb.ctx, internal.SelectDebitRowIndexByMonth, year, month).Scan(&index)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) RetrieveLegacyAddressDebitYearRowIndex(year int) (index int64, err error) {
	err = pgb.db.QueryRowContext(pgb.ctx, internal.SelectDebitRowIndexByYear, year).Scan(&index)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) RetrieveTreasuryCreditMonthRowIndex(year, month int) (index int64, err error) {
	err = pgb.db.QueryRowContext(pgb.ctx, internal.SelectTBaseRowIndexByMonth, year, month).Scan(&index)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) RetrieveTreasuryDebitMonthRowIndex(year, month int) (index int64, err error) {
	err = pgb.db.QueryRowContext(pgb.ctx, internal.SelectSpendRowIndexByMonth, year, month).Scan(&index)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) RetrieveCountLegacyCreditAddressRows() (count int64, err error) {
	projectFundAddress, addErr := dbtypes.DevSubsidyAddress(pgb.chainParams)
	if addErr != nil {
		log.Warnf("ChainDB.Get count Legacy credit address failed: %v", addErr)
		return 0, addErr
	}
	err = pgb.db.QueryRowContext(pgb.ctx, internal.CountCreditsRowByAddress, projectFundAddress).Scan(&count)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) RetrieveCountLegacyDebitAddressRows() (count int64, err error) {
	projectFundAddress, addErr := dbtypes.DevSubsidyAddress(pgb.chainParams)
	if addErr != nil {
		log.Warnf("ChainDB.Get count Legacy debit address failed: %v", addErr)
		return 0, addErr
	}
	err = pgb.db.QueryRowContext(pgb.ctx, internal.CountDebitRowByAddress, projectFundAddress).Scan(&count)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) RetrieveCountTreasuryCreditRows() (count int64, err error) {
	err = pgb.db.QueryRowContext(pgb.ctx, internal.CountTBaseRow).Scan(&count)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) RetrieveCountTreasuryDebitRows() (count int64, err error) {
	err = pgb.db.QueryRowContext(pgb.ctx, internal.CountSpendRow).Scan(&count)
	err = pgb.replaceCancelError(err)
	return
}

// BlockChainDbID gets the row ID of the given block hash in the block_chain
// table. The cancellation context is used without timeout.
func (pgb *ChainDB) BlockChainDbID(hash string) (dbID uint64, err error) {
	return pgb.blockChainDbID(pgb.ctx, hash)
}

// BlockChainDbIDNoCancel gets the row ID of the given block hash in the
// block_chain table. The cancellation context is used without timeout.
func (pgb *ChainDB) BlockChainDbIDNoCancel(hash string) (dbID uint64, err error) {
	return pgb.blockChainDbID(context.Background(), hash)
}

// RegisterCharts registers chart data fetchers and appenders with the provided
// ChartData.
func (pgb *ChainDB) RegisterCharts(charts *cache.ChartData) {
	charts.AddUpdater(cache.ChartUpdater{
		Tag:      "basic blocks",
		Fetcher:  pgb.chartBlocks,
		Appender: appendChartBlocks,
	})

	charts.AddUpdater(cache.ChartUpdater{
		Tag:      "coin supply",
		Fetcher:  pgb.coinSupply,
		Appender: appendCoinSupply,
	})

	charts.AddUpdater(cache.ChartUpdater{
		Tag:      "coin age",
		Fetcher:  pgb.coinAge,
		Appender: appendCoinAge,
	})

	// TODO: charts.AddUpdater(cache.ChartUpdater{
	// 	Tag:      "coin age bands",
	// 	Fetcher:  pgb.coinAgeBands,
	// 	Appender: appendCoinAgeBands,
	// })

	charts.AddUpdater(cache.ChartUpdater{
		Tag:      "window stats",
		Fetcher:  pgb.windowStats,
		Appender: appendWindowStats,
	})

	charts.AddUpdater(cache.ChartUpdater{
		Tag:      "missed votes stats",
		Fetcher:  pgb.missedVotesStats,
		Appender: appendMissedVotesPerWindow,
	})

	charts.AddUpdater(cache.ChartUpdater{
		Tag:      "fees",
		Fetcher:  pgb.blockFees,
		Appender: appendBlockFees,
	})

	charts.AddUpdater(cache.ChartUpdater{
		Tag:      "privacyParticipation",
		Fetcher:  pgb.privacyParticipation,
		Appender: appendPrivacyParticipation,
	})

	charts.AddUpdater(cache.ChartUpdater{
		Tag:      "anonymitySet",
		Fetcher:  pgb.anonymitySet,
		Appender: pgb.appendAnonymitySet, // ChainDB's method since it caches data.
	})

	charts.AddUpdater(cache.ChartUpdater{
		Tag:      "pool stats",
		Fetcher:  pgb.poolStats,
		Appender: appendPoolStats,
	})
}

func (pgb *ChainDB) RegisterMutilchainCharts(charts *cache.MutilchainChartData) {
	charts.AddUpdater(cache.ChartMutilchainUpdater{
		Tag:      "basic blocks",
		Fetcher:  pgb.chartMutilchainBlocks,
		Appender: appendMutilchainChartBlocks,
	})

	// TODO, uncomment in the future
	// charts.AddUpdater(cache.ChartMutilchainUpdater{
	// 	Tag:      "coin supply",
	// 	Fetcher:  pgb.mutilchainCoinSupply,
	// 	Appender: appendMutilchainCoinSupply,
	// })

	// charts.AddUpdater(cache.ChartMutilchainUpdater{
	// 	Tag:      "fees",
	// 	Fetcher:  pgb.mutilchainBlockFees,
	// 	Appender: appendMutilchainBlockFees,
	// })
}

// TransactionBlocks retrieves the blocks in which the specified transaction
// appears, along with the index of the transaction in each of the blocks. The
// next and previous block hashes are NOT SET in each BlockStatus.
func (pgb *ChainDB) TransactionBlocks(txHash string) ([]*dbtypes.BlockStatus, []uint32, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	hashes, heights, inds, valids, mainchains, err := RetrieveTxnsBlocks(ctx, pgb.db, txHash)
	if err != nil {
		return nil, nil, pgb.replaceCancelError(err)
	}

	blocks := make([]*dbtypes.BlockStatus, len(hashes))

	for i := range hashes {
		blocks[i] = &dbtypes.BlockStatus{
			IsValid:     valids[i],
			IsMainchain: mainchains[i],
			Height:      heights[i],
			Hash:        hashes[i],
			// Next and previous hash not set
		}
	}

	return blocks, inds, nil
}

func (pgb *ChainDB) MutilchainTransactionBlocks(txHash string, chainType string) ([]*dbtypes.BlockStatus, []uint32, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	hashes, heights, inds, err := RetrieveMutilchainTxnsBlocks(ctx, pgb.db, txHash, chainType)
	if err != nil {
		return nil, nil, pgb.replaceCancelError(err)
	}

	blocks := make([]*dbtypes.BlockStatus, len(hashes))

	for i := range hashes {
		blocks[i] = &dbtypes.BlockStatus{
			Height: heights[i],
			Hash:   hashes[i],
			// Next and previous hash not set
		}
	}

	return blocks, inds, nil
}

// HeightDB retrieves the best block height according to the meta table.
func (pgb *ChainDB) HeightDB() (int64, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, height, err := DBBestBlock(ctx, pgb.db)
	return height, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) MutilchainBlockHeight(hash string, chainType string) (int64, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	height, err := RetrieveMutilchainBlockHeight(ctx, pgb.db, hash, chainType)
	return height, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) MutilchainHeightDB(chainType string) (int64, error) {
	height, _, _, err := RetrieveMutilchainBestBlockHeight(pgb.db, chainType)
	return int64(height), err
}

// HashDB retrieves the best block hash according to the meta table.
func (pgb *ChainDB) HashDB() (string, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	hash, _, err := DBBestBlock(ctx, pgb.db)
	return hash, pgb.replaceCancelError(err)
}

// HeightHashDB retrieves the best block height and hash according to the meta
// table.
func (pgb *ChainDB) HeightHashDB() (int64, string, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	hash, height, err := DBBestBlock(ctx, pgb.db)
	return height, hash, pgb.replaceCancelError(err)
}

// HeightDBLegacy queries the blocks table for the best block height. When the
// tables are empty, the returned height will be -1.
func (pgb *ChainDB) HeightDBLegacy() (int64, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	bestHeight, _, _, err := RetrieveBestBlockHeight(ctx, pgb.db)
	height := int64(bestHeight)
	if errors.Is(err, sql.ErrNoRows) {
		height = -1
	}
	return height, pgb.replaceCancelError(err)
}

// HashDBLegacy queries the blocks table for the best block's hash.
func (pgb *ChainDB) HashDBLegacy() (string, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, bestHash, _, err := RetrieveBestBlockHeight(ctx, pgb.db)
	return bestHash, pgb.replaceCancelError(err)
}

// HeightHashDBLegacy queries the blocks table for the best block's height and
// hash.
func (pgb *ChainDB) HeightHashDBLegacy() (uint64, string, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	height, hash, _, err := RetrieveBestBlockHeight(ctx, pgb.db)
	return height, hash, pgb.replaceCancelError(err)
}

// Height is a getter for ChainDB.bestBlock.height.
func (pgb *ChainDB) Height() int64 {
	return pgb.bestBlock.Height()
}

func (pgb *ChainDB) MutilchainHeight(chainType string) int64 {
	switch chainType {
	case mutilchain.TYPELTC:
		return pgb.LtcBestBlock.MutilchainHeight()
	case mutilchain.TYPEBTC:
		return pgb.BtcBestBlock.MutilchainHeight()
	default:
		return pgb.bestBlock.Height()
	}
}

func (pgb *ChainDB) GetMutilchainBestBlock(chainType string) (int64, string) {
	switch chainType {
	case mutilchain.TYPELTC:
		if pgb.LtcBestBlock != nil {
			return pgb.LtcBestBlock.MutilchainHeightHash()
		}
		err := pgb.GetLTCBestBlock()
		if err != nil {
			return 0, ""
		}
		return pgb.LtcBestBlock.MutilchainHeightHash()
	case mutilchain.TYPEBTC:
		if pgb.BtcBestBlock != nil {
			return pgb.BtcBestBlock.MutilchainHeightHash()
		}
		err := pgb.GetBTCBestBlock()
		if err != nil {
			return 0, ""
		}
		return pgb.BtcBestBlock.MutilchainHeightHash()
	default:
		if pgb.bestBlock != nil {
			return pgb.bestBlock.HeightHash()
		}
		err := pgb.GetDecredBestBlock()
		if err != nil {
			return 0, ""
		}
		return pgb.bestBlock.HeightHash()
	}
}

func (pgb *ChainDB) MutilchainBestBlockTime(chainType string) int64 {
	switch chainType {
	case mutilchain.TYPELTC:
		return pgb.LtcBestBlock.MutilchainTime()
	case mutilchain.TYPEBTC:
		return pgb.BtcBestBlock.MutilchainTime()
	default:
		return 0
	}
}

// Height uses the last stored height.
func (block *BestBlock) Height() int64 {
	block.mtx.RLock()
	defer block.mtx.RUnlock()
	return block.height
}

func (block *BestBlock) HeightHash() (int64, string) {
	block.mtx.RLock()
	defer block.mtx.RUnlock()
	return block.height, block.hash
}

func (block *MutilchainBestBlock) MutilchainHeight() int64 {
	block.Mtx.RLock()
	defer block.Mtx.RUnlock()
	return block.Height
}

func (block *MutilchainBestBlock) MutilchainHeightHash() (int64, string) {
	block.Mtx.RLock()
	defer block.Mtx.RUnlock()
	return block.Height, block.Hash
}

func (block *MutilchainBestBlock) MutilchainTime() int64 {
	block.Mtx.RLock()
	defer block.Mtx.RUnlock()
	return block.Time
}

// GetHeight is for middleware DataSource compatibility. No DB query is
// performed; the last stored height is used.
func (pgb *ChainDB) GetHeight() (int64, error) {
	return pgb.Height(), nil
}

func (pgb *ChainDB) GetMutilchainHeight(chainType string) (int64, error) {
	return pgb.MutilchainHeight(chainType), nil
}

// GetBestBlockHash is for middleware DataSource compatibility. No DB query is
// performed; the last stored height is used.
func (pgb *ChainDB) GetBestBlockHash() (string, error) {
	return pgb.BestBlockHashStr(), nil
}

// HashStr uses the last stored block hash.
func (block *BestBlock) HashStr() string {
	block.mtx.RLock()
	defer block.mtx.RUnlock()
	return block.hash
}

// Hash uses the last stored block hash.
func (block *BestBlock) Hash() *chainhash.Hash {
	// Caller should check hash instead of error
	hash, _ := chainhash.NewHashFromStr(block.HashStr())
	return hash
}

func (pgb *ChainDB) BestBlock() (*chainhash.Hash, int64) {
	pgb.bestBlock.mtx.RLock()
	defer pgb.bestBlock.mtx.RUnlock()
	hash, _ := chainhash.NewHashFromStr(pgb.bestBlock.hash)
	return hash, pgb.bestBlock.height
}

func (pgb *ChainDB) BTCBestBlock() (*btc_chainhash.Hash, int64) {
	if pgb.BtcBestBlock == nil {
		return nil, 0
	}
	pgb.BtcBestBlock.Mtx.RLock()
	defer pgb.BtcBestBlock.Mtx.RUnlock()
	hash, _ := btc_chainhash.NewHashFromStr(pgb.BtcBestBlock.Hash)
	return hash, pgb.BtcBestBlock.Height
}

func (pgb *ChainDB) LTCBestBlock() (*ltc_chainhash.Hash, int64) {
	if pgb.LtcBestBlock == nil {
		return nil, 0
	}
	pgb.LtcBestBlock.Mtx.RLock()
	defer pgb.LtcBestBlock.Mtx.RUnlock()
	hash, _ := ltc_chainhash.NewHashFromStr(pgb.LtcBestBlock.Hash)
	return hash, pgb.LtcBestBlock.Height
}

func (pgb *ChainDB) BestBlockStr() (string, int64) {
	pgb.bestBlock.mtx.RLock()
	defer pgb.bestBlock.mtx.RUnlock()
	return pgb.bestBlock.hash, pgb.bestBlock.height
}

// BestBlockHash is a getter for ChainDB.bestBlock.hash.
func (pgb *ChainDB) BestBlockHash() *chainhash.Hash {
	return pgb.bestBlock.Hash()
}

// BestBlockHashStr is a getter for ChainDB.bestBlock.hash.
func (pgb *ChainDB) BestBlockHashStr() string {
	return pgb.bestBlock.HashStr()
}

// BlockHeight queries the DB for the height of the specified hash.
func (pgb *ChainDB) BlockHeight(hash string) (int64, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	height, err := RetrieveBlockHeight(ctx, pgb.db, hash)
	return height, pgb.replaceCancelError(err)
}

// BlockHash queries the DB for the hash of the mainchain block at the given
// height.
func (pgb *ChainDB) BlockHash(height int64) (string, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	hash, err := RetrieveBlockHash(ctx, pgb.db, height)
	return hash, pgb.replaceCancelError(err)
}

// BlockTimeByHeight queries the DB for the time of the mainchain block at the
// given height.
func (pgb *ChainDB) BlockTimeByHeight(height int64) (int64, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	time, err := RetrieveBlockTimeByHeight(ctx, pgb.db, height)
	return time.UNIX(), pgb.replaceCancelError(err)
}

// VotesInBlock returns the number of votes mined in the block with the
// specified hash.
func (pgb *ChainDB) VotesInBlock(hash string) (int16, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	voters, err := RetrieveBlockVoteCount(ctx, pgb.db, hash)
	if err != nil {
		err = pgb.replaceCancelError(err)
		log.Errorf("Unable to get block voter count for hash %s: %v", hash, err)
		return -1, err
	}
	return voters, nil
}

// ProposalVotes retrieves all the votes data associated with the provided token.
// TODO: Rewriting this func. may not be in chaindb struct, and in appContext instead
// func (pgb *ChainDB) ProposalVotes(proposalToken string) (*dbtypes.ProposalChartsData, error) {
// 	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
// 	defer cancel()
// 	chartsData, err := retrieveProposalVotesData(ctx, pgb.db, proposalToken)
// 	return chartsData, pgb.replaceCancelError(err)
// }

// SpendingTransactions retrieves all transactions spending outpoints from the
// specified funding transaction. The spending transaction hashes, the spending
// tx input indexes, and the corresponding funding tx output indexes, and an
// error value are returned.
func (pgb *ChainDB) SpendingTransactions(fundingTxID string) ([]string, []uint32, []uint32, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, spendingTxns, vinInds, voutInds, err := RetrieveSpendingTxsByFundingTx(ctx, pgb.db, fundingTxID)
	return spendingTxns, vinInds, voutInds, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) MutilchainSpendingTransactions(fundingTxID string, chainType string) ([]string, []uint32, []uint32, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, spendingTxns, vinInds, voutInds, err := RetrieveMutilchainSpendingTxsByFundingTx(ctx, pgb.db, fundingTxID, chainType)
	return spendingTxns, vinInds, voutInds, pgb.replaceCancelError(err)
}

// SpendingTransaction returns the transaction that spends the specified
// transaction outpoint, if it is spent. The spending transaction hash, input
// index, tx tree, and an error value are returned.
func (pgb *ChainDB) SpendingTransaction(fundingTxID string,
	fundingTxVout uint32) (string, uint32, int8, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, spendingTx, vinInd, tree, err := RetrieveSpendingTxByTxOut(ctx, pgb.db, fundingTxID, fundingTxVout)
	return spendingTx, vinInd, tree, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) MutilchainSpendingTransaction(fundingTxID string, fundingTxVout uint32, chainType string) (string, uint32, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, spendingTx, vinInd, err := RetrieveMutilchainSpendingTxByTxOut(ctx, pgb.db, fundingTxID, fundingTxVout, chainType)
	return spendingTx, vinInd, pgb.replaceCancelError(err)
}

// BlockTransactions retrieves all transactions in the specified block, their
// indexes in the block, their tree, and an error value.
func (pgb *ChainDB) BlockTransactions(blockHash string) ([]string, []uint32, []int8, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, blockTransactions, blockInds, trees, _, err := RetrieveTxsByBlockHash(ctx, pgb.db, blockHash)
	return blockTransactions, blockInds, trees, pgb.replaceCancelError(err)
}

// Transaction retrieves all rows from the transactions table for the given
// transaction hash.
func (pgb *ChainDB) Transaction(txHash string) ([]*dbtypes.Tx, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, dbTxs, err := RetrieveDbTxsByHash(ctx, pgb.db, txHash)
	return dbTxs, pgb.replaceCancelError(err)
}

// mutilchain transaction hash.
func (pgb *ChainDB) MutilchainTransaction(txHash string, chainType string) ([]*dbtypes.Tx, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, dbTxs, err := RetrieveMutilchainDbTxsByHash(ctx, pgb.db, txHash, chainType)
	return dbTxs, pgb.replaceCancelError(err)
}

// BlockMissedVotes retrieves the ticket IDs for all missed votes in the
// specified block, and an error value.
func (pgb *ChainDB) BlockMissedVotes(blockHash string) ([]string, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	mv, err := RetrieveMissedVotesInBlock(ctx, pgb.db, blockHash)
	return mv, pgb.replaceCancelError(err)
}

// missedVotesForBlockRange retrieves the number of missed votes for the block
// range specified.
func (pgb *ChainDB) missedVotesForBlockRange(startHeight, endHeight int64) (int64, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	missed, err := retrieveMissedVotesForBlockRange(ctx, pgb.db, startHeight, endHeight)
	return missed, pgb.replaceCancelError(err)
}

// TicketMisses retrieves all blocks in which the specified ticket was called to
// vote but failed to do so (miss). There may be multiple since this consideres
// side chain blocks. See TicketMiss for a mainchain-only version. If the ticket
// never missed a vote, the returned error will be dbtypes.ErrNoResult.
func (pgb *ChainDB) TicketMisses(ticketHash string) ([]string, []int64, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	blockHashes, blockHeights, err := RetrieveMissesForTicket(ctx, pgb.db, ticketHash)
	return blockHashes, blockHeights, pgb.replaceCancelError(err)
}

// TicketMiss retrieves the mainchain block in which the specified ticket was
// called to vote but failed to do so (miss). If the ticket never missed a vote,
// the returned error will be dbtypes.ErrNoResult.
func (pgb *ChainDB) TicketMiss(ticketHash string) (string, int64, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	blockHash, blockHeight, err := RetrieveMissForTicket(ctx, pgb.db, ticketHash)
	return blockHash, blockHeight, pgb.replaceCancelError(err)
}

// PoolStatusForTicket retrieves the specified ticket's spend status and ticket
// pool status, and an error value.
func (pgb *ChainDB) PoolStatusForTicket(txid string) (dbtypes.TicketSpendType, dbtypes.TicketPoolStatus, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, spendType, poolStatus, err := RetrieveTicketStatusByHash(ctx, pgb.db, txid)
	return spendType, poolStatus, pgb.replaceCancelError(err)
}

// VoutValue retrieves the value of the specified transaction outpoint in atoms.
func (pgb *ChainDB) VoutValue(txID string, vout uint32) (uint64, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	voutValue, err := RetrieveVoutValue(ctx, pgb.db, txID, vout)
	if err != nil {
		return 0, pgb.replaceCancelError(err)
	}
	return voutValue, nil
}

// VoutValues retrieves the values of each outpoint of the specified
// transaction. The corresponding indexes in the block and tx trees of the
// outpoints, and an error value are also returned.
func (pgb *ChainDB) VoutValues(txID string) ([]uint64, []uint32, []int8, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	voutValues, txInds, txTrees, err := RetrieveVoutValues(ctx, pgb.db, txID)
	if err != nil {
		return nil, nil, nil, pgb.replaceCancelError(err)
	}
	return voutValues, txInds, txTrees, nil
}

// TransactionBlock retrieves the hash of the block containing the specified
// transaction. The index of the transaction within the block, the transaction
// index, and an error value are also returned.
func (pgb *ChainDB) TransactionBlock(txID string) (string, uint32, int8, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, blockHash, blockInd, tree, err := RetrieveTxByHash(ctx, pgb.db, txID)
	return blockHash, blockInd, tree, pgb.replaceCancelError(err)
}

// AgendaVotes fetches the data used to plot a graph of votes cast per day per
// choice for the provided agenda.
func (pgb *ChainDB) AgendaVotes(agendaID string, chartType int) (*dbtypes.AgendaVoteChoices, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	chainInfo := pgb.ChainInfo()
	agendaInfo := chainInfo.AgendaMileStones[agendaID]

	// check if starttime is in the future exit.
	if time.Now().Before(agendaInfo.StartTime) {
		return nil, nil
	}

	avc, err := retrieveAgendaVoteChoices(ctx, pgb.db, agendaID, chartType,
		agendaInfo.VotingStarted, agendaInfo.VotingDone)
	return avc, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) TSpendTransactionVotes(tspendHash string, chartType int) (*dbtypes.AgendaVoteChoices, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	tvc, err := retrieveTSpendTxVoteChoices(ctx, pgb.db, tspendHash, chartType)
	return tvc, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) CountTSpendVotesRows() (uint64, error) {
	var rowCount uint64
	err := pgb.db.QueryRow(internal.CountTSpendVotesRows).Scan(&rowCount)
	if err != nil {
		return 0, err
	}
	return rowCount, nil
}

// AgendasVotesSummary fetches the total vote choices count for the provided
// agenda.
func (pgb *ChainDB) AgendasVotesSummary(agendaID string) (summary *dbtypes.AgendaSummary, err error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	chainInfo := pgb.ChainInfo()
	agendaInfo := chainInfo.AgendaMileStones[agendaID]

	// Check if starttime is in the future and exit if true.
	if time.Now().Before(agendaInfo.StartTime) {
		return
	}

	summary = &dbtypes.AgendaSummary{
		VotingStarted: agendaInfo.VotingStarted,
		LockedIn:      agendaInfo.VotingDone,
	}

	summary.Yes, summary.Abstain, summary.No, err = retrieveTotalAgendaVotesCount(ctx,
		pgb.db, agendaID, agendaInfo.VotingStarted, agendaInfo.VotingDone)
	return
}

// AgendaVoteCounts returns the vote counts for the agenda as builtin types.
func (pgb *ChainDB) AgendaVoteCounts(agendaID string) (yes, abstain, no uint32, err error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	chainInfo := pgb.ChainInfo()
	agendaInfo := chainInfo.AgendaMileStones[agendaID]

	// Check if starttime is in the future and exit if true.
	if time.Now().Before(agendaInfo.StartTime) {
		return
	}

	return retrieveTotalAgendaVotesCount(ctx, pgb.db, agendaID,
		agendaInfo.VotingStarted, agendaInfo.VotingDone)
}

// Getting proposal tokens needs to be synchronized
func (pgb *ChainDB) GetNeededSyncProposalTokens(tokens []string) (syncTokens []string, err error) {
	return retrieveNeededSyncProposalTokens(pgb.db, tokens)
}

// Check exist or create a new proposal_meta table
func (pgb *ChainDB) CheckCreateProposalMetaTable() (err error) {
	return checkExistAndCreateProposalMetaTable(pgb.db)
}

// Check exist or create a new proposal_meta table
func (pgb *ChainDB) CheckCreate24hBlocksTable() (err error) {
	return checkExistAndCreate24BlocksTable(pgb.db)
}

// Check exist or create a new proposal_meta table
func (pgb *ChainDB) CheckCreateTSpendVotesTable() (err error) {
	return checkExistAndCreateTSpendVotesTable(pgb.db)
}

// Check exist or create a new btc swaps table
func (pgb *ChainDB) CheckCreateBtcSwapsTable() (err error) {
	return checkExistAndCreateBtcSwapsTable(pgb.db)
}

// Check exist or create a new ltc swaps table
func (pgb *ChainDB) CheckCreateLtcSwapsTable() (err error) {
	return checkExistAndCreateLtcSwapsTable(pgb.db)
}

// Check exist or create a new address_summary table
func (pgb *ChainDB) CheckCreateAddressSummaryTable() (err error) {
	return checkExistAndCreateAddressSummaryTable(pgb.db)
}

// Check exist or create a new address_summary table
func (pgb *ChainDB) CheckCreateTreasurySummaryTable() (err error) {
	return checkExistAndCreateTreasurySummaryTable(pgb.db)
}

// Check exist or create a new monthly_price table
func (pgb *ChainDB) CheckCreateMonthlyPriceTable() (err error) {
	return checkExistAndCreateMonthlyPriceTable(pgb.db)
}

// Add proposal meta data to table
func (pgb *ChainDB) AddProposalMeta(proposalMetaData []map[string]string) (err error) {
	return addNewProposalMetaData(pgb.db, proposalMetaData)
}

// Get all proposal meta data
func (pgb *ChainDB) GetAllProposalMeta(searchKey string) (list []map[string]string, err error) {
	return getProposalMetaAll(pgb.db, searchKey)
}

// Get proposal meta by token
func (pgb *ChainDB) GetProposalByToken(token string) (proposalMeta map[string]string, err error) {
	return getProposalMetaByToken(pgb.db, token)
}

// Get proposal meta list by domain
func (pgb *ChainDB) GetProposalByDomain(domain string) (proposalMetaList []map[string]string, err error) {
	return getProposalMetasByDomain(pgb.db, domain)
}

// Get proposal meta list by owner
func (pgb *ChainDB) GetProposalByOwner(name string) (proposalMetaList []map[string]string, err error) {
	return getProposalMetasByOwner(pgb.db, name)
}

// Get all proposal domains
func (pgb *ChainDB) GetAllProposalDomains() []string {
	return getProposalDomainList(pgb.db)
}

// Get all proposal owner
func (pgb *ChainDB) GetAllProposalOwners() []string {
	return getProposalOwnerList(pgb.db)
}

// Get all proposal domains
func (pgb *ChainDB) GetAllProposalTokens() []string {
	return getAllProposalTokens(pgb.db)
}

// Get all proposal meta data
func (pgb *ChainDB) GetProposalMetaByMonth(year int, month int) (list []map[string]string, err error) {
	return getProposalMetaGroupByMonth(pgb.db, year, month)
}

// Get all proposal meta data
func (pgb *ChainDB) GetProposalMetaByYear(year int) (list []map[string]string, err error) {
	return getProposalMetaGroupByYear(pgb.db, year)
}

// AllAgendas returns all the agendas stored currently.
func (pgb *ChainDB) AllAgendas() (map[string]dbtypes.MileStone, error) {
	return retrieveAllAgendas(pgb.db)
}

func (pgb *ChainDB) SyncAndGet24hMetricsInfo(bestBlockHeight int64, chainType string) (*dbtypes.Block24hInfo, error) {
	//delete all invalid row
	numRow, delErr := DeleteInvalid24hBlocksRow(pgb.db)
	if delErr != nil {
		log.Errorf("failed to delete invalid block from DB: %v", delErr)
		return nil, delErr
	}

	if numRow > 0 {
		log.Infof("Deleted %d rows on 24hblocks table", numRow)
	}

	pgb.Sync24hMetricsByChainType(chainType)

	//check and sync new block
	return retrieve24hMetricsData(pgb.ctx, pgb.db, chainType)
}

// NumAddressIntervals gets the number of unique time intervals for the
// specified grouping where there are entries in the addresses table for the
// given address.
func (pgb *ChainDB) NumAddressIntervals(addr string, grouping dbtypes.TimeBasedGrouping) (int64, error) {
	if grouping >= dbtypes.NumIntervals {
		return 0, fmt.Errorf("invalid time grouping %d", grouping)
	}
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	return retrieveAddressTxsCount(ctx, pgb.db, addr, grouping.String())
}

// AddressMetrics returns the block time of the oldest transaction and the
// total count for all the transactions linked to the provided address grouped
// by years, months, weeks and days time grouping in seconds.
// This helps plot more meaningful address history graphs to the user.
func (pgb *ChainDB) AddressMetrics(addr string) (*dbtypes.AddressMetrics, error) {
	_, err := stdaddr.DecodeAddress(addr, pgb.chainParams)
	if err != nil {
		return nil, err
	}

	// For each time grouping/interval size, get the number if intervals with
	// data for the address.
	var metrics dbtypes.AddressMetrics
	for _, s := range dbtypes.TimeIntervals {
		numIntervals, err := pgb.NumAddressIntervals(addr, s)
		if err != nil {
			return nil, fmt.Errorf("NumAddressIntervals failed: error: %w", err)
		}

		switch s {
		case dbtypes.YearGrouping:
			metrics.YearTxsCount = numIntervals
		case dbtypes.MonthGrouping:
			metrics.MonthTxsCount = numIntervals
		case dbtypes.WeekGrouping:
			metrics.WeekTxsCount = numIntervals
		case dbtypes.DayGrouping:
			metrics.DayTxsCount = numIntervals
		}
	}

	// Get the time of the block with the first transaction involving the
	// address (oldest transaction block time).
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	blockTime, err := retrieveOldestTxBlockTime(ctx, pgb.db, addr)
	if err != nil {
		return nil, fmt.Errorf("retrieveOldestTxBlockTime failed: error: %w", err)
	}
	metrics.OldestBlockTime = blockTime

	return &metrics, pgb.replaceCancelError(err)
}

// AddressTransactions retrieves a slice of *dbtypes.AddressRow for a given
// address and transaction type (i.e. all, credit, or debit) from the DB. Only
// the first N transactions starting from the offset element in the set of all
// txnType transactions.
func (pgb *ChainDB) AddressTransactions(address string, N, offset int64,
	txnType dbtypes.AddrTxnViewType) (addressRows []*dbtypes.AddressRow, err error) {
	_, err = stdaddr.DecodeAddress(address, pgb.chainParams)
	if err != nil {
		return
	}
	var addrFunc func(context.Context, *sql.DB, string, int64, int64, int64, int64) ([]*dbtypes.AddressRow, error)
	switch txnType {
	case dbtypes.AddrTxnCredit:
		addrFunc = RetrieveAddressCreditTxns
	case dbtypes.AddrTxnAll:
		addrFunc = RetrieveAddressTxns
	case dbtypes.AddrTxnDebit:
		addrFunc = RetrieveAddressDebitTxns
	case dbtypes.AddrMergedTxnDebit:
		addrFunc = RetrieveAddressMergedDebitTxns
	case dbtypes.AddrMergedTxnCredit:
		addrFunc = RetrieveAddressMergedCreditTxns
	case dbtypes.AddrMergedTxn:
		addrFunc = RetrieveAddressMergedTxns
	default:
		return nil, fmt.Errorf("unknown AddrTxnViewType %v", txnType)
	}

	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	addressRows, err = addrFunc(ctx, pgb.db, address, N, offset, 0, 0)
	err = pgb.replaceCancelError(err)
	return
}

// AddressTransactionsAll retrieves all non-merged main chain addresses table
// rows for the given address. There is presently a hard limit of 3 million rows
// that may be returned, which is more than 4x the count for the treasury
// adddress as of mainnet block 521900.
func (pgb *ChainDB) AddressTransactionsAll(address string, year int64, month int64) (addressRows []*dbtypes.AddressRow, err error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	const limit = 3000000
	addressRows, err = RetrieveAddressTxns(ctx, pgb.db, address, limit, 0, year, month)
	// addressRows, err = RetrieveAllMainchainAddressTxns(ctx, pgb.db, address)
	err = pgb.replaceCancelError(err)
	return
}

func (pgb *ChainDB) MutilchainAddressTransactionsAll(address string, chainType string) (addressRows []*dbtypes.MutilchainAddressRow, err error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	const limit = 3000000
	addressRows, err = RetrieveMutilchainAddressTxns(ctx, pgb.db, address, limit, 0, chainType)
	// addressRows, err = RetrieveAllMainchainAddressTxns(ctx, pgb.db, address)
	err = pgb.replaceCancelError(err)
	return
}

// AddressTransactionsAllMerged retrieves all merged (stakeholder-approved and
// mainchain only) addresses table rows for the given address. There is
// presently a hard limit of 3 million rows that may be returned, which is more
// than 4x the count for the treasury adddress as of mainnet block 521900.
func (pgb *ChainDB) AddressTransactionsAllMerged(address string, year int64, month int64) (addressRows []*dbtypes.AddressRow, err error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	const limit = 3000000
	addressRows, err = RetrieveAddressMergedTxns(ctx, pgb.db, address, limit, 0, year, month)
	// const onlyValidMainchain = true
	// _, addressRows, err = RetrieveAllAddressMergedTxns(ctx, pgb.db, address,
	// 	onlyValidMainchain)
	err = pgb.replaceCancelError(err)
	return
}

// AddressHistoryAll retrieves N address rows of type AddrTxnAll, skipping over
// offset rows first, in order of block time.
func (pgb *ChainDB) AddressHistoryAll(address string, N, offset int64) ([]*dbtypes.AddressRow, *dbtypes.AddressBalance, error) {
	return pgb.AddressHistory(address, N, offset, dbtypes.AddrTxnAll, 0, 0)
}

// TicketPoolBlockMaturity returns the block at which all tickets with height
// greater than it are immature.
func (pgb *ChainDB) TicketPoolBlockMaturity() int64 {
	bestBlock := int64(pgb.stakeDB.Height())
	return bestBlock - int64(pgb.chainParams.TicketMaturity)
}

// TicketPoolByDateAndInterval fetches the tickets ordered by the purchase date
// interval provided and an error value.
func (pgb *ChainDB) TicketPoolByDateAndInterval(maturityBlock int64,
	interval dbtypes.TimeBasedGrouping) (*dbtypes.PoolTicketsData, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	tpd, err := retrieveTicketsByDate(ctx, pgb.db, maturityBlock, interval.String())
	return tpd, pgb.replaceCancelError(err)
}

// PosIntervals retrieves the blocks at the respective stakebase windows
// interval. The term "window" is used here to describe the group of blocks
// whose count is defined by chainParams.StakeDiffWindowSize. During this
// chainParams.StakeDiffWindowSize block interval the ticket price and the
// difficulty value is constant.
func (pgb *ChainDB) PosIntervals(limit, offset uint64) ([]*dbtypes.BlocksGroupedInfo, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	bgi, err := retrieveWindowBlocks(ctx, pgb.db,
		pgb.chainParams.StakeDiffWindowSize, pgb.Height(), limit, offset)
	return bgi, pgb.replaceCancelError(err)
}

// TimeBasedIntervals retrieves blocks groups by the selected time-based
// interval. For the consecutive groups the number of blocks grouped together is
// not uniform.
func (pgb *ChainDB) TimeBasedIntervals(timeGrouping dbtypes.TimeBasedGrouping,
	limit, offset uint64) ([]*dbtypes.BlocksGroupedInfo, error) {
	if timeGrouping >= dbtypes.NumIntervals {
		return nil, fmt.Errorf("invalid time grouping %d", timeGrouping)
	}
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	bgi, err := retrieveTimeBasedBlockListing(ctx, pgb.db, timeGrouping.String(),
		limit, offset)
	return bgi, pgb.replaceCancelError(err)
}

// TicketPoolVisualization helps block consecutive and duplicate DB queries for
// the requested ticket pool chart data. If the data for the given interval is
// cached and fresh, it is returned. If the cached data is stale and there are
// no queries running to update the cache for the given interval, this launches
// a query and updates the cache. If there is no cached data for the interval,
// this will launch a new query for the data if one is not already running, and
// if one is running, it will wait for the query to complete.
func (pgb *ChainDB) TicketPoolVisualization(interval dbtypes.TimeBasedGrouping) (*dbtypes.PoolTicketsData,
	*dbtypes.PoolTicketsData, *dbtypes.PoolTicketsData, int64, error) {
	if interval >= dbtypes.NumIntervals {
		return nil, nil, nil, -1, fmt.Errorf("invalid time grouping %d", interval)
	}
	// Attempt to retrieve data for the current block from cache.
	heightSeen := pgb.Height() // current block seen *by the ChainDB*
	if heightSeen < 0 {
		return nil, nil, nil, -1, fmt.Errorf("no charts data available")
	}
	timeChart, priceChart, outputsChart, height, intervalFound, stale :=
		TicketPoolData(interval, heightSeen)
	if intervalFound && !stale {
		// The cache was fresh.
		return timeChart, priceChart, outputsChart, height, nil
	}

	// Cache is stale or empty. Attempt to gain updater status.
	if !pgb.tpUpdatePermission[interval].TryLock() {
		// Another goroutine is running db query to get the updated data.
		if !intervalFound {
			// Do not even have stale data. Must wait for the DB update to
			// complete to get any data at all. Use a blocking call on the
			// updater lock even though we are not going to actually do an
			// update ourselves so we do not block the cache while waiting.
			pgb.tpUpdatePermission[interval].Lock()
			defer pgb.tpUpdatePermission[interval].Unlock()
			// Try again to pull it from cache now that the update is completed.
			heightSeen = pgb.Height()
			timeChart, priceChart, outputsChart, height, intervalFound, stale =
				TicketPoolData(interval, heightSeen)
			// We waited for the updater of this interval, so it should be found
			// at this point. If not, this is an error.
			if !intervalFound {
				log.Errorf("Charts data for interval %v failed to update.", interval)
				return nil, nil, nil, 0, fmt.Errorf("no charts data available")
			}
			if stale {
				log.Warnf("Charts data for interval %v updated, but still stale.", interval)
			}
		}
		// else return the stale data instead of waiting.

		return timeChart, priceChart, outputsChart, height, nil
	}
	// This goroutine is now the cache updater.
	defer pgb.tpUpdatePermission[interval].Unlock()

	// Retrieve chart data for best block in DB.
	var err error
	timeChart, priceChart, outputsChart, height, err = pgb.ticketPoolVisualization(interval)
	if err != nil {
		log.Errorf("Failed to fetch ticket pool data: %v", err)
		return nil, nil, nil, 0, err
	}

	// Update the cache with the new ticket pool data.
	UpdateTicketPoolData(interval, timeChart, priceChart, outputsChart, height)

	return timeChart, priceChart, outputsChart, height, nil
}

// ticketPoolVisualization fetches the following ticketpool data: tickets
// grouped on the specified interval, tickets grouped by price, and ticket
// counts by ticket type (solo, pool, other split). The interval may be one of:
// "mo", "wk", "day", or "all". The data is needed to populate the ticketpool
// graphs. The data grouped by time and price are returned in a slice.
func (pgb *ChainDB) ticketPoolVisualization(interval dbtypes.TimeBasedGrouping) (timeChart *dbtypes.PoolTicketsData,
	priceChart *dbtypes.PoolTicketsData, byInputs *dbtypes.PoolTicketsData, height int64, err error) {
	if interval >= dbtypes.NumIntervals {
		return nil, nil, nil, 0, fmt.Errorf("invalid time grouping %d", interval)
	}
	// Ensure DB height is the same before and after queries since they are not
	// atomic. Initial height:
	height = pgb.Height()
	for {
		// Latest block where mature tickets may have been mined.
		maturityBlock := pgb.TicketPoolBlockMaturity()

		// Tickets grouped by time interval
		timeChart, err = pgb.TicketPoolByDateAndInterval(maturityBlock, interval)
		if err != nil {
			return nil, nil, nil, 0, err
		}

		// Tickets grouped by price
		priceChart, err = pgb.TicketsByPrice(maturityBlock)
		if err != nil {
			return nil, nil, nil, 0, err
		}

		// Tickets grouped by number of inputs.
		byInputs, err = pgb.TicketsByInputCount()
		if err != nil {
			return nil, nil, nil, 0, err
		}

		heightEnd := pgb.Height()
		if heightEnd == height {
			break
		}
		// otherwise try again to ensure charts are consistent.
		height = heightEnd
	}

	return
}

// GetTicketInfo retrieves information about the pool and spend statuses, the
// purchase block, the lottery block, and the spending transaction.
func (pgb *ChainDB) GetTicketInfo(txid string) (*apitypes.TicketInfo, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	spendStatus, poolStatus, purchaseBlock, lotteryBlock, spendTxid, err := RetrieveTicketInfoByHash(ctx, pgb.db, txid)
	if err != nil {
		return nil, pgb.replaceCancelError(err)
	}

	var vote, revocation *string
	status := strings.ToLower(poolStatus.String())
	maturity := purchaseBlock.Height + uint32(pgb.chainParams.TicketMaturity)
	expiration := maturity + pgb.chainParams.TicketExpiry
	if pgb.Height() < int64(maturity) {
		status = "immature"
	}
	if spendStatus == dbtypes.TicketRevoked {
		status = spendStatus.String()
		revocation = &spendTxid
	} else if spendStatus == dbtypes.TicketVoted {
		vote = &spendTxid
	}

	if poolStatus == dbtypes.PoolStatusMissed {
		hash, height, err := RetrieveMissForTicket(ctx, pgb.db, txid)
		if err != nil {
			return nil, pgb.replaceCancelError(err)
		}
		lotteryBlock = &apitypes.TinyBlock{
			Hash:   hash,
			Height: uint32(height),
		}
	}

	return &apitypes.TicketInfo{
		Status:           status,
		PurchaseBlock:    purchaseBlock,
		MaturityHeight:   maturity,
		ExpirationHeight: expiration,
		LotteryBlock:     lotteryBlock,
		Vote:             vote,
		Revocation:       revocation,
	}, nil
}

func (pgb *ChainDB) TSpendVotes(tspendID *chainhash.Hash) (*dbtypes.TreasurySpendVotes, error) {
	tspendVotesResult, err := pgb.Client.GetTreasurySpendVotes(pgb.ctx, nil, []*chainhash.Hash{tspendID})
	if err != nil {
		return nil, err
	}
	if len(tspendVotesResult.Votes) != 1 {
		return nil, fmt.Errorf("expected 1 tally, got %d", len(tspendVotesResult.Votes))
	}

	tsv := dbtypes.TreasurySpendVotes(tspendVotesResult.Votes[0])

	return &tsv, nil
}

// TreasuryBalance calculates the *dbtypes.TreasuryBalance.
func (pgb *ChainDB) TreasuryBalance() (*dbtypes.TreasuryBalance, error) {
	return pgb.TreasuryBalanceWithPeriod(0, 0)
}

// TreasuryBalance calculates the *dbtypes.TreasuryBalance.
func (pgb *ChainDB) TreasuryBalanceWithPeriod(year int64, month int64) (*dbtypes.TreasuryBalance, error) {
	var addCount, added, immatureCount, immature, spendCount, spent, baseCount, base int64

	_, tipHeight := pgb.BestBlock()
	maturityHeight := tipHeight - int64(pgb.chainParams.CoinbaseMaturity)
	var rows *sql.Rows
	var err error
	if year == 0 {
		rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectTreasuryBalance, maturityHeight)
	} else if month == 0 {
		rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectTreasuryBalanceYear, maturityHeight, year)
	} else {
		rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectTreasuryBalanceYearMonth, maturityHeight, year, month)
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var txType, matureCount, allCount, matureValue, allValue sql.NullInt64
		if err = rows.Scan(&txType, &matureCount, &allCount, &matureValue, &allValue); err != nil {
			return nil, err
		}

		imCount := allCount.Int64 - matureCount.Int64
		imValue := allValue.Int64 - matureValue.Int64

		switch stake.TxType(txType.Int64) {
		case stake.TxTypeTSpend:
			spendCount = allCount.Int64
			spent = -matureValue.Int64
		case stake.TxTypeTAdd:
			immatureCount += imCount
			immature += imValue
			addCount = allCount.Int64
			added = matureValue.Int64
		case stake.TxTypeTreasuryBase:
			immatureCount += imCount
			immature += imValue
			baseCount = allCount.Int64
			base = matureValue.Int64
		}
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return &dbtypes.TreasuryBalance{
		Height:         tipHeight,
		MaturityHeight: maturityHeight,
		Balance:        added + base - spent,
		TxCount:        addCount + spendCount + baseCount,
		AddCount:       addCount,
		Added:          added,
		SpendCount:     spendCount,
		Spent:          spent,
		TBaseCount:     baseCount,
		TBase:          base,
		ImmatureCount:  immatureCount,
		Immature:       immature,
	}, nil
}

func (pgb *ChainDB) GetLegacySummaryGroupByMonth(year int) ([]dbtypes.TreasurySummary, error) {
	var rows *sql.Rows
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectAddressSummaryYearDataGroupByMonth, year)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dataObjList = make([]dbtypes.TreasurySummary, 0)
	startOfYear := time.Date(year, time.January, 1, 0, 0, 0, 0, time.Local)
	var now = time.Now()
	endOfYear := time.Date(year+1, 1, 0, 0, 0, 0, 0, time.Local)
	if now.Year() == year {
		endOfYear = now
	}
	monthPriceMap := pgb.GetCurrencyPriceMapByPeriod(startOfYear, endOfYear, true)
	for rows.Next() {
		var monthTime dbtypes.TimeDef
		var monthSummary dbtypes.TreasurySummary
		err = rows.Scan(&monthTime, &monthSummary.Outvalue, &monthSummary.Invalue)
		if err != nil {
			return nil, err
		}
		month := monthTime.Format("2006-01")
		monthPrice := monthPriceMap[month]
		monthSummary.Month = month
		difference := math.Abs(float64(monthSummary.Invalue - monthSummary.Outvalue))
		total := monthSummary.Invalue + monthSummary.Outvalue
		monthSummary.InvalueUSD = monthPrice * float64(monthSummary.Invalue) / 1e8
		monthSummary.OutvalueUSD = monthPrice * float64(monthSummary.Outvalue) / 1e8
		monthSummary.Difference = int64(difference)
		monthSummary.DifferenceUSD = monthPrice * float64(monthSummary.Difference) / 1e8
		monthSummary.Total = int64(total)
		monthSummary.TotalUSD = monthPrice * float64(monthSummary.Total) / 1e8
		dataObjList = append(dataObjList, monthSummary)
	}

	return dataObjList, nil
}

func (pgb *ChainDB) GetLegacySummaryByYear(year int) (*dbtypes.TreasurySummary, error) {
	var rows *sql.Rows
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectAddressSummaryDataByYear, year)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summary = dbtypes.TreasurySummary{}
	var startOfYear time.Time
	var endOfYear time.Time
	var hasData = false
	for rows.Next() {
		hasData = true
		var yearTime dbtypes.TimeDef
		err = rows.Scan(&yearTime, &summary.Outvalue, &summary.Invalue)
		if err != nil {
			return nil, err
		}

		startOfYear = time.Date(year, time.January, 1, 0, 0, 0, 0, time.Local)
		var now = time.Now()
		endOfYear = time.Date(year+1, 1, 0, 0, 0, 0, 0, time.Local)
		if now.Year() == year {
			endOfYear = now
		}
		summary.Month = strconv.Itoa(year)

		difference := math.Abs(float64(summary.Invalue - summary.Outvalue))
		total := summary.Invalue + summary.Outvalue
		summary.Difference = int64(difference)
		summary.Total = int64(total)
	}

	if rows.Err() != nil || !hasData {
		return &dbtypes.TreasurySummary{
			Month:         strconv.Itoa(year),
			Invalue:       0,
			InvalueUSD:    0,
			Outvalue:      0,
			OutvalueUSD:   0,
			Difference:    0,
			DifferenceUSD: 0,
			Total:         0,
			TotalUSD:      0,
		}, nil
	}

	monthPriceMap := pgb.GetCurrencyPriceMapByPeriod(startOfYear, endOfYear, true)
	var total = float64(0)
	var count = 0
	for _, val := range monthPriceMap {
		total += val
		count++
	}

	var average = total / float64(count)
	summary.InvalueUSD = average * float64(summary.Invalue) / 1e8
	summary.OutvalueUSD = average * float64(summary.Outvalue) / 1e8
	summary.DifferenceUSD = average * float64(summary.Difference) / 1e8
	summary.TotalUSD = average * float64(summary.Total) / 1e8
	return &summary, nil
}

func (pgb *ChainDB) GetTreasurySummaryByYear(year int) (*dbtypes.TreasurySummary, error) {
	var rows *sql.Rows
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectTreasurySummaryYearlyData, year)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summary = dbtypes.TreasurySummary{}
	var startOfYear time.Time
	var endOfYear time.Time
	var hasData = false

	for rows.Next() {
		hasData = true
		err = rows.Scan(&summary.Outvalue, &summary.Invalue, &summary.TaddValue)
		if err != nil {
			return nil, err
		}

		startOfYear = time.Date(year, time.January, 1, 0, 0, 0, 0, time.Local)
		var now = time.Now()
		endOfYear = time.Date(year+1, 1, 0, 0, 0, 0, 0, time.Local)
		if now.Year() == year {
			endOfYear = now
		}
		summary.Outvalue = int64(math.Abs(float64(summary.Outvalue)))
		summary.Month = strconv.Itoa(year)
		difference := math.Abs(float64(summary.Invalue - summary.Outvalue))
		total := summary.Invalue + summary.Outvalue
		summary.Difference = int64(difference)
		summary.Total = int64(total)
	}

	if rows.Err() != nil || !hasData {
		return &dbtypes.TreasurySummary{
			Month:         strconv.Itoa(year),
			Invalue:       0,
			InvalueUSD:    0,
			Outvalue:      0,
			OutvalueUSD:   0,
			Difference:    0,
			DifferenceUSD: 0,
			Total:         0,
			TotalUSD:      0,
		}, nil
	}

	currencyMap := pgb.GetCurrencyPriceMapByPeriod(startOfYear, endOfYear, true)
	var total = float64(0)
	var count = 0
	for _, v := range currencyMap {
		total += v
		count++
	}
	var average float64
	if count == 0 {
		average = 0
	} else {
		average = total / float64(count)
	}
	summary.InvalueUSD = average * float64(summary.Invalue) / 1e8
	summary.OutvalueUSD = average * float64(summary.Outvalue) / 1e8
	summary.DifferenceUSD = average * float64(summary.Difference) / 1e8
	summary.TotalUSD = average * float64(summary.Total) / 1e8
	summary.TaddValueUSD = average * float64(summary.TaddValue) / 1e8
	return &summary, nil
}

func (pgb *ChainDB) GetLegacySummaryAllYear() ([]dbtypes.TreasurySummary, error) {
	var rows *sql.Rows
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectAddressSummaryDataRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dataObjList = make([]dbtypes.TreasurySummary, 0)
	//get min date
	projectFundAddress, addErr := dbtypes.DevSubsidyAddress(pgb.chainParams)
	if addErr != nil {
		log.Warnf("ChainDB.Get Legacy address failed: %v", addErr)
		return nil, addErr
	}
	//get min date
	var oldestTime dbtypes.TimeDef
	err = pgb.db.QueryRow(internal.SelectOldestAddressCreditTime, projectFundAddress).Scan(&oldestTime)
	if err != nil {
		return nil, err
	}
	monthPriceMap := pgb.GetCurrencyPriceMapByPeriod(oldestTime.T, time.Now(), false)
	for rows.Next() {
		var monthTime dbtypes.TimeDef
		var monthSummary dbtypes.TreasurySummary
		err = rows.Scan(&monthTime, &monthSummary.Outvalue, &monthSummary.Invalue)
		if err != nil {
			return nil, err
		}
		month := monthTime.Format("2006-01")
		monthPrice := monthPriceMap[month]
		monthSummary.Month = month
		difference := math.Abs(float64(monthSummary.Invalue - monthSummary.Outvalue))
		total := monthSummary.Invalue + monthSummary.Outvalue
		monthSummary.InvalueUSD = monthPrice * float64(monthSummary.Invalue) / 1e8
		monthSummary.OutvalueUSD = monthPrice * float64(monthSummary.Outvalue) / 1e8
		monthSummary.Difference = int64(difference)
		monthSummary.DifferenceUSD = monthPrice * float64(monthSummary.Difference) / 1e8
		monthSummary.Total = int64(total)
		monthSummary.TotalUSD = monthPrice * float64(monthSummary.Total) / 1e8
		dataObjList = append(dataObjList, monthSummary)
	}

	return dataObjList, nil
}

func (pgb *ChainDB) GetTreasurySummaryAllYear() ([]dbtypes.TreasurySummary, error) {
	var rows *sql.Rows
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectTreasurySummaryDataRows)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dataObjList = make([]dbtypes.TreasurySummary, 0)
	//get min date
	var oldestTime dbtypes.TimeDef
	err = pgb.db.QueryRow(internal.SelectTreasuryOldestTime).Scan(&oldestTime)
	if err != nil {
		return nil, err
	}
	monthPriceMap := pgb.GetCurrencyPriceMapByPeriod(oldestTime.T, time.Now(), true)
	for rows.Next() {
		var time dbtypes.TimeDef
		var outValue, inValue, tadd int64
		err = rows.Scan(&time, &outValue, &inValue, &tadd)
		if err != nil {
			return nil, err
		}
		month := time.Format("2006-01")
		monthPrice := monthPriceMap[month]
		dataObj := dbtypes.TreasurySummary{
			Month:        month,
			Outvalue:     -outValue,
			OutvalueUSD:  monthPrice * float64(-outValue) / 1e8,
			Invalue:      inValue,
			InvalueUSD:   monthPrice * float64(inValue) / 1e8,
			TaddValue:    tadd,
			TaddValueUSD: monthPrice * float64(tadd) / 1e8,
			Difference:   int64(math.Abs(float64(inValue + outValue))),
			Total:        inValue - outValue,
		}
		dataObj.DifferenceUSD = monthPrice * float64(dataObj.Difference) / 1e8
		dataObj.TotalUSD = monthPrice * float64(dataObj.Total) / 1e8
		dataObjList = append(dataObjList, dataObj)
	}

	if rows.Err() != nil {
		return dataObjList, nil
	}
	return dataObjList, nil
}

func (pgb *ChainDB) GetTreasurySummaryGroupByMonth(year int) ([]dbtypes.TreasurySummary, error) {
	var rows *sql.Rows
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectTreasurySummaryRowsByYearly, year)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var dataObjList = make([]dbtypes.TreasurySummary, 0)
	startOfYear := time.Date(year, time.January, 1, 0, 0, 0, 0, time.Local)
	var now = time.Now()
	endOfYear := time.Date(year+1, 1, 0, 0, 0, 0, 0, time.Local)
	if now.Year() == year {
		endOfYear = now
	}
	monthPriceMap := pgb.GetCurrencyPriceMapByPeriod(startOfYear, endOfYear, true)
	for rows.Next() {
		var time dbtypes.TimeDef
		var outValue, inValue, tadd int64
		err = rows.Scan(&time, &outValue, &inValue, &tadd)
		if err != nil {
			return nil, err
		}
		month := time.Format("2006-01")
		monthPrice := monthPriceMap[month]
		dataObj := dbtypes.TreasurySummary{
			Month:        month,
			Outvalue:     -outValue,
			OutvalueUSD:  monthPrice * float64(-outValue) / 1e8,
			Invalue:      inValue,
			InvalueUSD:   monthPrice * float64(inValue) / 1e8,
			TaddValue:    tadd,
			TaddValueUSD: monthPrice * float64(tadd) / 1e8,
			Difference:   int64(math.Abs(float64(inValue + outValue))),
			Total:        inValue - outValue,
		}
		dataObj.DifferenceUSD = monthPrice * float64(dataObj.Difference) / 1e8
		dataObj.TotalUSD = monthPrice * float64(dataObj.Total) / 1e8
		dataObjList = append(dataObjList, dataObj)
	}

	if rows.Err() != nil {
		return dataObjList, nil
	}

	return dataObjList, nil
}

func (pgb *ChainDB) GetMonthlyPrice(year, month int) (float64, error) {
	monthTime := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.Local)
	startOfMonth := monthTime.AddDate(0, 0, -monthTime.Day()+1)
	endOfMonth := monthTime.AddDate(0, 1, -monthTime.Day())
	monthPriceMap := pgb.GetCurrencyPriceMapByPeriod(startOfMonth, endOfMonth, false)
	monthFormat := monthTime.Format("2006-01")
	monthPrice, ok := monthPriceMap[monthFormat]
	if !ok {
		return 0, fmt.Errorf("get month price failed")
	}
	return monthPrice, nil
}

func (pgb *ChainDB) GetLegacySummaryByMonth(year int, month int) (*dbtypes.TreasurySummary, error) {
	var rows *sql.Rows
	rows, queryErr := pgb.db.QueryContext(pgb.ctx, internal.SelectAddressSummaryDataByMonth, year, month)
	if queryErr != nil {
		return nil, queryErr
	}
	defer rows.Close()

	var summary = dbtypes.TreasurySummary{}
	var startOfMonth time.Time
	var endOfMonth time.Time
	var hasData = false

	for rows.Next() {
		hasData = true
		var time time.Time
		err := rows.Scan(&time, &summary.Outvalue, &summary.Invalue)
		if err != nil {
			return nil, err
		}
		startOfMonth = time.AddDate(0, 0, -time.Day()+1)
		endOfMonth = time.AddDate(0, 1, -time.Day())
		summary.Month = time.Format("2006-01")
		difference := math.Abs(float64(summary.Invalue - summary.Outvalue))
		total := summary.Invalue + summary.Outvalue
		summary.Difference = int64(difference)
		summary.Total = int64(total)
	}

	if rows.Err() != nil || !hasData {
		return &dbtypes.TreasurySummary{
			Month:         fmt.Sprintf("%d-%d", year, month),
			Invalue:       0,
			InvalueUSD:    0,
			Outvalue:      0,
			OutvalueUSD:   0,
			Difference:    0,
			DifferenceUSD: 0,
			Total:         0,
			TotalUSD:      0,
		}, nil
	}

	monthPriceMap := pgb.GetCurrencyPriceMapByPeriod(startOfMonth, endOfMonth, true)
	monthPrice := monthPriceMap[summary.Month]
	summary.InvalueUSD = monthPrice * float64(summary.Invalue) / 1e8
	summary.OutvalueUSD = monthPrice * float64(summary.Outvalue) / 1e8
	summary.DifferenceUSD = monthPrice * float64(summary.Difference) / 1e8
	summary.TotalUSD = monthPrice * float64(summary.Total) / 1e8
	summary.MonthPrice = monthPrice
	return &summary, nil
}

func (pgb *ChainDB) GetTreasurySummaryByMonth(year int, month int) (*dbtypes.TreasurySummary, error) {
	var rows *sql.Rows
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectTreasurySummaryMonthlyData, year, month)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var summary = dbtypes.TreasurySummary{}
	var startOfMonth time.Time
	var endOfMonth time.Time
	var hasData = false
	for rows.Next() {
		hasData = true
		var time dbtypes.TimeDef
		err = rows.Scan(&time, &summary.Outvalue, &summary.Invalue, &summary.TaddValue)
		if err != nil {
			return nil, err
		}
		summary.Outvalue = int64(math.Abs(float64(summary.Outvalue)))
		startOfMonth = time.T.AddDate(0, 0, -time.T.Day()+1)
		endOfMonth = time.T.AddDate(0, 1, -time.T.Day())
		summary.Month = time.Format("2006-01")
		difference := math.Abs(float64(summary.Invalue - summary.Outvalue))
		total := summary.Invalue + summary.Outvalue
		summary.Difference = int64(difference)
		summary.Total = int64(total)
	}

	if rows.Err() != nil || !hasData {
		return &dbtypes.TreasurySummary{
			Month:         fmt.Sprintf("%d-%d", year, month),
			Invalue:       0,
			InvalueUSD:    0,
			Outvalue:      0,
			OutvalueUSD:   0,
			Difference:    0,
			DifferenceUSD: 0,
			Total:         0,
			TotalUSD:      0,
		}, nil
	}

	monthPriceMap := pgb.GetCurrencyPriceMapByPeriod(startOfMonth, endOfMonth, true)
	monthPrice := monthPriceMap[summary.Month]
	summary.InvalueUSD = monthPrice * float64(summary.Invalue) / 1e8
	summary.OutvalueUSD = monthPrice * float64(summary.Outvalue) / 1e8
	summary.DifferenceUSD = monthPrice * float64(summary.Difference) / 1e8
	summary.TotalUSD = monthPrice * float64(summary.Total) / 1e8
	summary.TaddValueUSD = monthPrice * float64(summary.TaddValue) / 1e8
	summary.MonthPrice = monthPrice
	return &summary, nil
}

func (pgb *ChainDB) GetTreasuryTimeRange() (int64, int64, error) {
	var minTime, maxTime dbtypes.TimeDef
	err := pgb.db.QueryRowContext(pgb.ctx, internal.SelectTreasuryTimeRange).Scan(&minTime, &maxTime)
	if err != nil {
		return 0, 0, nil
	}
	return minTime.UNIX(), maxTime.UNIX(), nil
}

func (pgb *ChainDB) GetLegacyTimeRange() (int64, int64, error) {
	var minTime, maxTime dbtypes.TimeDef
	err := pgb.db.QueryRowContext(pgb.ctx, internal.SelectAddressSummaryTimeRange).Scan(&minTime, &maxTime)
	if err != nil {
		return 0, 0, nil
	}
	return minTime.UNIX(), maxTime.UNIX(), nil
}

// Get treasury summary data
func (pgb *ChainDB) GetTreasurySummary() ([]*dbtypes.TreasurySummary, error) {
	var rows *sql.Rows
	// rows, queryErr := pgb.db.QueryContext(pgb.ctx, internal.SelectLegacySummaryByMonth, projectFundAddress, dbtypes.AddrMergedTxnCredit)
	rows, queryErr := pgb.db.QueryContext(pgb.ctx, internal.SelectTreasurySummaryDataRows)
	if queryErr != nil {
		return nil, queryErr
	}
	defer rows.Close()
	treasuryCreditTxnCount, err := pgb.RetrieveCountTreasuryCreditRows()
	if err != nil {
		return nil, err
	}
	treasuryDebitTxnCount, debitErr := pgb.RetrieveCountTreasuryDebitRows()
	if debitErr != nil {
		return nil, debitErr
	}
	var summaryList []*dbtypes.TreasurySummary
	for rows.Next() {
		var summary = dbtypes.TreasurySummary{}
		var time dbtypes.TimeDef
		err := rows.Scan(&time, &summary.Outvalue, &summary.Invalue, &summary.TaddValue)
		if err != nil {
			return nil, err
		}
		summary.Outvalue = int64(math.Abs(float64(summary.Outvalue)))
		summary.Month = time.Format("2006-01")
		difference := math.Abs(float64(summary.Invalue - summary.Outvalue))
		total := summary.Invalue + summary.Outvalue
		summary.Difference = int64(difference)
		summary.Total = int64(total)
		summary.MonthTime = time.T
		summaryList = append(summaryList, &summary)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	//create balance data for treasury summary
	balance := int64(0)
	for i := len(summaryList) - 1; i >= 0; i-- {
		summary := summaryList[i]
		balance += summary.Invalue - summary.Outvalue
		summary.Balance = balance
	}

	//get min date
	var oldestTime dbtypes.TimeDef
	err = pgb.db.QueryRow(internal.SelectTreasuryOldestTime).Scan(&oldestTime)
	if err != nil {
		return nil, err
	}

	monthPriceMap := pgb.GetCurrencyPriceMapByPeriod(oldestTime.T, time.Now(), true)

	for _, summary := range summaryList {
		monthPrice := monthPriceMap[summary.Month]
		summary.InvalueUSD = monthPrice * float64(summary.Invalue) / 1e8
		summary.OutvalueUSD = monthPrice * float64(summary.Outvalue) / 1e8
		summary.DifferenceUSD = monthPrice * float64(summary.Difference) / 1e8
		summary.BalanceUSD = monthPrice * float64(summary.Balance) / 1e8
		summary.TotalUSD = monthPrice * float64(summary.Total) / 1e8
		summary.TaddValueUSD = monthPrice * float64(summary.TaddValue) / 1e8
		summary.MonthPrice = monthPrice
		//get select index by month
		//select index by month
		creditIndexOfRows := int64(0)
		debitIndexOfRows := int64(0)
		creditAddrIndex, err := pgb.RetrieveTreasuryCreditMonthRowIndex(summary.MonthTime.Year(), int(summary.MonthTime.Month()))
		if err == nil && creditAddrIndex > 0 {
			creditIndexOfRows = treasuryCreditTxnCount - creditAddrIndex
		}
		debitAddrIndex, err := pgb.RetrieveTreasuryDebitMonthRowIndex(summary.MonthTime.Year(), int(summary.MonthTime.Month()))
		if err == nil && debitAddrIndex > 0 {
			debitIndexOfRows = treasuryDebitTxnCount - debitAddrIndex
		}
		creditOffset := 20 * (creditIndexOfRows / 20)
		debitOffset := 20 * (debitIndexOfRows / 20)

		creditLink := fmt.Sprintf("/treasury?n=20&start=%d&txntype=treasurybase", creditOffset)
		debitLink := fmt.Sprintf("/treasury?n=20&start=%d&txntype=tspend", debitOffset)
		summary.CreditLink = creditLink
		summary.DebitLink = debitLink
	}
	return summaryList, nil
}

func (pgb *ChainDB) SyncTreasuryMonthlyPrice() {
	//check last month in database
	var lastestMonthly time.Time
	var lastestUpdateTime time.Time
	err := pgb.db.QueryRow(internal.SelectLastMonthlyPrice).Scan(&lastestMonthly, &lastestUpdateTime)
	now := time.Now()
	//if error or lastest day same with today, return
	if err != nil || lastestUpdateTime.After(now) || (lastestUpdateTime.Year() == now.Year() && lastestUpdateTime.Month() == now.Month() && lastestUpdateTime.Day() == now.Day()) {
		return
	}

	//sync from first day of next month of last month
	fistOfLastMonth := lastestMonthly.AddDate(0, 0, -lastestMonthly.Day()+1)
	currencyPriceMap := pgb.GetMexcPriceMap(fistOfLastMonth.Unix(), now.Unix())
	createTableErr := pgb.CheckCreateMonthlyPriceTable()
	if createTableErr != nil {
		log.Errorf("Check exist and create monthly_price table failed: %v", err)
		return
	}

	pgb.CheckAndInsertToMonthlyPriceTable(currencyPriceMap)
}

func (pgb *ChainDB) CheckAndInsertToMonthlyPriceTable(currencyPriceMap map[string]float64) {
	now := time.Now()
	for month, price := range currencyPriceMap {
		timeArr := strings.Split(month, "-")
		if len(timeArr) < 2 {
			continue
		}
		year, yearErr := strconv.ParseInt(timeArr[0], 0, 32)
		monthInt := dbtypes.GetMonthFromString(timeArr[1])
		if yearErr != nil {
			continue
		}
		var isCompleted bool
		var lastUpdated dbtypes.TimeDef
		existErr := pgb.db.QueryRowContext(pgb.ctx, internal.GetMonthlyPriceInfoByMonth, year*12+monthInt).Scan(&isCompleted, &lastUpdated)
		//if error, continue
		startOfMonth := time.Date(int(year), time.Month(monthInt), 1, 0, 0, 0, 0, time.Local)
		if existErr != nil {
			if !errors.Is(existErr, sql.ErrNoRows) {
				continue
			} else {
				//if this month, save with not complete
				if now.Year() == int(year) && now.Month() == time.Month(monthInt) {
					pgb.db.QueryRow(internal.InsertMonthlyPriceRow, dbtypes.NewTimeDef(startOfMonth), price, false, dbtypes.NewTimeDef(now))
				} else {
					pgb.db.QueryRow(internal.InsertMonthlyPriceRow, dbtypes.NewTimeDef(startOfMonth), price, true, dbtypes.NewTimeDef(now))
				}
			}
		} else {
			if isCompleted {
				continue
			}
			//if exist but not complete, update row
			if now.Year() != int(year) || now.Month() != time.Month(monthInt) {
				pgb.db.QueryRow(internal.UpdateMonthlyPriceRow, price, true, dbtypes.NewTimeDef(now), year*12+monthInt)
			} else {
				//if this month, update with not complete
				pgb.db.QueryRow(internal.UpdateMonthlyPriceRow, price, false, dbtypes.NewTimeDef(now), year*12+monthInt)
			}
		}
	}
}

func (pgb *ChainDB) GetPeerCount() (peerCount int, err error) {
	// get peer count from jholdstock api
	peerCount, err = externalapi.GetWorldNodesCount()
	return
}

func (pgb *ChainDB) GetPriceAll() (*dbtypes.BitDegreeOhlcResponse, error) {
	var result dbtypes.BitDegreeOhlcResponse
	fetchUrl := "https://www.bitdegree.org/api/cryptocurrencies/ohlc-chart/decred-dcr"
	query := map[string]string{
		"period": "all",
	}
	req := &externalapi.ReqConfig{
		Method:  http.MethodGet,
		HttpUrl: fetchUrl,
		Payload: query,
	}
	if err := externalapi.HttpRequest(req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (pgb *ChainDB) GetMexcPriceData(from, to int64) (dbtypes.MexcMonthlyPriceResponse, error) {
	var result dbtypes.MexcMonthlyPriceResponse
	fetchUrl := "https://api.mexc.com/api/v3/klines"
	query := map[string]string{
		"symbol":    "DCRUSDT",
		"interval":  "1M",
		"startTime": fmt.Sprintf("%d", from*1000),
		"endTime":   fmt.Sprintf("%d", to*1000),
	}
	req := &externalapi.ReqConfig{
		Method:  http.MethodGet,
		HttpUrl: fetchUrl,
		Payload: query,
	}
	if err := externalapi.HttpRequest(req, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (pgb *ChainDB) GetMexcPriceMap(from, to int64) map[string]float64 {
	result := make(map[string]float64)
	res, err := pgb.GetMexcPriceData(from, to)
	if err != nil {
		return result
	}
	countMap := make(map[string]int)
	if len(res) == 0 {
		return result
	}
	for _, dataArr := range res {
		if len(dataArr) < 8 {
			continue
		}

		timeInt, ok := dataArr[0].(int64)
		if !ok {
			continue
		}

		date := time.Unix(timeInt/1000, 0)
		key := fmt.Sprintf("%d-%s", date.Year(), dbtypes.GetFullMonthDisplay(int(date.Month())))
		val, ok := result[key]
		priceStr, ok := dataArr[4].(string)
		if !ok {
			continue
		}
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			continue
		}

		if ok {
			result[key] = val + price
			countMap[key] = countMap[key] + 1
		} else {
			result[key] = price
			countMap[key] = 1
		}
	}
	for k, v := range result {
		result[k] = v / float64(countMap[k])
	}
	return result
}

func (pgb *ChainDB) GetBitDegreeAllPriceMap() map[string]float64 {
	result := make(map[string]float64)
	res, err := pgb.GetPriceAll()
	if err != nil {
		return result
	}
	countMap := make(map[string]int)
	if len(res.Ohlc) == 0 {
		return result
	}
	for _, dataArr := range res.Ohlc {
		if len(dataArr) < 4 {
			continue
		}
		date := time.Unix(int64(dataArr[0])/1000, 0)
		key := fmt.Sprintf("%d-%s", date.Year(), dbtypes.GetFullMonthDisplay(int(date.Month())))
		val, ok := result[key]
		if ok {
			result[key] = val + dataArr[4]
			countMap[key] = countMap[key] + 1
		} else {
			result[key] = dataArr[4]
			countMap[key] = 1
		}
	}
	for k, v := range result {
		result[k] = v / float64(countMap[k])
	}
	return result
}

// Get legacy summary data
func (pgb *ChainDB) GetLegacySummary() ([]*dbtypes.TreasurySummary, error) {
	var rows *sql.Rows
	// rows, queryErr := pgb.db.QueryContext(pgb.ctx, internal.SelectLegacySummaryByMonth, projectFundAddress, dbtypes.AddrMergedTxnCredit)
	rows, queryErr := pgb.db.QueryContext(pgb.ctx, internal.SelectAddressSummaryDataRows)
	if queryErr != nil {
		return nil, queryErr
	}
	defer rows.Close()
	addrCreditTxnCount, err := pgb.RetrieveCountLegacyCreditAddressRows()
	if err != nil {
		return nil, err
	}
	addrDebitTxnCount, debitErr := pgb.RetrieveCountLegacyDebitAddressRows()
	if debitErr != nil {
		return nil, debitErr
	}
	var summaryList []*dbtypes.TreasurySummary
	for rows.Next() {
		var summary = dbtypes.TreasurySummary{}
		var time dbtypes.TimeDef
		err := rows.Scan(&time, &summary.Outvalue, &summary.Invalue)
		if err != nil {
			return nil, err
		}
		summary.Month = time.Format("2006-01")
		difference := math.Abs(float64(summary.Invalue - summary.Outvalue))
		total := summary.Invalue + summary.Outvalue
		summary.Difference = int64(difference)
		summary.Total = int64(total)
		summary.MonthTime = time.T
		summaryList = append(summaryList, &summary)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	//create balance data for treasury summary
	balance := int64(0)
	for i := len(summaryList) - 1; i >= 0; i-- {
		summary := summaryList[i]
		balance += summary.Invalue - summary.Outvalue
		summary.Balance = balance
	}

	projectFundAddress, addErr := dbtypes.DevSubsidyAddress(pgb.chainParams)
	if addErr != nil {
		log.Warnf("ChainDB.Get Legacy address failed: %v", addErr)
		return nil, addErr
	}
	//get min date
	var oldestTime dbtypes.TimeDef
	err = pgb.db.QueryRow(internal.SelectOldestAddressCreditTime, projectFundAddress).Scan(&oldestTime)
	if err != nil {
		return nil, err
	}

	monthPriceMap := pgb.GetCurrencyPriceMapByPeriod(oldestTime.T, time.Now(), false)

	for _, summary := range summaryList {
		monthPrice := monthPriceMap[summary.Month]
		summary.InvalueUSD = monthPrice * float64(summary.Invalue) / 1e8
		summary.OutvalueUSD = monthPrice * float64(summary.Outvalue) / 1e8
		summary.DifferenceUSD = monthPrice * float64(summary.Difference) / 1e8
		summary.BalanceUSD = monthPrice * float64(summary.Balance) / 1e8
		summary.TotalUSD = monthPrice * float64(summary.Total) / 1e8
		summary.MonthPrice = monthPrice
		//get select index by month
		//select index by month
		creditIndexOfRows := int64(0)
		debitIndexOfRows := int64(0)
		creditAddrIndex, err := pgb.RetrieveLegacyAddressCreditMonthRowIndex(summary.MonthTime.Year(), int(summary.MonthTime.Month()))
		if err == nil && creditAddrIndex > 0 {
			creditIndexOfRows = addrCreditTxnCount - creditAddrIndex
		}
		debitAddrIndex, err := pgb.RetrieveLegacyAddressDebitMonthRowIndex(summary.MonthTime.Year(), int(summary.MonthTime.Month()))
		if err == nil && debitAddrIndex > 0 {
			debitIndexOfRows = addrDebitTxnCount - debitAddrIndex
		}
		creditOffset := 20 * (creditIndexOfRows / 20)
		debitOffset := 20 * (debitIndexOfRows / 20)

		creditLink := fmt.Sprintf("/address/%s?n=20&start=%d&txntype=credit", projectFundAddress, creditOffset)
		debitLink := fmt.Sprintf("/address/%s?n=20&start=%d&txntype=debit", projectFundAddress, debitOffset)
		summary.CreditLink = creditLink
		summary.DebitLink = debitLink
	}
	return summaryList, nil
}

func (pgb *ChainDB) GetCurrencyPriceMapByPeriod(from time.Time, to time.Time, isSync bool) map[string]float64 {
	if from.After(to) {
		return make(map[string]float64, 0)
	}
	//get start day on month of from
	startDayOfFromMonth := time.Date(from.Year(), from.Month(), 1, 0, 0, 0, 0, time.Local)
	endDayOfToMonth := time.Date(to.Year(), to.Month(), 1, 23, 59, 59, 0, time.Local)
	endDayOfToMonth = endDayOfToMonth.AddDate(0, 1, -1)
	//Synchronize price data by month before retrieving data
	lastMonth := ""
	lastPrice := 0.0
	if isSync {
		pgb.SyncTreasuryMonthlyPrice()
	}
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectMonthlyPriceRowsByPeriod, startDayOfFromMonth.Unix(), endDayOfToMonth.Unix())

	priceMap := make(map[string]float64, 0)
	if isSync && lastMonth != "" {
		priceMap[lastMonth] = lastPrice
	}
	if err == nil {
		for rows.Next() {
			var month time.Time
			var price float64
			err = rows.Scan(&month, &price)
			if err != nil {
				return priceMap
			}
			key := fmt.Sprintf("%d-%s", month.Year(), dbtypes.GetFullMonthDisplay(int(month.Month())))
			_, ok := priceMap[key]
			if !ok {
				priceMap[key] = price
			}
		}
	}
	var currentTime = from
	var breakFlg = false
	for !breakFlg {
		key := fmt.Sprintf("%d-%s", currentTime.Year(), dbtypes.GetFullMonthDisplay(int(currentTime.Month())))
		_, ok := priceMap[key]
		//if not exist on monthly price table, get value from api
		if !ok {
			startOfMonth := time.Date(currentTime.Year(), currentTime.Month(), 1, 0, 0, 0, 0, time.Local)
			//if last month, get from api
			if endDayOfToMonth.Month() == currentTime.Month() && endDayOfToMonth.Year() == currentTime.Year() {
				mapData := pgb.GetMexcPriceMap(startOfMonth.Unix(), endDayOfToMonth.Unix())
				for k, v := range mapData {
					if _, keyOk := priceMap[k]; !keyOk {
						priceMap[k] = v
					}
				}
				breakFlg = true
			} else {
				endOfMonth := time.Date(currentTime.Year(), currentTime.Month()+1, 1, 0, 0, 0, -1, time.Local)
				mapData := pgb.GetMexcPriceMap(startOfMonth.Unix(), endOfMonth.Unix())
				for k, v := range mapData {
					if _, keyOk := priceMap[k]; !keyOk {
						priceMap[k] = v
						//insert to table
						pgb.db.QueryRow(internal.InsertMonthlyPriceRow, dbtypes.NewTimeDef(startOfMonth), v)
					}
				}
			}
		}

		if endDayOfToMonth.Month() == currentTime.Month() && endDayOfToMonth.Year() == currentTime.Year() {
			breakFlg = true
			continue
		}
		currentTime = currentTime.AddDate(0, 1, 0)
	}
	return priceMap
}

// GetAtomicSwapsContractGroupQuery return query for get atomic swap contract tx list
func (pgb *ChainDB) GetAtomicSwapsContractGroupQuery(pair, status, searchKey string) string {
	if searchKey != "" {
		return internal.MakeSelectAtomicSwapsContractGroupWithSearchFilter(pair, status)
	}
	return internal.MakeSelectAtomicSwapsContractGroupWithFilter(pair, status)
}

func (pgb *ChainDB) GetSwapFullDataByContractTx(contractTx, groupTx string) (spends []*dbtypes.AtomicSwapTxData, err error) {
	// get contract spends data
	var rows *sql.Rows
	rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectAtomicSpendsByContractTx, contractTx, groupTx)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var spendData dbtypes.AtomicSwapTxData
		err = rows.Scan(&spendData.Txid, &spendData.Vin, &spendData.Height, &spendData.Value, &spendData.LockTime)
		if err != nil {
			return
		}
		spendData.LockTimeDisp = utils.DateTimeWithoutTimeZone(spendData.LockTime)
		// get spend tx time
		var spendHash *chainhash.Hash
		spendHash, err = chainhash.NewHashFromStr(spendData.Txid)
		if err != nil {
			return
		}
		var txRaw *chainjson.TxRawResult
		txRaw, err = pgb.Client.GetRawTransactionVerbose(pgb.ctx, spendHash)
		if err != nil {
			return
		}
		spendData.Time = txRaw.Time
		spendData.TimeDisp = utils.DateTimeWithoutTimeZone(spendData.Time)
		spends = append(spends, &spendData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	return
}

func (pgb *ChainDB) GetLTCSwapFullDataByContractTx(contractTx, groupTx string) (spends []*dbtypes.AtomicSwapTxData, err error) {
	// get contract spends data
	var rows *sql.Rows
	rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectLTCAtomicSpendsByContractTx, contractTx, groupTx)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var spendData dbtypes.AtomicSwapTxData
		err = rows.Scan(&spendData.Txid, &spendData.Vin, &spendData.Height, &spendData.Value, &spendData.LockTime)
		if err != nil {
			return
		}
		spendData.LockTimeDisp = utils.DateTimeWithoutTimeZone(spendData.LockTime)
		// get spend tx time
		var txHash *ltc_chainhash.Hash
		txHash, err = ltc_chainhash.NewHashFromStr(spendData.Txid)
		if err != nil {
			return
		}
		var txRaw *ltcjson.TxRawResult
		txRaw, err = pgb.LtcClient.GetRawTransactionVerbose(txHash)
		if err != nil {
			return
		}
		spendData.Time = txRaw.Time
		spendData.TimeDisp = utils.DateTimeWithoutTimeZone(spendData.Time)
		spends = append(spends, &spendData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	return
}

func (pgb *ChainDB) GetBTCSwapFullDataByContractTx(contractTx, groupTx string) (spends []*dbtypes.AtomicSwapTxData, err error) {
	// get contract spends data
	var rows *sql.Rows
	rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectBTCAtomicSpendsByContractTx, contractTx, groupTx)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var spendData dbtypes.AtomicSwapTxData
		err = rows.Scan(&spendData.Txid, &spendData.Vin, &spendData.Height, &spendData.Value, &spendData.LockTime)
		if err != nil {
			return
		}
		spendData.LockTimeDisp = utils.DateTimeWithoutTimeZone(spendData.LockTime)
		// get spend tx time
		var txHash *btc_chainhash.Hash
		txHash, err = btc_chainhash.NewHashFromStr(spendData.Txid)
		if err != nil {
			return
		}
		var txRaw *btcjson.TxRawResult
		txRaw, err = pgb.BtcClient.GetRawTransactionVerbose(txHash)
		if err != nil {
			return
		}
		spendData.Time = txRaw.Time
		spendData.TimeDisp = utils.DateTimeWithoutTimeZone(spendData.Time)
		spends = append(spends, &spendData)
	}
	err = rows.Err()
	if err != nil {
		return
	}
	return
}

// CheckSwapIsRefund return true if swap is refund
func (pgb *ChainDB) CheckSwapIsRefund(groupTx string) (bool, error) {
	var isRefund bool
	err := pgb.db.QueryRow(internal.CheckSwapIsRefund, groupTx).Scan(&isRefund)
	if err != nil {
		return false, err
	}
	return isRefund, nil
}

// GetContractDetailOutputs return all detail data of contract
func (pgb *ChainDB) GetContractSwapDataByGroup(groupTx, targetTokenString string) (*dbtypes.AtomicSwapFullData, error) {
	var isRefund bool
	err := pgb.db.QueryRow(internal.CheckSwapIsRefund, groupTx).Scan(&isRefund)
	if err != nil {
		return nil, err
	}
	cSwapData := &dbtypes.AtomicSwapFullData{
		TargetToken: targetTokenString,
		IsRefund:    isRefund,
		GroupTx:     groupTx,
		Source: &dbtypes.AtomicSwapForTokenData{
			Contracts: make([]*dbtypes.AtomicSwapTxData, 0),
			Results:   make([]*dbtypes.AtomicSwapTxData, 0),
		},
	}
	// Get contract txs with
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectContractListByGroupTx, groupTx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var contractData dbtypes.AtomicSwapTxData
		err = rows.Scan(&contractData.Txid, &contractData.Time, &contractData.Value)
		if err != nil {
			return nil, err
		}
		contractData.TimeDisp = utils.DateTimeWithoutTimeZone(contractData.Time)
		// get spends of contract
		spendDatas, err := pgb.GetSwapFullDataByContractTx(contractData.Txid, groupTx)
		if err != nil {
			return nil, err
		}
		// check and insert to Source spends data
		for _, spend := range spendDatas {
			exist := false
			for index, existSpend := range cSwapData.Source.Results {
				if spend.Txid == existSpend.Txid {
					exist = true
					existSpend.Value += spend.Value
					cSwapData.Source.Results[index] = existSpend
					break
				}
			}
			if !exist {
				cSwapData.Source.Results = append(cSwapData.Source.Results, spend)
			}
		}
		contractTxHash, err := chainhash.NewHashFromStr(contractData.Txid)
		if err != nil {
			return nil, err
		}
		contractTxRaw, err := pgb.Client.GetRawTransactionVerbose(pgb.ctx, contractTxHash)
		if err != nil {
			return nil, err
		}
		contractData.Height = contractTxRaw.BlockHeight
		contractFees, err := txhelpers.GetTxFee(contractTxRaw)
		if err != nil {
			return nil, err
		}
		contractData.Fees = int64(contractFees)
		cSwapData.Source.TotalAmount += contractData.Value
		cSwapData.Source.Contracts = append(cSwapData.Source.Contracts, &contractData)
	}
	cSwapData.Time = cSwapData.Source.Contracts[0].Time
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	// Get remaining token swap info on pair
	targetData := &dbtypes.AtomicSwapForTokenData{}
	switch targetTokenString {
	case mutilchain.TYPEBTC:
		targetData, _ = pgb.GetBTCAtomicSwapTarget(groupTx)
	case mutilchain.TYPELTC:
		targetData, _ = pgb.GetLTCAtomicSwapTarget(groupTx)
	}
	cSwapData.Target = targetData
	// get swap data for decred by contract list
	return cSwapData, nil
}

// GetAtomicSwapList fetches filtered atomic swap list.
func (pgb *ChainDB) GetAtomicSwapList(n, offset int64, pair, status, searchKey string) (swaps []*dbtypes.AtomicSwapFullData, allFilterCount int64, err error) {
	// get count all atomic swaps with filter pair, status
	if searchKey != "" {
		err = pgb.db.QueryRow(internal.MakeCountAtomicSwapsRowWithSearchFilter(pair, status), searchKey).Scan(&allFilterCount)
	} else {
		err = pgb.db.QueryRow(internal.MakeCountAtomicSwapsRowWithFilter(pair, status)).Scan(&allFilterCount)
	}
	if err != nil {
		log.Errorf("Get count atomic swaps faled: %v", err)
		return
	}
	var rows *sql.Rows
	atomicSwapsContractGroupQuery := pgb.GetAtomicSwapsContractGroupQuery(pair, status, searchKey)
	if searchKey != "" {
		rows, err = pgb.db.QueryContext(pgb.ctx, atomicSwapsContractGroupQuery, searchKey, n, offset)
	} else {
		rows, err = pgb.db.QueryContext(pgb.ctx, atomicSwapsContractGroupQuery, n, offset)
	}

	if err != nil {
		log.Errorf("Get atomic swaps list faled: %v", err)
		return
	}

	defer rows.Close()
	for rows.Next() {
		var groupTx string
		var targetToken sql.NullString
		var isRefund bool
		err = rows.Scan(&groupTx, &targetToken, &isRefund)
		if err != nil {
			return
		}
		targetTokenString := ""
		if targetToken.Valid {
			targetTokenString = targetToken.String
		}
		var swapItem *dbtypes.AtomicSwapFullData
		swapItem, err = pgb.GetContractSwapDataByGroup(groupTx, targetTokenString)
		if err != nil {
			return
		}
		swaps = append(swaps, swapItem)
	}
	err = rows.Err()
	return
}

func (pgb *ChainDB) GetAtomicSwapSummary() (txCount, amount, oldestContract int64, err error) {
	// get count all atomic swaps
	err = pgb.db.QueryRow(internal.CountAtomicSwapsRow).Scan(&txCount)
	if err != nil {
		return
	}
	// get total trading amount
	err = pgb.db.QueryRow(internal.SelectTotalTradingAmount).Scan(&amount)
	if err != nil {
		return
	}
	// get oldest contract time on swaps txs
	err = pgb.db.QueryRow(internal.SelectOldestContractTime).Scan(&oldestContract)
	return
}

func (pgb *ChainDB) CountRefundContract() (int64, error) {
	var refundCount int64
	err := pgb.db.QueryRow(internal.CountRefundAtomicSwapsRow).Scan(&refundCount)
	if err != nil {
		return 0, err
	}
	return refundCount, nil
}

// GetBTCAtomicSwapTarget return atomic swap detail of BTC
func (pgb *ChainDB) GetBTCAtomicSwapTarget(groupTx string) (*dbtypes.AtomicSwapForTokenData, error) {
	targetData := &dbtypes.AtomicSwapForTokenData{
		Contracts: make([]*dbtypes.AtomicSwapTxData, 0),
		Results:   make([]*dbtypes.AtomicSwapTxData, 0),
	}
	// Get contract txs with
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectBTCContractListByGroupTx, groupTx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var contractData dbtypes.AtomicSwapTxData
		err = rows.Scan(&contractData.Txid, &contractData.Value)
		if err != nil {
			return nil, err
		}
		// get spends of contract
		spendDatas, err := pgb.GetBTCSwapFullDataByContractTx(contractData.Txid, groupTx)
		if err != nil {
			return nil, err
		}
		// check and insert to Source spends data
		for _, spend := range spendDatas {
			exist := false
			for index, existSpend := range targetData.Results {
				if spend.Txid == existSpend.Txid {
					exist = true
					existSpend.Value += spend.Value
					targetData.Results[index] = existSpend
					break
				}
			}
			if !exist {
				targetData.Results = append(targetData.Results, spend)
			}
		}

		contractTxHash, err := btc_chainhash.NewHashFromStr(contractData.Txid)
		if err != nil {
			return nil, err
		}
		contractTxRaw, err := pgb.BtcClient.GetRawTransactionVerbose(contractTxHash)
		if err != nil {
			return nil, err
		}
		targetTxRaw, err := pgb.BtcClient.GetRawTransaction(contractTxHash)
		if err != nil {
			return nil, err
		}
		targetBlockHash, err := btc_chainhash.NewHashFromStr(contractTxRaw.BlockHash)
		if err != nil {
			return nil, err
		}
		targetBlockHeader, err := pgb.BtcClient.GetBlockHeaderVerbose(targetBlockHash)
		if err != nil {
			return nil, err
		}
		contractData.Height = int64(targetBlockHeader.Height)
		contractFees, err := txhelpers.CalculateBTCTxFee(pgb.BtcClient, targetTxRaw.MsgTx())
		if err != nil {
			return nil, err
		}
		contractData.Fees = int64(contractFees)
		contractData.Time = contractTxRaw.Time
		contractData.TimeDisp = utils.DateTimeWithoutTimeZone(contractData.Time)
		targetData.TotalAmount += contractData.Value
		targetData.Contracts = append(targetData.Contracts, &contractData)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return targetData, nil
}

// GetMutilchainVoutIndexsOfContract return vout index of contract
func (pgb *ChainDB) GetMutilchainVoutIndexsOfContract(contractTx, chainType string) ([]int, error) {
	res := make([]int, 0)
	rows, err := pgb.db.QueryContext(pgb.ctx, fmt.Sprintf(internal.SelectVoutIndexOfContract, chainType), contractTx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var vout int
		err = rows.Scan(&vout)
		if err != nil {
			return nil, err
		}
		res = append(res, vout)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetMutilchainVinIndexsOfRedeem return vin index of spend tx
func (pgb *ChainDB) GetMutilchainVinIndexsOfRedeem(spendTx, chainType string) ([]int, error) {
	res := make([]int, 0)
	rows, err := pgb.db.QueryContext(pgb.ctx, fmt.Sprintf(internal.SelectVinIndexOfRedeem, chainType), spendTx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var vin int
		err = rows.Scan(&vin)
		if err != nil {
			return nil, err
		}
		res = append(res, vin)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return res, nil
}

// GetLTCAtomicSwapTarget return atomic swap detail of LTC
func (pgb *ChainDB) GetLTCAtomicSwapTarget(groupTx string) (*dbtypes.AtomicSwapForTokenData, error) {
	targetData := &dbtypes.AtomicSwapForTokenData{
		Contracts: make([]*dbtypes.AtomicSwapTxData, 0),
		Results:   make([]*dbtypes.AtomicSwapTxData, 0),
	}
	// Get contract txs with
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectLTCContractListByGroupTx, groupTx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var contractData dbtypes.AtomicSwapTxData
		err = rows.Scan(&contractData.Txid, &contractData.Value)
		if err != nil {
			return nil, err
		}
		// get spends of contract
		spendDatas, err := pgb.GetLTCSwapFullDataByContractTx(contractData.Txid, groupTx)
		if err != nil {
			return nil, err
		}
		// check and insert to Source spends data
		for _, spend := range spendDatas {
			exist := false
			for index, existSpend := range targetData.Results {
				if spend.Txid == existSpend.Txid {
					exist = true
					existSpend.Value += spend.Value
					targetData.Results[index] = existSpend
					break
				}
			}
			if !exist {
				targetData.Results = append(targetData.Results, spend)
			}
		}

		contractTxHash, err := ltc_chainhash.NewHashFromStr(contractData.Txid)
		if err != nil {
			return nil, err
		}
		contractTxRaw, err := pgb.LtcClient.GetRawTransactionVerbose(contractTxHash)
		if err != nil {
			return nil, err
		}
		targetTxRaw, err := pgb.LtcClient.GetRawTransaction(contractTxHash)
		if err != nil {
			return nil, err
		}
		targetBlockHash, err := ltc_chainhash.NewHashFromStr(contractTxRaw.BlockHash)
		if err != nil {
			return nil, err
		}
		targetBlockHeader, err := pgb.LtcClient.GetBlockHeaderVerbose(targetBlockHash)
		if err != nil {
			return nil, err
		}
		contractData.Height = int64(targetBlockHeader.Height)
		contractFees, err := txhelpers.CalculateLTCTxFee(pgb.LtcClient, targetTxRaw.MsgTx())
		if err != nil {
			return nil, err
		}
		contractData.Fees = int64(contractFees)
		contractData.Time = contractTxRaw.Time
		contractData.TimeDisp = utils.DateTimeWithoutTimeZone(contractData.Time)
		targetData.TotalAmount += contractData.Value
		targetData.Contracts = append(targetData.Contracts, &contractData)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return targetData, nil
}

func (pgb *ChainDB) TreasuryTxns(n, offset int64, txType stake.TxType) ([]*dbtypes.TreasuryTx, error) {
	return pgb.TreasuryTxnsWithPeriod(n, offset, txType, 0, 0)
}

// Get all Tspend
func (pgb *ChainDB) GetAllTSpendTxns() ([]*dbtypes.TreasuryTx, error) {
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectTypedTreasuryTxnsAll, stake.TxTypeTSpend)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var txns []*dbtypes.TreasuryTx
	for rows.Next() {
		var tx dbtypes.TreasuryTx
		var mainchain bool
		err = rows.Scan(&tx.TxID, &tx.Type, &tx.Amount, &tx.BlockHash, &tx.BlockHeight, &tx.BlockTime, &mainchain)
		if err != nil {
			return nil, err
		}
		txns = append(txns, &tx)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return txns, nil
}

// TreasuryTxns fetches filtered treasury transactions.
func (pgb *ChainDB) TreasuryTxnsWithPeriod(n, offset int64, txType stake.TxType, year int64, month int64) ([]*dbtypes.TreasuryTx, error) {
	var rows *sql.Rows
	var err error
	switch txType {
	case -1:
		if year == 0 {
			rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectTreasuryTxns, n, offset)
		} else if month == 0 {
			rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectTreasuryTxnsYear, year, n, offset)
		} else {
			rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectTreasuryTxnsYearMonth, year, month, n, offset)
		}
	default:
		if year == 0 {
			rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectTypedTreasuryTxns, txType, n, offset)
		} else if month == 0 {
			rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectTypedTreasuryTxnsYear, txType, year, n, offset)
		} else {
			rows, err = pgb.db.QueryContext(pgb.ctx, internal.SelectTypedTreasuryTxnsYearMonth, txType, year, month, n, offset)
		}
	}

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var txns []*dbtypes.TreasuryTx
	for rows.Next() {
		var tx dbtypes.TreasuryTx
		var mainchain bool
		err = rows.Scan(&tx.TxID, &tx.Type, &tx.Amount, &tx.BlockHash, &tx.BlockHeight, &tx.BlockTime, &mainchain)
		if err != nil {
			return nil, err
		}
		// get vote info if tx is tspend
		if tx.Type == int(stake.TxTypeTSpend) {
			tspendMeta, err := pgb.getTSpendSimpleVoteInfo(tx.TxID)
			if err != nil {
				log.Warnf("Get Tspend vote info failed. TxID: %s, Error: %v", tx.TxID, err)
				continue
			}
			tx.TSpendMeta = tspendMeta
		}
		txns = append(txns, &tx)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}
	return txns, nil
}

// Get Simple tspend vote info (yes, no, total votes, approval rate)
func (pgb *ChainDB) getTSpendSimpleVoteInfo(txHash string) (*dbtypes.TreasurySpendVotesSummaryData, error) {
	var res dbtypes.TreasurySpendVotesSummaryData
	err := pgb.db.QueryRow(internal.SelectTSpendVotesSummary, dbtypes.TSpendYes, dbtypes.TSpendNo, txHash).Scan(&res.YesVotes, &res.NoVotes, &res.TotalVotes)
	if err != nil {
		return nil, err
	}
	res.Approval = float32(res.YesVotes) / float32(res.TotalVotes)
	return &res, nil
}

func (pgb *ChainDB) updateProjectFundCache() error {
	_, _, err := pgb.AddressHistoryAll(pgb.devAddress, 1, 0)
	if err != nil && !IsRetryError(err) {
		err = pgb.replaceCancelError(err)
		return fmt.Errorf("failed to update project fund data: %w", err)
	}
	log.Infof("Project fund data is up-to-date.")
	return nil
	// Similar to individually updating balance and rows, but more efficient:
	// pgb.AddressBalance(pgb.devAddress)
	// pgb.AddressRowsCompact(pgb.devAddress)
}

// FreshenAddressCaches resets the address balance cache by purging data for the
// addresses listed in expireAddresses, and prefetches the project fund balance
// if devPrefetch is enabled and not mid-reorg. The project fund update is run
// asynchronously if lazyProjectFund is true.
func (pgb *ChainDB) FreshenAddressCaches(lazyProjectFund bool, expireAddresses []string) error {
	// Clear existing cache entries.
	numCleared := pgb.AddressCache.Clear(expireAddresses)
	if expireAddresses == nil {
		log.Debugf("Cleared cache of all %d cached addresses.", numCleared)
	} else if len(expireAddresses) > 0 {
		log.Debugf("Cleared cache of %d of %d addresses with activity.", numCleared, len(expireAddresses))
	}

	// Do not initiate project fund queries if a reorg is in progress, or
	// pre-fetch is disabled.
	if !pgb.devPrefetch || pgb.InReorg {
		return nil
	}

	// Update project fund data.
	log.Infof("Pre-fetching project fund data at height %d...", pgb.Height())
	if lazyProjectFund {
		go func() {
			if err := pgb.updateProjectFundCache(); err != nil {
				log.Error(err)
			}
		}()
		return nil
	}
	return pgb.updateProjectFundCache()
}

// DevBalance returns the current development/project fund balance, updating the
// cached balance if it is stale. DevBalance differs slightly from
// addressBalance(devAddress) in that it will not initiate a DB query if a chain
// reorganization is in progress.
func (pgb *ChainDB) DevBalance() (*dbtypes.AddressBalance, error) {
	// Check cache first.
	cachedBalance, validBlock := pgb.AddressCache.Balance(pgb.devAddress) // bestBlockHash := pgb.BestBlockHash()
	if cachedBalance != nil && validBlock != nil /*  && validBlock.Hash == *bestBlockHash */ {
		return cachedBalance, nil
	}

	if !pgb.InReorg {
		bal, _, err := pgb.AddressBalance(pgb.devAddress)
		if err != nil {
			return nil, err
		}
		return bal, nil
	}

	// In reorg and cache is stale.
	if cachedBalance != nil {
		return cachedBalance, nil
	}
	return nil, fmt.Errorf("unable to query for balance during reorg")
}

// GetLast5PoolDataList return last 5 block pool info
func (pgb *ChainDB) GetLast5PoolDataList() (last5PoolData []*dbtypes.PoolDataItem, err error) {
	last5PoolData, err = externalapi.GetLastBlocksPool()
	return
}

// GetLast5PoolDataList return last 5 block pool info
func (pgb *ChainDB) GetLastMultichainPoolDataList(chainType string, startHeight int64) ([]*dbtypes.MultichainPoolDataItem, error) {
	switch chainType {
	case mutilchain.TYPEBTC:
		return externalapi.GetBitcoinLastBlocksPool(startHeight)
	case mutilchain.TYPELTC:
		return externalapi.GetLitecoinLastBlocksPool(startHeight)
	default:
		return make([]*dbtypes.MultichainPoolDataItem, 0), nil
	}
}

// AddressBalance attempts to retrieve balance information for a specific
// address from cache, and if cache is stale or missing data for the address, a
// DB query is used. A successful DB query will freshen the cache.
func (pgb *ChainDB) AddressBalance(address string) (bal *dbtypes.AddressBalance, cacheUpdated bool, err error) {
	_, err = stdaddr.DecodeAddress(address, pgb.chainParams)
	if err != nil {
		return
	}

	// Check the cache first.
	bestHash, height := pgb.BestBlock()
	var validBlock *cache.BlockID
	bal, validBlock = pgb.AddressCache.Balance(address) // bal is a copy
	if bal != nil && validBlock != nil /* && validBlock.Hash == *bestHash */ {
		log.Tracef("AddressBalance: cache HIT for %s.", address)
		return
	}

	busy, wait, done := pgb.CacheLocks.bal.TryLock(address)
	if busy {
		// Let others get the wait channel while we wait.
		// To return stale cache data if it is available:
		// bal, _ := pgb.AddressCache.Balance(address)
		// if bal != nil {
		// 	return bal, nil
		// }
		<-wait

		// Try again, starting with the cache.
		return pgb.AddressBalance(address)
	}

	// We will run the DB query, so block others from doing the same. When query
	// and/or cache update is completed, broadcast to any waiters that the coast
	// is clear.
	defer done()

	log.Tracef("AddressBalance: cache MISS for %s.", address)

	// Cache is empty or stale, so query the DB.
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	bal, err = RetrieveAddressBalance(ctx, pgb.db, address)
	if err != nil {
		err = pgb.replaceCancelError(err)
		return
	}

	// Update the address cache.
	cacheUpdated = pgb.AddressCache.StoreBalance(address, bal,
		cache.NewBlockID(bestHash, height)) // stores a copy of bal
	return
}

func (pgb *ChainDB) IsMutilchainValidAddress(chainType string, address string) bool {
	var err error
	switch chainType {
	case mutilchain.TYPEBTC:
		_, err = btcutil.DecodeAddress(address, pgb.btcChainParams)
	case mutilchain.TYPELTC:
		_, err = ltcutil.DecodeAddress(address, pgb.ltcChainParams)
	default:
		return false
	}
	return err == nil
}

func (pgb *ChainDB) MutilchainAddressBalance(address string, chainType string) (bal *dbtypes.AddressBalance, cacheUpdated bool, err error) {
	isValidAddress := pgb.IsMutilchainValidAddress(chainType, address)
	if !isValidAddress {
		return
	}

	// Check the cache first.
	bestHash, height := pgb.GetMutilchainHashHeight(chainType)
	var validBlock *cache.MutilchainBlockID
	bal, validBlock = pgb.AddressCache.MutilchainBalance(address, chainType) // bal is a copy
	if bal != nil && validBlock != nil /* && validBlock.Hash == *bestHash */ {
		log.Tracef("AddressBalance: cache HIT for %s.", address)
		return
	}

	busy, wait, done := pgb.CacheLocks.bal.TryLock(address)
	if busy {
		// Let others get the wait channel while we wait.
		// To return stale cache data if it is available:
		// bal, _ := pgb.AddressCache.Balance(address)
		// if bal != nil {
		// 	return bal, nil
		// }
		<-wait

		// Try again, starting with the cache.
		return pgb.MutilchainAddressBalance(address, chainType)
	}

	// We will run the DB query, so block others from doing the same. When query
	// and/or cache update is completed, broadcast to any waiters that the coast
	// is clear.
	defer done()

	log.Tracef("AddressBalance: cache MISS for %s.", address)

	// Cache is empty or stale, so query the DB.
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	bal, err = RetrieveMutilchainAddressBalance(ctx, pgb.db, address, chainType)
	if err != nil {
		err = pgb.replaceCancelError(err)
		return
	}

	// Update the address cache.
	cacheUpdated = pgb.AddressCache.StoreMutilchainBalance(address, bal, cache.NewMutilchainBlockID(bestHash, height), chainType) // stores a copy of bal
	return
}

// updateAddressRows updates address rows, or waits for them to update by an
// ongoing query. On completion, the cache should be ready, although it must be
// checked again. The returned []*dbtypes.AddressRow contains ALL non-merged
// address transaction rows that were stored in the cache.
func (pgb *ChainDB) updateAddressRows(address string, year int64, month int64) (rows []*dbtypes.AddressRow, err error) {
	busy, wait, done := pgb.CacheLocks.rows.TryLock(address)
	if busy {
		// Just wait until the updater is finished.
		<-wait
		err = retryError{}
		return
	}

	// We will run the DB query, so block others from doing the same. When query
	// and/or cache update is completed, broadcast to any waiters that the coast
	// is clear.
	defer done()

	// Prior to performing the query, clear the old rows to save memory.
	pgb.AddressCache.ClearRows(address)

	hash, height := pgb.BestBlock()
	blockID := cache.NewBlockID(hash, height)

	// Retrieve all non-merged address transaction rows.
	rows, err = pgb.AddressTransactionsAll(address, year, month)
	if err != nil && !errors.Is(err, dbtypes.ErrNoResult) {
		return
	}

	// Update address rows cache.
	pgb.AddressCache.StoreRows(address, rows, blockID)
	return
}

func (pgb *ChainDB) GetMutilchainHashHeight(chainType string) (hash string, height int64) {
	switch chainType {
	case mutilchain.TYPEBTC:
		chainHash, btcheight := pgb.BTCBestBlock()
		if chainHash == nil {
			return "", 0
		}
		hash = chainHash.String()
		height = btcheight
	case mutilchain.TYPELTC:
		chainHash, ltcheight := pgb.LTCBestBlock()
		if chainHash == nil {
			return "", 0
		}
		hash = chainHash.String()
		height = ltcheight
	default:
		chainHash, dcrheight := pgb.BestBlock()
		hash = chainHash.String()
		height = dcrheight
	}
	return
}

func (pgb *ChainDB) updateMutilchainAddressRows(address string, chainType string) (rows []*dbtypes.MutilchainAddressRow, err error) {
	busy, wait, done := pgb.CacheLocks.rows.TryLock(address)
	if busy {
		// Just wait until the updater is finished.
		<-wait
		err = retryError{}
		return
	}

	// We will run the DB query, so block others from doing the same. When query
	// and/or cache update is completed, broadcast to any waiters that the coast
	// is clear.
	defer done()
	// Prior to performing the query, clear the old rows to save memory.
	pgb.AddressCache.ClearMutilchainRows(address, chainType)
	hash, height := pgb.GetMutilchainHashHeight(chainType)
	blockID := cache.NewMutilchainBlockID(hash, height)
	// Retrieve all non-merged address transaction rows.
	rows, err = pgb.MutilchainAddressTransactionsAll(address, chainType)
	if err != nil && !errors.Is(err, dbtypes.ErrNoResult) {
		return
	}
	// Update address rows cache.
	pgb.AddressCache.StoreMutilchainRows(address, rows, blockID, chainType)
	return
}

// AddressRowsMerged gets the merged address rows either from cache or via DB
// query.
func (pgb *ChainDB) AddressRowsMerged(address string) ([]*dbtypes.AddressRowMerged, error) {
	_, err := stdaddr.DecodeAddress(address, pgb.chainParams)
	if err != nil {
		return nil, err
	}

	// Try the address cache.
	rowsCompact, validBlock := pgb.AddressCache.Rows(address)
	cacheHit := rowsCompact != nil && validBlock != nil
	// hash := pgb.BestBlockHash()
	// cacheHit = validBlock != nil && validBlock.Hash == *hash
	if cacheHit {
		log.Tracef("AddressRowsMerged: rows cache HIT for %s.", address)
		return dbtypes.MergeRowsCompact(rowsCompact), nil
	}

	// Make the pointed to AddressRowMerged structs eligible for garbage
	// collection. pgb.updateAddressRows sets a new AddressRowMerged slice
	// retrieved from the database, so we do not want to hang on to a copy of
	// the old data.
	//nolint:ineffassign
	rowsCompact = nil

	log.Tracef("AddressRowsMerged: rows cache MISS for %s.", address)

	// Update or wait for an update to the cached AddressRows.
	rows, err := pgb.updateAddressRows(address, 0, 0)
	if err != nil {
		if IsRetryError(err) {
			// Try again, starting with cache.
			return pgb.AddressRowsMerged(address)
		}
		return nil, err
	}

	// We have a result.
	return dbtypes.MergeRows(rows)
}

// AddressRowsCompact gets non-merged address rows either from cache or via DB
// query.
func (pgb *ChainDB) AddressRowsCompact(address string) ([]*dbtypes.AddressRowCompact, error) {
	_, err := stdaddr.DecodeAddress(address, pgb.chainParams)
	if err != nil {
		return nil, err
	}

	// Try the address cache.
	rowsCompact, validBlock := pgb.AddressCache.Rows(address)
	cacheHit := rowsCompact != nil && validBlock != nil
	// hash := pgb.BestBlockHash()
	// cacheHit = validBlock != nil && validBlock.Hash == *hash
	if cacheHit {
		log.Tracef("AddressRowsCompact: rows cache HIT for %s.", address)
		return rowsCompact, nil
	}

	// Make the pointed to AddressRowCompact structs eligible for garbage
	// collection. pgb.updateAddressRows sets a new AddressRowCompact slice
	// retrieved from the database, so we do not want to hang on to a copy of
	// the old data.
	//nolint:ineffassign
	rowsCompact = nil

	log.Tracef("AddressRowsCompact: rows cache MISS for %s.", address)

	// Update or wait for an update to the cached AddressRows.
	rows, err := pgb.updateAddressRows(address, 0, 0)
	if err != nil {
		if IsRetryError(err) {
			// Try again, starting with cache.
			return pgb.AddressRowsCompact(address)
		}
		return nil, err
	}

	// We have a result.
	return dbtypes.CompactRows(rows), err
}

// retrieveMergedTxnCount queries the DB for the merged address transaction view
// row count.
func (pgb *ChainDB) retrieveMergedTxnCount(addr string, txnView dbtypes.AddrTxnViewType) (int, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	var count int64
	var err error
	switch txnView {
	case dbtypes.AddrMergedTxnDebit:
		count, err = CountMergedSpendingTxns(ctx, pgb.db, addr)
	case dbtypes.AddrMergedTxnCredit:
		count, err = CountMergedFundingTxns(ctx, pgb.db, addr)
	case dbtypes.AddrMergedTxn:
		count, err = CountMergedTxns(ctx, pgb.db, addr)
	default:
		return 0, fmt.Errorf("retrieveMergedTxnCount: requested count for non-merged view")
	}
	return int(count), err
}

// mergedTxnCount checks cache and falls back to retrieveMergedTxnCount.
func (pgb *ChainDB) mergedTxnCount(addr string, txnView dbtypes.AddrTxnViewType) (int, error) {
	// Try the cache first.
	rows, blockID := pgb.AddressCache.Rows(addr)
	if blockID == nil {
		// Query the DB.
		return pgb.retrieveMergedTxnCount(addr, txnView)
	}

	return dbtypes.CountMergedRowsCompact(rows, txnView)
}

// nonMergedTxnCount gets the non-merged address transaction view row count via
// AddressBalance, which checks the cache and falls back to a DB query.
func (pgb *ChainDB) nonMergedTxnCount(addr string, txnView dbtypes.AddrTxnViewType) (int, error) {
	bal, _, err := pgb.AddressBalance(addr)
	if err != nil {
		return 0, err
	}
	var count int64
	switch txnView {
	case dbtypes.AddrTxnAll:
		count = (bal.NumSpent * 2) + bal.NumUnspent
	case dbtypes.AddrTxnCredit:
		count = bal.NumSpent + bal.NumUnspent
	case dbtypes.AddrTxnDebit:
		count = bal.NumSpent
	default:
		return 0, fmt.Errorf("NonMergedTxnCount: requested count for merged view")
	}
	return int(count), nil
}

func (pgb *ChainDB) GetBlockchainSummaryInfo() (addrCount, outputs int64, err error) {
	// get blockchain total address count, outputs count
	err = pgb.db.QueryRow(internal.CountAddressOutputs).Scan(&addrCount, &outputs)
	return
}

// CountTransactions gets the total row count for the given address and address
// transaction view.
func (pgb *ChainDB) CountTransactions(addr string, txnView dbtypes.AddrTxnViewType) (int, error) {
	_, err := stdaddr.DecodeAddress(addr, pgb.chainParams)
	if err != nil {
		return 0, err
	}

	merged, err := txnView.IsMerged()
	if err != nil {
		return 0, err
	}

	countFn := pgb.nonMergedTxnCount
	if merged {
		countFn = pgb.mergedTxnCount
	}

	count, err := countFn(addr, txnView)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Get mutilchain address history
func (pgb *ChainDB) MutilchainAddressHistory(address string, N, offset int64,
	txnView dbtypes.AddrTxnViewType, chainType string) ([]*dbtypes.MutilchainAddressRow, *dbtypes.AddressBalance, error) {
	// Try the address rows cache
	hash, height := pgb.GetMutilchainHashHeight(chainType)
	addressRows, validBlock, err := pgb.AddressCache.MutilchainTransactions(address, N, offset, txnView, chainType)
	if err != nil {
		return nil, nil, err
	}

	if addressRows == nil || validBlock == nil /* || validBlock.Hash != *hash) */ {
		//nolint:ineffassign
		addressRows = nil // allow garbage collection of each AddressRow in cache.
		log.Debugf("AddressHistory: Address rows (view=%s) cache MISS for %s.",
			txnView.String(), address)

		// Update or wait for an update to the cached AddressRows, returning ALL
		// NON-MERGED address transaction rows.
		addressRows, err = pgb.updateMutilchainAddressRows(address, chainType)
		if err != nil && !errors.Is(err, dbtypes.ErrNoResult) && !errors.Is(err, sql.ErrNoRows) {
			// See if another caller ran the update, in which case we were just
			// waiting to avoid a simultaneous query. With luck the cache will
			// be updated with this data, although it may not be. Try again.
			if IsRetryError(err) {
				// Try again, starting with cache.
				return pgb.MutilchainAddressHistory(address, N, offset, txnView, chainType)
			}
			return nil, nil, fmt.Errorf("failed to updateAddressRows: %w", err)
		}

		// Select the correct type and range of address rows, merging if needed.
		addressRows, err = dbtypes.MutilchainSliceAddressRows(addressRows, int(N), int(offset), txnView)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to SliceAddressRows: %w", err)
		}
	}
	log.Debugf("AddressHistory: Address rows (view=%s) cache HIT for %s.",
		txnView.String(), address)

	// addressRows is now present and current. Proceed to get the balance.

	// Try the address balance cache.
	balance, _ := pgb.AddressCache.MutilchainBalance(address, chainType) // balance is a copy
	if balance != nil /* && validBlock != nil && validBlock.Hash == *hash) */ {
		log.Debugf("AddressHistory: Address balance cache HIT for %s.", address)
		return addressRows, balance, nil
	}
	log.Debugf("AddressHistory: Address balance cache MISS for %s.", address)

	// Short cut: we have all txs when the total number of fetched txs is less
	// than the limit, txtype is AddrTxnAll, and Offset is zero.
	if len(addressRows) < int(N) && offset == 0 && txnView == dbtypes.AddrTxnAll {
		log.Debugf("Taking balance shortcut since address rows includes all.")
		// Zero balances and txn counts when rows is zero length.
		if len(addressRows) == 0 {
			balance = &dbtypes.AddressBalance{
				Address: address,
			}
		} else {
			addrInfo := dbtypes.ReduceMutilchainAddressHistory(addressRows, chainType)
			if addrInfo == nil {
				return addressRows, nil,
					fmt.Errorf("ReduceAddressHistory failed. len(addressRows) = %d",
						len(addressRows))
			}

			balance = &dbtypes.AddressBalance{
				Address:      address,
				NumSpent:     addrInfo.NumSpendingTxns,
				NumUnspent:   addrInfo.NumFundingTxns - addrInfo.NumSpendingTxns,
				TotalSpent:   int64(addrInfo.Sent),
				TotalUnspent: int64(addrInfo.Unspent),
			}
		}
		// Update balance cache.
		blockID := cache.NewMutilchainBlockID(hash, height)
		pgb.AddressCache.StoreMutilchainBalance(address, balance, blockID, chainType) // a copy of balance is stored
	} else {
		// Count spent/unspent amounts and transactions.
		log.Debugf("AddressHistory: Obtaining balance via DB query.")
		balance, _, err = pgb.MutilchainAddressBalance(address, chainType)
		if err != nil && !errors.Is(err, dbtypes.ErrNoResult) && !errors.Is(err, sql.ErrNoRows) {
			return nil, nil, err
		}
	}
	log.Infof("From DB: Address: %s: %d spent totalling %f %s, %d unspent totalling %f %s",
		address, balance.NumSpent, dbtypes.GetMutilchainCoinAmount(balance.TotalSpent, chainType), strings.ToUpper(chainType),
		balance.NumUnspent, dbtypes.GetMutilchainCoinAmount(balance.TotalUnspent, chainType), strings.ToUpper(chainType))
	log.Infof("From DB (%s): Receive count for address %s: count = %d at block %d.", strings.ToUpper(chainType),
		address, balance.NumSpent+balance.NumUnspent, height)

	return addressRows, balance, nil
}

// AddressHistory queries the database for rows of the addresses table
// containing values for a certain type of transaction (all, credits, or debits)
// for the given address.
func (pgb *ChainDB) AddressHistory(address string, N, offset int64,
	txnView dbtypes.AddrTxnViewType, year int64, month int64) ([]*dbtypes.AddressRow, *dbtypes.AddressBalance, error) {
	_, err := stdaddr.DecodeAddress(address, pgb.chainParams)
	if err != nil {
		return nil, nil, err
	}

	// Try the address rows cache.
	hash, height := pgb.BestBlock()
	addressRows, validBlock, err := pgb.AddressCache.TransactionsYearMonth(address, N, offset, txnView, year, month)
	if err != nil {
		return nil, nil, err
	}

	if addressRows == nil || validBlock == nil /* || validBlock.Hash != *hash) */ {
		//nolint:ineffassign
		addressRows = nil // allow garbage collection of each AddressRow in cache.
		log.Debugf("AddressHistory: Address rows (view=%s) cache MISS for %s.",
			txnView.String(), address)

		// Update or wait for an update to the cached AddressRows, returning ALL
		// NON-MERGED address transaction rows.
		addressRows, err = pgb.updateAddressRows(address, year, month)
		if err != nil && !errors.Is(err, dbtypes.ErrNoResult) && !errors.Is(err, sql.ErrNoRows) {
			// See if another caller ran the update, in which case we were just
			// waiting to avoid a simultaneous query. With luck the cache will
			// be updated with this data, although it may not be. Try again.
			if IsRetryError(err) {
				// Try again, starting with cache.
				return pgb.AddressHistory(address, N, offset, txnView, year, month)
			}
			return nil, nil, fmt.Errorf("failed to updateAddressRows: %w", err)
		}

		// Select the correct type and range of address rows, merging if needed.
		addressRows, err = dbtypes.SliceAddressRows(addressRows, int(N), int(offset), txnView)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to SliceAddressRows: %w", err)
		}
	}
	log.Debugf("AddressHistory: Address rows (view=%s) cache HIT for %s.",
		txnView.String(), address)

	// addressRows is now present and current. Proceed to get the balance.

	// Try the address balance cache.
	balance, _ := pgb.AddressCache.Balance(address) // balance is a copy
	if balance != nil /* && validBlock != nil && validBlock.Hash == *hash) */ {
		log.Debugf("AddressHistory: Address balance cache HIT for %s.", address)
		return addressRows, balance, nil
	}
	log.Debugf("AddressHistory: Address balance cache MISS for %s.", address)

	// Short cut: we have all txs when the total number of fetched txs is less
	// than the limit, txtype is AddrTxnAll, and Offset is zero.
	if len(addressRows) < int(N) && offset == 0 && txnView == dbtypes.AddrTxnAll {
		log.Debugf("Taking balance shortcut since address rows includes all.")
		// Zero balances and txn counts when rows is zero length.
		if len(addressRows) == 0 {
			balance = &dbtypes.AddressBalance{
				Address: address,
			}
		} else {
			addrInfo, fromStake, toStake := dbtypes.ReduceAddressHistory(addressRows)
			if addrInfo == nil {
				return addressRows, nil,
					fmt.Errorf("ReduceAddressHistory failed. len(addressRows) = %d",
						len(addressRows))
			}

			balance = &dbtypes.AddressBalance{
				Address:      address,
				NumSpent:     addrInfo.NumSpendingTxns,
				NumUnspent:   addrInfo.NumFundingTxns - addrInfo.NumSpendingTxns,
				TotalSpent:   int64(addrInfo.AmountSent),
				TotalUnspent: int64(addrInfo.AmountUnspent),
				FromStake:    fromStake,
				ToStake:      toStake,
			}
		}
		// Update balance cache.
		blockID := cache.NewBlockID(hash, height)
		pgb.AddressCache.StoreBalance(address, balance, blockID) // a copy of balance is stored
	} else {
		// Count spent/unspent amounts and transactions.
		log.Debugf("AddressHistory: Obtaining balance via DB query.")
		balance, _, err = pgb.AddressBalance(address)
		if err != nil && !errors.Is(err, dbtypes.ErrNoResult) && !errors.Is(err, sql.ErrNoRows) {
			return nil, nil, err
		}
	}

	log.Infof("%s: %d spent totalling %f DCR, %d unspent totalling %f DCR",
		address, balance.NumSpent, dcrutil.Amount(balance.TotalSpent).ToCoin(),
		balance.NumUnspent, dcrutil.Amount(balance.TotalUnspent).ToCoin())
	log.Infof("Receive count for address %s: count = %d at block %d.",
		address, balance.NumSpent+balance.NumUnspent, height)

	return addressRows, balance, nil
}

// AddressData returns comprehensive, paginated information for an address.
func (pgb *ChainDB) AddressData(address string, limitN, offsetAddrOuts int64,
	txnType dbtypes.AddrTxnViewType, year int64, month int64) (addrData *dbtypes.AddressInfo, err error) {
	_, addrType, addrErr := txhelpers.AddressValidation(address, pgb.chainParams)
	if addrErr != nil && !errors.Is(err, txhelpers.AddressErrorNoError) {
		return nil, err
	}

	merged, err := txnType.IsMerged()
	if err != nil {
		return nil, err
	}

	addrHist, balance, err := pgb.AddressHistory(address, limitN, offsetAddrOuts, txnType, year, month)
	//if have period, get separator balance
	if year != 0 {
		balance, err = RetrieveAddressBalancePeriod(pgb.ctx, pgb.db, address, txnType, year, month)
	}
	if dbtypes.IsTimeoutErr(err) {
		return nil, err
	}

	populateTemplate := func() {
		addrData.Type = addrType
		addrData.Offset = offsetAddrOuts
		addrData.Limit = limitN
		addrData.TxnType = txnType.String()
		addrData.Address = address
	}

	if errors.Is(err, dbtypes.ErrNoResult) || errors.Is(err, sql.ErrNoRows) || (err == nil && len(addrHist) == 0) {
		// We do not have any confirmed transactions. Prep to display ONLY
		// unconfirmed transactions (or none at all).
		addrData = new(dbtypes.AddressInfo)
		populateTemplate()
		addrData.Balance = &dbtypes.AddressBalance{}
		log.Tracef("AddressHistory: No confirmed transactions for address %s.", address)
	} else if err != nil {
		// Unexpected error
		log.Errorf("AddressHistory: %v", err)
		return nil, fmt.Errorf("AddressHistory: %w", err)
	} else /*err == nil*/ {
		// Generate AddressInfo skeleton from the address table rows.
		addrData, _, _ = dbtypes.ReduceAddressHistory(addrHist)
		if addrData == nil {
			// Empty history is not expected for credit or all txnType with any
			// txns. i.e. Empty history is OK for debit views (merged or not).
			if (txnType != dbtypes.AddrTxnDebit && txnType != dbtypes.AddrMergedTxnDebit) &&
				(balance.NumSpent+balance.NumUnspent) > 0 {
				log.Debugf("empty address history (%s) for view %s: n=%d&start=%d",
					address, txnType.String(), limitN, offsetAddrOuts)
				return nil, fmt.Errorf("that address has no history")
			}
			addrData = new(dbtypes.AddressInfo)
		}

		// Balances and txn counts
		populateTemplate()
		addrData.Balance = balance
		addrData.KnownTransactions = (balance.NumSpent * 2) + balance.NumUnspent
		addrData.KnownFundingTxns = balance.NumSpent + balance.NumUnspent
		addrData.KnownSpendingTxns = balance.NumSpent

		// Obtain the TxnCount, which pertains to the number of table rows.
		addrData.IsMerged, err = txnType.IsMerged()
		if err != nil {
			return nil, err
		}
		if addrData.IsMerged {
			// For merged views, check the cache and fall back on a DB query.
			count, err := pgb.mergedTxnCount(address, txnType)
			if err != nil {
				return nil, err
			}
			addrData.TxnCount = int64(count)
		} else {
			// For non-merged views, use the balance data.
			switch txnType {
			case dbtypes.AddrTxnAll:
				addrData.TxnCount = addrData.KnownFundingTxns + addrData.KnownSpendingTxns
			case dbtypes.AddrTxnCredit:
				addrData.TxnCount = addrData.KnownFundingTxns
				addrData.Transactions = addrData.TxnsFunding
			case dbtypes.AddrTxnDebit:
				addrData.TxnCount = addrData.KnownSpendingTxns
				addrData.Transactions = addrData.TxnsSpending
			case dbtypes.AddrUnspentTxn:
				addrData.TxnCount = addrData.NumFundingTxns - addrData.NumSpendingTxns
			}
		}

		// Transactions on current page
		addrData.NumTransactions = int64(len(addrData.Transactions))
		if addrData.NumTransactions > limitN {
			addrData.NumTransactions = limitN
		}

		// Query database for transaction details.
		err = pgb.FillAddressTransactions(addrData)
		if dbtypes.IsTimeoutErr(err) {
			return nil, err
		}
		if err != nil {
			return nil, fmt.Errorf("Unable to fill address %s transactions: %w", address, err)
		}
	}

	// Check for unconfirmed transactions.
	addressUTXOs, numUnconfirmed, err := pgb.mp.UnconfirmedTxnsForAddress(address)
	if err != nil || addressUTXOs == nil {
		return nil, fmt.Errorf("UnconfirmedTxnsForAddress failed for address %s: %w", address, err)
	}
	addrData.NumUnconfirmed = numUnconfirmed
	addrData.NumTransactions += numUnconfirmed
	if addrData.UnconfirmedTxns == nil {
		addrData.UnconfirmedTxns = new(dbtypes.AddressTransactions)
	}

	// Funding transactions (unconfirmed)
	var received, sent, numReceived, numSent int64
FUNDING_TX_DUPLICATE_CHECK:
	for _, f := range addressUTXOs.Outpoints {
		// TODO: handle merged transactions
		if merged {
			break FUNDING_TX_DUPLICATE_CHECK
		}

		// Mempool transactions stick around for 2 blocks. The first block
		// incorporates the transaction and mines it. The second block
		// validates it by the stake. However, transactions move into our
		// database as soon as they are mined and thus we need to be careful
		// to not include those transactions in our list.
		for _, b := range addrData.Transactions {
			if f.Hash.String() == b.TxID && f.Index == b.InOutID {
				continue FUNDING_TX_DUPLICATE_CHECK
			}
		}
		fundingTx, ok := addressUTXOs.TxnsStore[f.Hash]
		if !ok {
			log.Errorf("An outpoint's transaction is not available in TxnStore.")
			continue
		}
		if fundingTx.Confirmed() {
			log.Errorf("An outpoint's transaction is unexpectedly confirmed.")
			continue
		}
		if txnType == dbtypes.AddrTxnAll || txnType == dbtypes.AddrTxnCredit || txnType == dbtypes.AddrUnspentTxn {
			addrTx := &dbtypes.AddressTx{
				TxID:          fundingTx.Hash().String(),
				TxType:        txhelpers.DetermineTxTypeString(fundingTx.Tx),
				InOutID:       f.Index,
				Time:          dbtypes.NewTimeDefFromUNIX(fundingTx.MemPoolTime),
				FormattedSize: humanize.Bytes(uint64(fundingTx.Tx.SerializeSize())),
				Total:         txhelpers.TotalOutFromMsgTx(fundingTx.Tx).ToCoin(),
				ReceivedTotal: dcrutil.Amount(fundingTx.Tx.TxOut[f.Index].Value).ToCoin(),
				IsFunding:     true,
			}
			addrData.Transactions = append(addrData.Transactions, addrTx)
		}
		received += fundingTx.Tx.TxOut[f.Index].Value
		numReceived++
	}

	// Spending transactions (unconfirmed)
SPENDING_TX_DUPLICATE_CHECK:
	for _, f := range addressUTXOs.PrevOuts {
		// TODO: handle merged transactions
		if merged {
			break SPENDING_TX_DUPLICATE_CHECK
		}

		// Mempool transactions stick around for 2 blocks. The first block
		// incorporates the transaction and mines it. The second block
		// validates it by the stake. However, transactions move into our
		// database as soon as they are mined and thus we need to be careful
		// to not include those transactions in our list.
		for _, b := range addrData.Transactions {
			if f.TxSpending.String() == b.TxID && f.InputIndex == int(b.InOutID) {
				continue SPENDING_TX_DUPLICATE_CHECK
			}
		}
		spendingTx, ok := addressUTXOs.TxnsStore[f.TxSpending]
		if !ok {
			log.Errorf("A previous outpoint's spending transaction is not available in TxnStore.")
			continue
		}
		if spendingTx.Confirmed() {
			log.Errorf("An outpoint's transaction is unexpectedly confirmed.")
			continue
		}

		// The total send amount must be looked up from the previous
		// outpoint because vin:i valuein is not reliable from dcrd.
		prevhash := spendingTx.Tx.TxIn[f.InputIndex].PreviousOutPoint.Hash
		strprevhash := prevhash.String()
		previndex := spendingTx.Tx.TxIn[f.InputIndex].PreviousOutPoint.Index
		valuein := addressUTXOs.TxnsStore[prevhash].Tx.TxOut[previndex].Value

		// Look through old transactions and set the spending transactions'
		// matching transaction fields.
		for _, dbTxn := range addrData.Transactions {
			if dbTxn.TxID == strprevhash && dbTxn.InOutID == previndex && dbTxn.IsFunding {
				dbTxn.MatchedTx = spendingTx.Hash().String()
				dbTxn.MatchedTxIndex = uint32(f.InputIndex)
			}
		}

		if txnType == dbtypes.AddrTxnAll || txnType == dbtypes.AddrTxnDebit {
			addrTx := &dbtypes.AddressTx{
				TxID:           spendingTx.Hash().String(),
				TxType:         txhelpers.DetermineTxTypeString(spendingTx.Tx),
				InOutID:        uint32(f.InputIndex),
				Time:           dbtypes.NewTimeDefFromUNIX(spendingTx.MemPoolTime),
				FormattedSize:  humanize.Bytes(uint64(spendingTx.Tx.SerializeSize())),
				Total:          txhelpers.TotalOutFromMsgTx(spendingTx.Tx).ToCoin(),
				SentTotal:      dcrutil.Amount(valuein).ToCoin(),
				MatchedTx:      strprevhash,
				MatchedTxIndex: previndex,
			}
			addrData.Transactions = append(addrData.Transactions, addrTx)
		}

		sent += valuein
		numSent++
	} // range addressUTXOs.PrevOuts

	// Totals from funding and spending transactions.
	addrData.Balance.NumSpent += numSent
	addrData.Balance.NumUnspent += (numReceived - numSent)
	addrData.Balance.TotalSpent += sent
	addrData.Balance.TotalUnspent += (received - sent)

	// Sort by date and calculate block height.
	addrData.PostProcess(uint32(pgb.Height()))

	return
}

// AddressData returns comprehensive, paginated information for an address.
func (pgb *ChainDB) MutilchainAddressData(address string, limitN, offsetAddrOuts int64,
	txnType dbtypes.AddrTxnViewType, chainType string) (addrData *dbtypes.AddressInfo, err error) {
	var addrHist []*dbtypes.MutilchainAddressRow
	var balance *dbtypes.AddressBalance
	if !pgb.ChainDBDisabled {
		addrHist, balance, err = pgb.MutilchainAddressHistory(address, limitN, offsetAddrOuts, txnType, chainType)
		if dbtypes.IsTimeoutErr(err) {
			return nil, err
		}
	}
	populateTemplate := func() {
		addrData.Offset = offsetAddrOuts
		addrData.Limit = limitN
		addrData.TxnType = txnType.String()
		addrData.Address = address
	}
	useAPI := false
	if err != nil || (err == nil && len(addrHist) == 0) {
		// We do not have any confirmed transactions. Prep to display ONLY
		// unconfirmed transactions (or none at all).
		addrData = new(dbtypes.AddressInfo)
		populateTemplate()
		//set client for api
		externalapi.BTCClient = pgb.BtcClient
		externalapi.LTCClient = pgb.LtcClient
		apiAddrInfo, err := externalapi.GetAPIMutilchainAddressDetails(pgb.OkLinkAPIKey, address, chainType, limitN, offsetAddrOuts, pgb.MutilchainHeight(chainType), txnType)
		useAPI = true
		if err != nil || apiAddrInfo == nil {
			addrData.Balance = &dbtypes.AddressBalance{
				NumSpent:     0,
				NumUnspent:   0,
				TotalSpent:   0,
				TotalUnspent: 0,
			}
			log.Tracef("AddressHistory: No confirmed transactions for address %s.", address)
		} else {
			balance = &dbtypes.AddressBalance{
				Address:       address,
				NumSpent:      apiAddrInfo.NumSpendingTxns,
				NumUnspent:    apiAddrInfo.NumFundingTxns - apiAddrInfo.NumSpendingTxns,
				TotalSpent:    apiAddrInfo.Sent,
				TotalReceived: apiAddrInfo.Received,
				TotalUnspent:  apiAddrInfo.Unspent,
			}
			addrData.Address = address
			addrData.Transactions = apiAddrInfo.Transactions
			addrData.Received = apiAddrInfo.Received
			addrData.Sent = apiAddrInfo.Sent
			addrData.Unspent = apiAddrInfo.Unspent
			addrData.NumTransactions = apiAddrInfo.NumTransactions
			addrData.TxnCount = addrData.NumTransactions
			//update balance cache
			// hash, height := pgb.GetMutilchainHashHeight(chainType)
			// blockID := cache.NewMutilchainBlockID(hash, height)
			// pgb.AddressCache.StoreMutilchainBalance(address, balance, blockID, chainType)
		}
	} else /*err == nil*/ {
		// Generate AddressInfo skeleton from the address table rows.
		addrData = dbtypes.ReduceMutilchainAddressHistory(addrHist, chainType)
		if addrData == nil {
			addrData = new(dbtypes.AddressInfo)
		}

		// Balances and txn counts
		populateTemplate()
		addrData.KnownTransactions = (balance.NumSpent * 2) + balance.NumUnspent
		addrData.KnownFundingTxns = balance.NumSpent + balance.NumUnspent
		addrData.KnownSpendingTxns = balance.NumSpent

		// For non-merged views, use the balance data.
		switch txnType {
		case dbtypes.AddrTxnAll:
			addrData.TxnCount = addrData.KnownFundingTxns + addrData.KnownSpendingTxns
		case dbtypes.AddrTxnCredit:
			addrData.TxnCount = addrData.KnownFundingTxns
			addrData.Transactions = addrData.TxnsFunding
		case dbtypes.AddrTxnDebit:
			addrData.TxnCount = addrData.KnownSpendingTxns
			addrData.Transactions = addrData.TxnsSpending
		case dbtypes.AddrUnspentTxn:
			addrData.TxnCount = addrData.NumFundingTxns - addrData.NumSpendingTxns
		}
	}

	addrData.Balance = balance

	// Transactions on current page
	addrData.NumTransactions = int64(len(addrData.Transactions))
	if addrData.NumTransactions > limitN {
		addrData.NumTransactions = limitN
	}

	if !useAPI {
		// Query database for transaction details.
		err = pgb.FillMutilchainAddressTransactions(addrData, chainType)
		if dbtypes.IsTimeoutErr(err) {
			return nil, err
		}
		if err != nil {
			return nil, fmt.Errorf("Unable to fill address %s transactions: %w", address, err)
		}
	}

	// // Check for unconfirmed transactions.
	// addressUTXOs, numUnconfirmed, err := pgb.mp.UnconfirmedTxnsForAddress(address)
	// if err != nil || addressUTXOs == nil {
	// 	return nil, fmt.Errorf("UnconfirmedTxnsForAddress failed for address %s: %w", address, err)
	// }
	// addrData.NumUnconfirmed = numUnconfirmed
	// addrData.NumTransactions += numUnconfirmed
	// if addrData.UnconfirmedTxns == nil {
	// 	addrData.UnconfirmedTxns = new(dbtypes.AddressTransactions)
	// }

	// Funding transactions (unconfirmed)
	// 	var received, sent, numReceived, numSent int64
	// FUNDING_TX_DUPLICATE_CHECK:
	// 	for _, f := range addressUTXOs.Outpoints {
	// 		// TODO: handle merged transactions
	// 		if merged {
	// 			break FUNDING_TX_DUPLICATE_CHECK
	// 		}

	// 		// Mempool transactions stick around for 2 blocks. The first block
	// 		// incorporates the transaction and mines it. The second block
	// 		// validates it by the stake. However, transactions move into our
	// 		// database as soon as they are mined and thus we need to be careful
	// 		// to not include those transactions in our list.
	// 		for _, b := range addrData.Transactions {
	// 			if f.Hash.String() == b.TxID && f.Index == b.InOutID {
	// 				continue FUNDING_TX_DUPLICATE_CHECK
	// 			}
	// 		}
	// 		fundingTx, ok := addressUTXOs.TxnsStore[f.Hash]
	// 		if !ok {
	// 			log.Errorf("An outpoint's transaction is not available in TxnStore.")
	// 			continue
	// 		}
	// 		if fundingTx.Confirmed() {
	// 			log.Errorf("An outpoint's transaction is unexpectedly confirmed.")
	// 			continue
	// 		}
	// 		if txnType == dbtypes.AddrTxnAll || txnType == dbtypes.AddrTxnCredit || txnType == dbtypes.AddrUnspentTxn {
	// 			addrTx := &dbtypes.AddressTx{
	// 				TxID:          fundingTx.Hash().String(),
	// 				TxType:        txhelpers.DetermineTxTypeString(fundingTx.Tx),
	// 				InOutID:       f.Index,
	// 				Time:          dbtypes.NewTimeDefFromUNIX(fundingTx.MemPoolTime),
	// 				FormattedSize: humanize.Bytes(uint64(fundingTx.Tx.SerializeSize())),
	// 				Total:         txhelpers.TotalOutFromMsgTx(fundingTx.Tx).ToCoin(),
	// 				ReceivedTotal: dcrutil.Amount(fundingTx.Tx.TxOut[f.Index].Value).ToCoin(),
	// 				IsFunding:     true,
	// 			}
	// 			addrData.Transactions = append(addrData.Transactions, addrTx)
	// 		}
	// 		received += fundingTx.Tx.TxOut[f.Index].Value
	// 		numReceived++
	// 	}

	// Spending transactions (unconfirmed)
	// SPENDING_TX_DUPLICATE_CHECK:
	// 	for _, f := range addressUTXOs.PrevOuts {
	// 		// TODO: handle merged transactions
	// 		if merged {
	// 			break SPENDING_TX_DUPLICATE_CHECK
	// 		}

	// 		// Mempool transactions stick around for 2 blocks. The first block
	// 		// incorporates the transaction and mines it. The second block
	// 		// validates it by the stake. However, transactions move into our
	// 		// database as soon as they are mined and thus we need to be careful
	// 		// to not include those transactions in our list.
	// 		for _, b := range addrData.Transactions {
	// 			if f.TxSpending.String() == b.TxID && f.InputIndex == int(b.InOutID) {
	// 				continue SPENDING_TX_DUPLICATE_CHECK
	// 			}
	// 		}
	// 		spendingTx, ok := addressUTXOs.TxnsStore[f.TxSpending]
	// 		if !ok {
	// 			log.Errorf("A previous outpoint's spending transaction is not available in TxnStore.")
	// 			continue
	// 		}
	// 		if spendingTx.Confirmed() {
	// 			log.Errorf("An outpoint's transaction is unexpectedly confirmed.")
	// 			continue
	// 		}

	// 		// The total send amount must be looked up from the previous
	// 		// outpoint because vin:i valuein is not reliable from dcrd.
	// 		prevhash := spendingTx.Tx.TxIn[f.InputIndex].PreviousOutPoint.Hash
	// 		strprevhash := prevhash.String()
	// 		previndex := spendingTx.Tx.TxIn[f.InputIndex].PreviousOutPoint.Index
	// 		valuein := addressUTXOs.TxnsStore[prevhash].Tx.TxOut[previndex].Value

	// 		// Look through old transactions and set the spending transactions'
	// 		// matching transaction fields.
	// 		for _, dbTxn := range addrData.Transactions {
	// 			if dbTxn.TxID == strprevhash && dbTxn.InOutID == previndex && dbTxn.IsFunding {
	// 				dbTxn.MatchedTx = spendingTx.Hash().String()
	// 				dbTxn.MatchedTxIndex = uint32(f.InputIndex)
	// 			}
	// 		}

	// 		if txnType == dbtypes.AddrTxnAll || txnType == dbtypes.AddrTxnDebit {
	// 			addrTx := &dbtypes.AddressTx{
	// 				TxID:           spendingTx.Hash().String(),
	// 				TxType:         txhelpers.DetermineTxTypeString(spendingTx.Tx),
	// 				InOutID:        uint32(f.InputIndex),
	// 				Time:           dbtypes.NewTimeDefFromUNIX(spendingTx.MemPoolTime),
	// 				FormattedSize:  humanize.Bytes(uint64(spendingTx.Tx.SerializeSize())),
	// 				Total:          txhelpers.TotalOutFromMsgTx(spendingTx.Tx).ToCoin(),
	// 				SentTotal:      dcrutil.Amount(valuein).ToCoin(),
	// 				MatchedTx:      strprevhash,
	// 				MatchedTxIndex: previndex,
	// 			}
	// 			addrData.Transactions = append(addrData.Transactions, addrTx)
	// 		}

	// 		sent += valuein
	// 		numSent++
	// 	} // range addressUTXOs.PrevOuts

	// Totals from funding and spending transactions.
	// addrData.Balance.NumSpent += numSent
	// addrData.Balance.NumUnspent += (numReceived - numSent)
	// addrData.Balance.TotalSpent += sent
	// addrData.Balance.TotalUnspent += (received - sent)

	// Sort by date and calculate block height.
	addrData.PostProcess(uint32(pgb.Height()))

	return
}

// DbTxByHash retrieves a row of the transactions table corresponding to the
// given transaction hash. Transactions in valid and mainchain blocks are chosen
// first.
func (pgb *ChainDB) DbTxByHash(txid string) (*dbtypes.Tx, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, dbTx, err := RetrieveDbTxByHash(ctx, pgb.db, txid)
	return dbTx, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) DbMutilchainTxByHash(txid string, chainType string) (*dbtypes.Tx, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	_, dbTx, err := RetrieveMutilchainDbTxByHash(ctx, pgb.db, txid, chainType)
	return dbTx, pgb.replaceCancelError(err)
}

// FundingOutpointIndxByVinID retrieves the the transaction output index of the
// previous outpoint for a transaction input specified by row ID in the vins
// table, which stores previous outpoints for each vin.
func (pgb *ChainDB) FundingOutpointIndxByVinID(id uint64) (uint32, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	ind, err := RetrieveFundingOutpointIndxByVinID(ctx, pgb.db, id)
	return ind, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) FundingMutilchainOutpointIndxByVinID(id uint64, chainType string) (uint32, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	ind, err := RetrieveMutilchainFundingOutpointIndxByVinID(ctx, pgb.db, id, chainType)
	return ind, pgb.replaceCancelError(err)
}

// FillAddressTransactions is used to fill out the transaction details in an
// explorer.AddressInfo generated by dbtypes.ReduceAddressHistory, usually from
// the output of AddressHistory. This function also sets the number of
// unconfirmed transactions for the current best block in the database.
func (pgb *ChainDB) FillAddressTransactions(addrInfo *dbtypes.AddressInfo) error {
	if addrInfo == nil {
		return nil
	}

	var numUnconfirmed int64

	for i, txn := range addrInfo.Transactions {
		// Retrieve the most valid, most mainchain, and most recent tx with this
		// hash. This means it prefers mainchain and valid blocks first.
		dbTx, err := pgb.DbTxByHash(txn.TxID)
		if err != nil {
			return err
		}
		txn.Size = dbTx.Size
		txn.FormattedSize = humanize.Bytes(uint64(dbTx.Size))
		txn.Total = dcrutil.Amount(dbTx.Sent).ToCoin()
		txn.Time = dbTx.BlockTime
		if txn.Time.UNIX() > 0 {
			txn.Confirmations = uint64(pgb.Height() - dbTx.BlockHeight + 1)
		} else {
			numUnconfirmed++
			txn.Confirmations = 0
		}

		// Get the funding or spending transaction matching index if there is a
		// matching tx hash already present.  During the next database
		// restructuring we may want to consider including matching tx index
		// along with matching tx hash in the addresses table.
		if txn.MatchedTx != `` {
			if !txn.IsFunding {
				// Spending transaction: lookup the previous outpoint's txout
				// index by the vins table row ID.
				idx, err := pgb.FundingOutpointIndxByVinID(dbTx.VinDbIds[txn.InOutID])
				if err != nil {
					log.Warnf("Matched Transaction Lookup failed for %s:%d: id: %d:  %v",
						txn.TxID, txn.InOutID, txn.InOutID, err)
				} else {
					addrInfo.Transactions[i].MatchedTxIndex = idx
				}
			} else {
				// Funding transaction: lookup by the matching (spending) tx
				// hash and tx index.
				_, idx, _, err := pgb.SpendingTransaction(txn.TxID, txn.InOutID)
				if err != nil {
					log.Warnf("Matched Transaction Lookup failed for %s:%d: %v",
						txn.TxID, txn.InOutID, err)
				} else {
					addrInfo.Transactions[i].MatchedTxIndex = idx
				}
			}
		}
	}

	addrInfo.NumUnconfirmed = numUnconfirmed

	return nil
}

func (pgb *ChainDB) FillMutilchainAddressTransactions(addrInfo *dbtypes.AddressInfo, chainType string) error {
	if addrInfo == nil {
		return nil
	}

	var numUnconfirmed int64

	for i, txn := range addrInfo.Transactions {
		// Retrieve the most valid, most mainchain, and most recent tx with this
		// hash. This means it prefers mainchain and valid blocks first.
		dbTx, err := pgb.DbMutilchainTxByHash(txn.TxID, chainType)
		if err != nil {
			return err
		}
		txn.Size = dbTx.Size
		txn.FormattedSize = humanize.Bytes(uint64(dbTx.Size))
		txn.Total = dbtypes.GetMutilchainCoinAmount(dbTx.Sent, chainType)
		txn.Time = dbTx.BlockTime
		if txn.Time.UNIX() > 0 {
			txn.Confirmations = uint64(pgb.MutilchainHeight(chainType) - dbTx.BlockHeight + 1)
		} else {
			numUnconfirmed++
			txn.Confirmations = 0
		}
		// Get the funding or spending transaction matching index if there is a
		// matching tx hash already present.  During the next database
		// restructuring we may want to consider including matching tx index
		// along with matching tx hash in the addresses table.
		if !txn.IsFunding {
			// Spending transaction: lookup the previous outpoint's txout
			// index by the vins table row ID.
			idx, err := pgb.FundingMutilchainOutpointIndxByVinID(dbTx.VinDbIds[txn.InOutID], chainType)
			if err != nil {
				log.Warnf("Matched Transaction Lookup failed for %s:%d: id: %d:  %v",
					txn.TxID, txn.InOutID, txn.InOutID, err)
			} else {
				addrInfo.Transactions[i].MatchedTxIndex = idx
			}
		} else {
			// Funding transaction: lookup by the matching (spending) tx
			// hash and tx index.
			_, idx, err := pgb.MutilchainSpendingTransaction(txn.TxID, txn.InOutID, chainType)
			if err != nil {
				log.Warnf("Matched Transaction Lookup failed for %s:%d: %v",
					txn.TxID, txn.InOutID, err)
			} else {
				addrInfo.Transactions[i].MatchedTxIndex = idx
			}
		}
	}

	addrInfo.NumUnconfirmed = numUnconfirmed

	return nil
}

// AddressTotals queries for the following totals: amount spent, amount unspent,
// number of unspent transaction outputs and number spent.
func (pgb *ChainDB) AddressTotals(address string) (*apitypes.AddressTotals, error) {
	// Fetch address totals
	var err error
	var ab *dbtypes.AddressBalance
	if address == pgb.devAddress {
		ab, err = pgb.DevBalance()
	} else {
		ab, _, err = pgb.AddressBalance(address)
	}

	if err != nil || ab == nil {
		return nil, err
	}

	bestHash, bestHeight := pgb.BestBlockStr()

	return &apitypes.AddressTotals{
		Address:      address,
		BlockHeight:  uint64(bestHeight),
		BlockHash:    bestHash,
		NumSpent:     ab.NumSpent,
		NumUnspent:   ab.NumUnspent,
		CoinsSpent:   dcrutil.Amount(ab.TotalSpent).ToCoin(),
		CoinsUnspent: dcrutil.Amount(ab.TotalUnspent).ToCoin(),
	}, nil
}

func (pgb *ChainDB) addressInfo(addr string, count, skip int64, txnType dbtypes.AddrTxnViewType) (*dbtypes.AddressInfo, *dbtypes.AddressBalance, error) {
	address, err := stdaddr.DecodeAddress(addr, pgb.chainParams)
	if err != nil {
		log.Infof("Invalid address %s: %v", addr, err)
		return nil, nil, err
	}

	// Get rows from the addresses table for the address
	addrHist, balance, err := pgb.AddressHistory(addr, count, skip, txnType, 0, 0)
	if err != nil {
		log.Errorf("Unable to get address %s history: %v", address, err)
		return nil, nil, err
	}

	// Generate AddressInfo skeleton from the address table rows
	addrData, _, _ := dbtypes.ReduceAddressHistory(addrHist)
	if addrData == nil {
		// Empty history is not expected for credit txnType with any txns.
		if txnType != dbtypes.AddrTxnDebit && (balance.NumSpent+balance.NumUnspent) > 0 {
			return nil, nil, fmt.Errorf("empty address history (%s): n=%d&start=%d", address, count, skip)
		}
		// No mined transactions. Return Address with nil Transactions slice.
		return nil, balance, nil
	}

	// Transactions to fetch with FillAddressTransactions. This should be a
	// noop if AddressHistory/ReduceAddressHistory are working right.
	switch txnType {
	case dbtypes.AddrTxnAll, dbtypes.AddrMergedTxnDebit:
	case dbtypes.AddrTxnCredit:
		addrData.Transactions = addrData.TxnsFunding
	case dbtypes.AddrTxnDebit:
		addrData.Transactions = addrData.TxnsSpending
	default:
		// shouldn't happen because AddressHistory does this check
		return nil, nil, fmt.Errorf("unknown address transaction type: %v", txnType)
	}

	// Query database for transaction details
	err = pgb.FillAddressTransactions(addrData)
	if err != nil {
		return nil, balance, fmt.Errorf("Unable to fill address %s transactions: %w", address, err)
	}

	return addrData, balance, nil
}

// AddressTransactionDetails returns an apitypes.Address with at most the last
// count transactions of type txnType in which the address was involved,
// starting after skip transactions. This does NOT include unconfirmed
// transactions.
func (pgb *ChainDB) AddressTransactionDetails(addr string, count, skip int64,
	txnType dbtypes.AddrTxnViewType) (*apitypes.Address, error) {
	// Fetch address history for given transaction range and type
	addrData, _, err := pgb.addressInfo(addr, count, skip, txnType)
	if err != nil {
		return nil, err
	}
	// No transactions found. Not an error.
	if addrData == nil {
		return &apitypes.Address{
			Address:      addr,
			Transactions: make([]*apitypes.AddressTxShort, 0), // not nil for JSON formatting
		}, nil
	}

	// Convert each dbtypes.AddressTx to apitypes.AddressTxShort
	txs := addrData.Transactions
	txsShort := make([]*apitypes.AddressTxShort, 0, len(txs))
	for i := range txs {
		txsShort = append(txsShort, &apitypes.AddressTxShort{
			TxID:          txs[i].TxID,
			Time:          apitypes.TimeAPI{S: txs[i].Time},
			Value:         txs[i].Total,
			Confirmations: int64(txs[i].Confirmations),
			Size:          int32(txs[i].Size),
		})
	}

	// put a bow on it
	return &apitypes.Address{
		Address:      addr,
		Transactions: txsShort,
	}, nil
}

func (pgb *ChainDB) MutilchainAddressTransactionDetails(addr, chainType string, count, skip int64,
	txnType dbtypes.AddrTxnViewType) (*apitypes.Address, error) {

	apiAddrInfo, err := externalapi.GetAPIMutilchainAddressDetails(pgb.OkLinkAPIKey, addr, chainType, count, skip, pgb.MutilchainHeight(chainType), txnType)
	if err != nil {
		return &apitypes.Address{
			Address:      addr,
			Transactions: make([]*apitypes.AddressTxShort, 0), // not nil for JSON formatting
		}, nil
	}
	txs := apiAddrInfo.Transactions
	// Convert each dbtypes.AddressTx to apitypes.AddressTxShort
	txsShort := make([]*apitypes.AddressTxShort, 0, len(txs))
	for i := range txs {
		txsShort = append(txsShort, &apitypes.AddressTxShort{
			TxID:          txs[i].TxID,
			Time:          apitypes.TimeAPI{S: txs[i].Time},
			Value:         txs[i].Total,
			Confirmations: int64(txs[i].Confirmations),
			Size:          int32(txs[i].Size),
		})
	}

	// put a bow on it
	return &apitypes.Address{
		Address:      addr,
		Transactions: txsShort,
	}, nil
}

// UpdateChainState updates the blockchain's state, which includes each of the
// agenda's VotingDone and Activated heights. If the agenda passed (i.e. status
// is "lockedIn" or "activated"), Activated is set to the height at which the
// rule change will take(or took) place.
func (pgb *ChainDB) UpdateChainState(blockChainInfo *chainjson.GetBlockChainInfoResult) {
	if pgb == nil {
		return
	}
	if blockChainInfo == nil {
		log.Errorf("chainjson.GetBlockChainInfoResult data passed is empty")
		return
	}

	ruleChangeInterval := int64(pgb.chainParams.RuleChangeActivationInterval)

	chainInfo := dbtypes.BlockChainData{
		Chain:                  blockChainInfo.Chain,
		SyncHeight:             blockChainInfo.SyncHeight,
		BestHeight:             blockChainInfo.Blocks,
		BestBlockHash:          blockChainInfo.BestBlockHash,
		Difficulty:             blockChainInfo.Difficulty,
		VerificationProgress:   blockChainInfo.VerificationProgress,
		ChainWork:              blockChainInfo.ChainWork,
		IsInitialBlockDownload: blockChainInfo.InitialBlockDownload,
		MaxBlockSize:           blockChainInfo.MaxBlockSize,
	}

	chainInfo.AgendaMileStones = make(map[string]dbtypes.MileStone, len(blockChainInfo.Deployments))

	for agendaID, entry := range blockChainInfo.Deployments {
		agendaInfo := dbtypes.MileStone{
			Status:     dbtypes.AgendaStatusFromStr(entry.Status),
			StartTime:  time.Unix(int64(entry.StartTime), 0).UTC(),
			ExpireTime: time.Unix(int64(entry.ExpireTime), 0).UTC(),
		}

		// The period between Voting start height to voting end height takes
		// chainParams.RuleChangeActivationInterval blocks to change. The Period
		// between Voting Done and activation also takes
		// chainParams.RuleChangeActivationInterval blocks
		switch agendaInfo.Status {
		case dbtypes.StartedAgendaStatus:
			// The voting period start height is not necessarily the height when the
			// state changed to StartedAgendaStatus because there could have already
			// been one or more RCIs of voting that failed to reach quorum. Get the
			// right voting period start height here.
			h := blockChainInfo.Blocks
			agendaInfo.VotingStarted = h - (h-entry.Since)%ruleChangeInterval
			agendaInfo.Activated = agendaInfo.VotingStarted + 2*ruleChangeInterval // if it passes

		case dbtypes.FailedAgendaStatus:
			agendaInfo.VotingStarted = entry.Since - ruleChangeInterval

		case dbtypes.LockedInAgendaStatus:
			agendaInfo.VotingStarted = entry.Since - ruleChangeInterval
			agendaInfo.Activated = entry.Since + ruleChangeInterval

		case dbtypes.ActivatedAgendaStatus:
			agendaInfo.VotingStarted = entry.Since - 2*ruleChangeInterval
			agendaInfo.Activated = entry.Since
		}

		if agendaInfo.VotingStarted != 0 {
			agendaInfo.VotingDone = agendaInfo.VotingStarted + ruleChangeInterval - 1
		}

		chainInfo.AgendaMileStones[agendaID] = agendaInfo
	}

	pgb.deployments.mtx.Lock()
	pgb.deployments.chainInfo = &chainInfo
	pgb.deployments.mtx.Unlock()
}

func (pgb *ChainDB) UpdateBTCChainState(blockChainInfo *btcjson.GetBlockChainInfoResult) {
	if pgb == nil {
		return
	}
	if blockChainInfo == nil {
		log.Errorf("chainjson.GetBlockChainInfoResult data passed is empty")
		return
	}

	chainInfo := dbtypes.BlockChainData{
		Chain:                  blockChainInfo.Chain,
		BestHeight:             int64(blockChainInfo.Blocks),
		BestBlockHash:          blockChainInfo.BestBlockHash,
		Difficulty:             uint32(blockChainInfo.Difficulty),
		VerificationProgress:   blockChainInfo.VerificationProgress,
		ChainWork:              blockChainInfo.ChainWork,
		IsInitialBlockDownload: blockChainInfo.InitialBlockDownload,
	}

	pgb.deployments.mtx.Lock()
	pgb.deployments.btcChainInfo = &chainInfo
	pgb.deployments.mtx.Unlock()
}

func (pgb *ChainDB) UpdateLTCChainState(blockChainInfo *ltcjson.GetBlockChainInfoResult) {
	if pgb == nil {
		return
	}
	if blockChainInfo == nil {
		log.Errorf("chainjson.GetBlockChainInfoResult data passed is empty")
		return
	}

	chainInfo := dbtypes.BlockChainData{
		Chain:                  blockChainInfo.Chain,
		BestHeight:             int64(blockChainInfo.Blocks),
		BestBlockHash:          blockChainInfo.BestBlockHash,
		Difficulty:             uint32(blockChainInfo.Difficulty),
		VerificationProgress:   blockChainInfo.VerificationProgress,
		ChainWork:              blockChainInfo.ChainWork,
		IsInitialBlockDownload: blockChainInfo.InitialBlockDownload,
	}

	pgb.deployments.mtx.Lock()
	pgb.deployments.ltcChainInfo = &chainInfo
	pgb.deployments.mtx.Unlock()
}

// ChainInfo guarantees thread-safe access of the deployment data.
func (pgb *ChainDB) ChainInfo() *dbtypes.BlockChainData {
	pgb.deployments.mtx.RLock()
	defer pgb.deployments.mtx.RUnlock()
	return pgb.deployments.chainInfo
}

// Store satisfies BlockDataSaver. Blocks stored this way are considered valid
// and part of mainchain. Store should not be used for batch block processing;
// instead, use StoreBlock and specify appropriate flags.
func (pgb *ChainDB) Store(blockData *blockdata.BlockData, msgBlock *wire.MsgBlock) error {
	// This function must handle being run when pgb is nil (not constructed).
	if pgb == nil {
		return nil
	}

	// update blockchain state
	pgb.UpdateChainState(blockData.BlockchainInfo)
	log.Infof("Current DCP0010 activation height is %d.", pgb.DCP0010ActivationHeight())

	// New blocks stored this way are considered valid and part of mainchain,
	// warranting updates to existing records. When adding side chain blocks
	// manually, call StoreBlock directly with appropriate flags for isValid,
	// isMainchain, and updateExistingRecords, and nil winningTickets.
	isValid, isMainChain, updateExistingRecords := true, true, true

	// Since Store should not be used in batch block processing, address'
	// spending information is updated.
	updateAddressesSpendingInfo := true

	_, _, _, err := pgb.StoreBlock(msgBlock, isValid, isMainChain,
		updateExistingRecords, updateAddressesSpendingInfo,
		blockData.Header.ChainWork)
	if err != nil {
		log.Errorf("Store block %d failed. %v", blockData.Header.Height, err)
		return err
	}
	// Signal updates to any subscribed heightClients.
	pgb.SignalHeight(msgBlock.Header.Height)
	// sync coin age table
	err = pgb.SyncCoinAgeTableWithHeight(int64(msgBlock.Header.Height))
	if err != nil {
		log.Errorf("Sync coin_age table on height %d failed. %v", blockData.Header.Height, err)
		return err
	}
	// sync utxo_history table
	err = pgb.SyncUtxoHistoryHeight(int64(msgBlock.Header.Height))
	if err != nil {
		log.Errorf("Sync utxo_history table on height %d failed. %v", blockData.Header.Height, err)
		return err
	}
	// err = pgb.SyncCoinAgeBandsWithHeightRange(int64(msgBlock.Header.Height), int64(msgBlock.Header.Height))
	return nil
}

// Store satisfies BlockDataSaver. Blocks stored this way are considered valid
// and part of mainchain. Store should not be used for batch block processing;
// instead, use StoreBlock and specify appropriate flags.
func (pgb *ChainDB) LTCStore(blockData *blockdataltc.BlockData, msgBlock *ltcwire.MsgBlock) error {
	// This function must handle being run when pgb is nil (not constructed).
	if pgb == nil || pgb.LtcBestBlock == nil {
		return nil
	}
	// update blockchain state
	pgb.UpdateLTCChainState(blockData.BlockchainInfo)
	if !pgb.ChainDBDisabled {
		isValid := true
		// Since Store should not be used in batch block processing, address'
		// spending information is updated.
		updateAddressesSpendingInfo := true

		_, _, err := pgb.StoreLTCBlock(pgb.LtcClient, msgBlock, isValid, updateAddressesSpendingInfo)
		if err != nil {
			return err
		}
	}
	//update best ltc block
	pgb.LtcBestBlock.Hash = blockData.Header.Hash
	pgb.LtcBestBlock.Height = int64(blockData.Header.Height)
	pgb.LtcBestBlock.Time = blockData.Header.Time
	// Signal updates to any subscribed heightClients.
	pgb.SignalLTCHeight(uint32(blockData.Header.Height))
	// sync for ltc atomic swap
	pgb.SyncLTCAtomicSwapData(int64(blockData.Header.Height))
	return nil
}

// Store satisfies BlockDataSaver. Blocks stored this way are considered valid
// and part of mainchain. Store should not be used for batch block processing;
// instead, use StoreBlock and specify appropriate flags.
func (pgb *ChainDB) BTCStore(blockData *blockdatabtc.BlockData, msgBlock *btcwire.MsgBlock) error {
	// This function must handle being run when pgb is nil (not constructed).
	if pgb == nil || pgb.BtcBestBlock == nil {
		return nil
	}

	// update blockchain state
	pgb.UpdateBTCChainState(blockData.BlockchainInfo)

	// New blocks stored this way are considered valid and part of mainchain,
	// warranting updates to existing records. When adding side chain blocks
	// manually, call StoreBlock directly with appropriate flags for isValid,
	// isMainchain, and updateExistingRecords, and nil winningTickets.
	if !pgb.ChainDBDisabled {
		isValid := true

		// Since Store should not be used in batch block processing, address'
		// spending information is updated.
		updateAddressesSpendingInfo := true
		_, _, err := pgb.StoreBTCBlock(pgb.BtcClient, msgBlock, isValid, updateAddressesSpendingInfo)
		if err != nil {
			return err
		}
	} else {
		if !pgb.BTC20BlocksSyncing {
			go func() {
				err := pgb.SyncLast20BTCBlocks(blockData.Header.Height)
				if err != nil {
					log.Error(err)
				} else {
					log.Infof("Sync last 20 BTC Blocks successfully")
				}
				pgb.BTC20BlocksSyncing = false
			}()
		}
	}

	//update best ltc block
	pgb.BtcBestBlock.Hash = blockData.Header.Hash
	pgb.BtcBestBlock.Height = int64(blockData.Header.Height)
	pgb.BtcBestBlock.Time = blockData.Header.Time
	// Signal updates to any subscribed heightClients.
	pgb.SignalBTCHeight(uint32(blockData.Header.Height))
	// sync for btc atomic swap
	pgb.SyncBTCAtomicSwapData(int64(blockData.Header.Height))
	return nil
}

// PurgeBestBlocks deletes all data for the N best blocks in the DB.
func (pgb *ChainDB) PurgeBestBlocks(N int64) (*dbtypes.DeletionSummary, int64, error) {
	res, height, _, err := DeleteBlocks(pgb.ctx, N, pgb.db)
	if err != nil {
		return nil, height, pgb.replaceCancelError(err)
	}

	summary := dbtypes.DeletionSummarySlice(res).Reduce()

	// Rewind stake database to this height.
	stakeDBHeight, err := pgb.RewindStakeDB(pgb.ctx, height, true)
	if err != nil {
		return nil, height, pgb.replaceCancelError(err)
	}
	if stakeDBHeight != height {
		err = fmt.Errorf("rewind of StakeDatabase to height %d failed, "+
			"reaching height %d instead", height, stakeDBHeight)
		return nil, height, pgb.replaceCancelError(err)
	}

	return &summary, height, err
}

// RewindStakeDB attempts to disconnect blocks from the stake database to reach
// the specified height. A Context may be provided to allow cancellation of the
// rewind process. If the specified height is greater than the current stake DB
// height, RewindStakeDB will exit without error, returning the current stake DB
// height and a nil error.
func (pgb *ChainDB) RewindStakeDB(ctx context.Context, toHeight int64, quiet ...bool) (stakeDBHeight int64, err error) {
	// Target height must be non-negative. It is not possible to disconnect the
	// genesis block.
	if toHeight < 0 {
		toHeight = 0
	}

	// Periodically log progress unless quiet[0]==true
	showProgress := true
	if len(quiet) > 0 {
		showProgress = !quiet[0]
	}

	// Disconnect blocks until the stake database reaches the target height.
	stakeDBHeight = int64(pgb.stakeDB.Height())
	startHeight := stakeDBHeight
	pStep := int64(1000)
	for stakeDBHeight > toHeight {
		// Log rewind progress at regular intervals.
		if stakeDBHeight == startHeight || stakeDBHeight%pStep == 0 {
			endSegment := pStep * ((stakeDBHeight - 1) / pStep)
			if endSegment < toHeight {
				endSegment = toHeight
			}
			if showProgress {
				log.Infof("Rewinding from %d to %d", stakeDBHeight, endSegment)
			}
		}

		// Check for quit signal.
		select {
		case <-ctx.Done():
			log.Infof("Rewind cancelled at height %d.", stakeDBHeight)
			return
		default:
		}

		// Disconnect the best block.
		if err = pgb.stakeDB.DisconnectBlock(false); err != nil {
			return
		}
		stakeDBHeight = int64(pgb.stakeDB.Height())
		log.Tracef("Stake db now at height %d.", stakeDBHeight)
	}
	return
}

// TxHistoryData fetches the address history chart data for specified chart
// type and time grouping.
func (pgb *ChainDB) TxHistoryData(address string, addrChart dbtypes.HistoryChart,
	chartGroupings dbtypes.TimeBasedGrouping) (cd *dbtypes.ChartsData, err error) {
	if chartGroupings >= dbtypes.NumIntervals {
		return nil, fmt.Errorf("invalid time grouping %d", chartGroupings)
	}
	_, err = stdaddr.DecodeAddress(address, pgb.chainParams)
	if err != nil {
		return nil, err
	}

	// First check cache for this address' chart data of the given type and
	// interval.
	bestHash, height := pgb.BestBlock()
	var validBlock *cache.BlockID
	cd, validBlock = pgb.AddressCache.HistoryChart(address, addrChart, chartGroupings)
	if cd != nil && validBlock != nil /* validBlock.Hash == *bestHash */ {
		return
	}

	// Make the pointed to ChartsData eligible for garbage collection.
	// pgb.AddressCache.StoreHistoryChart sets a new ChartsData retrieved from
	// the database, so we do not want to hang on to a copy of the old data.
	//nolint:ineffassign
	cd = nil

	busy, wait, done := pgb.CacheLocks.bal.TryLock(address)
	if busy {
		// Let others get the wait channel while we wait.
		// To return stale cache data if it is available:
		// cd, _ := pgb.AddressCache.HistoryChart(...)
		// if cd != nil {
		// 	return cd, nil
		// }
		<-wait

		// Try again, starting with the cache.
		return pgb.TxHistoryData(address, addrChart, chartGroupings)
	}

	// We will run the DB query, so block others from doing the same. When query
	// and/or cache update is completed, broadcast to any waiters that the coast
	// is clear.
	defer done()

	timeInterval := chartGroupings.String()

	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	switch addrChart {
	case dbtypes.TxsType:
		cd, err = retrieveTxHistoryByType(ctx, pgb.db, address, timeInterval)

	case dbtypes.AmountFlow:
		cd, err = retrieveTxHistoryByAmountFlow(ctx, pgb.db, address, timeInterval)

	default:
		cd, err = nil, fmt.Errorf("unknown error occurred")
	}
	err = pgb.replaceCancelError(err)
	if err != nil {
		return
	}

	// Update cache.
	_ = pgb.AddressCache.StoreHistoryChart(address, addrChart, chartGroupings,
		cd, cache.NewBlockID(bestHash, height))
	return
}

// SwapsChartData fetches the atomic swap info chart data for specified chart
// type and time grouping.
func (pgb *ChainDB) SwapsChartData(swapChart dbtypes.AtomicSwapChart,
	chartGroupings dbtypes.TimeBasedGrouping) (cd *dbtypes.ChartsData, err error) {
	if chartGroupings >= dbtypes.NumIntervals {
		return nil, fmt.Errorf("invalid time grouping %d", chartGroupings)
	}
	// TODO: Handler cache for swaps chart data
	// Make the pointed to ChartsData eligible for garbage collection.
	cd = nil
	timeInterval := chartGroupings.String()
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	switch swapChart {
	case dbtypes.SwapAmount:
		cd, err = retrieveSwapsByAmount(ctx, pgb.db, timeInterval)
	case dbtypes.SwapTxCount:
		cd, err = retrieveSwapsByTxcount(ctx, pgb.db, timeInterval)
	default:
		cd, err = nil, fmt.Errorf("unknown error occurred")
	}
	err = pgb.replaceCancelError(err)
	if err != nil {
		return
	}
	// TODO: Update cache.
	return
}

func (pgb *ChainDB) BinnedTreasuryIO(chartGroupings dbtypes.TimeBasedGrouping) (*dbtypes.ChartsData, error) {
	if chartGroupings >= dbtypes.NumIntervals {
		return nil, fmt.Errorf("invalid time grouping %d", chartGroupings)
	}
	timeInterval := chartGroupings.String()
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	return binnedTreasuryIO(ctx, pgb.db, timeInterval)
}

// TicketsByPrice returns chart data for tickets grouped by price. maturityBlock
// is used to define when tickets are considered live.
func (pgb *ChainDB) TicketsByPrice(maturityBlock int64) (*dbtypes.PoolTicketsData, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	ptd, err := retrieveTicketByPrice(ctx, pgb.db, maturityBlock)
	return ptd, pgb.replaceCancelError(err)
}

// TicketsByInputCount returns chart data for tickets grouped by number of
// inputs.
func (pgb *ChainDB) TicketsByInputCount() (*dbtypes.PoolTicketsData, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	ptd, err := retrieveTicketsGroupedByType(ctx, pgb.db)
	return ptd, pgb.replaceCancelError(err)
}

// windowStats fetches the charts data from retrieveWindowStats.
// This is the Fetcher half of a pair that make up a cache.ChartUpdater. The
// Appender half is appendWindowStats.
func (pgb *ChainDB) windowStats(charts *cache.ChartData) (*sql.Rows, func(), error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	rows, err := retrieveWindowStats(ctx, pgb.db, charts)
	if err != nil {
		return nil, cancel, fmt.Errorf("windowStats: %w", pgb.replaceCancelError(err))
	}

	return rows, cancel, nil
}

// missedVotesStats fetches the charts data from retrieveMissedVotes.
// This is the Fetcher half of a pair that make up a cache.ChartUpdater. The
// Appender half is appendMissedVotes.
func (pgb *ChainDB) missedVotesStats(charts *cache.ChartData) (*sql.Rows, func(), error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	rows, err := retrieveMissedVotes(ctx, pgb.db, charts)
	if err != nil {
		return nil, cancel, fmt.Errorf("missedVotesStats: %w", pgb.replaceCancelError(err))
	}

	return rows, cancel, nil
}

// chartBlocks sets or updates a series of per-block datasets.
// This is the Fetcher half of a pair that make up a cache.ChartUpdater. The
// Appender half is appendChartBlocks.
func (pgb *ChainDB) chartBlocks(charts *cache.ChartData) (*sql.Rows, func(), error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	rows, err := retrieveChartBlocks(ctx, pgb.db, charts)
	if err != nil {
		return nil, cancel, fmt.Errorf("chartBlocks: %w", pgb.replaceCancelError(err))
	}
	return rows, cancel, nil
}

func (pgb *ChainDB) chartMutilchainBlocks(charts *cache.MutilchainChartData) (*sql.Rows, func(), error) {
	// TODO when handler sync all blockchain data to DB, uncomment
	_, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	// rows, err := retrieveMutilchainChartBlocks(ctx, pgb.db, charts, charts.ChainType)
	// if err != nil {
	// 	return nil, cancel, fmt.Errorf("chartBlocks: %w", pgb.replaceCancelError(err))
	// }
	// lastBlockHeight, blockErr := pgb.GetMutilchainHeight(charts.ChainType)
	// if blockErr == nil {
	// 	charts.LastBlockHeight = lastBlockHeight
	// }
	return nil, cancel, nil
}

// coinSupply fetches the coin supply chart data from retrieveCoinSupply.
// This is the Fetcher half of a pair that make up a cache.ChartUpdater. The
// Appender half is appendCoinSupply.
func (pgb *ChainDB) coinSupply(charts *cache.ChartData) (*sql.Rows, func(), error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	rows, err := retrieveCoinSupply(ctx, pgb.db, charts)
	if err != nil {
		return nil, cancel, fmt.Errorf("coinSupply: %w", pgb.replaceCancelError(err))
	}

	return rows, cancel, nil
}

func (pgb *ChainDB) coinAge(charts *cache.ChartData) (*sql.Rows, func(), error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	rows, err := retrieveCoinAge(ctx, pgb.db, charts)
	if err != nil {
		return nil, cancel, fmt.Errorf("coinAge: %w", pgb.replaceCancelError(err))
	}

	return rows, cancel, nil
}

func (pgb *ChainDB) coinAgeBands(charts *cache.ChartData) (*sql.Rows, func(), error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	rows, err := retrieveCoinAgeBands(ctx, pgb.db, charts)
	if err != nil {
		return nil, cancel, fmt.Errorf("coinAgeBands: %w", pgb.replaceCancelError(err))
	}
	return rows, cancel, nil
}

func (pgb *ChainDB) mutilchainCoinSupply(charts *cache.MutilchainChartData) (*sql.Rows, func(), error) {
	// TODO when handler sync all blockchain data to DB, uncomment
	_, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	// rows, err := retrieveMutilchainCoinSupply(ctx, pgb.db, charts)
	// if err != nil {
	// 	return nil, cancel, fmt.Errorf("coinSupply: %w", pgb.replaceCancelError(err))
	// }

	return nil, cancel, nil
}

// txPerDay fetches the tx-per-day chart data from retrieveTxPerDay.
func (pgb *ChainDB) txPerDay(timeArr []dbtypes.TimeDef, txCountArr []uint64) (
	[]dbtypes.TimeDef, []uint64, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()

	var err error
	timeArr, txCountArr, err = retrieveTxPerDay(ctx, pgb.db, timeArr, txCountArr)
	if err != nil {
		err = fmt.Errorf("txPerDay: %w", pgb.replaceCancelError(err))
	}

	return timeArr, txCountArr, err
}

// blockFees sets or updates a series of per-block fees.
// This is the Fetcher half of a pair that make up a cache.ChartUpdater. The
// Appender half is appendBlockFees.
func (pgb *ChainDB) blockFees(charts *cache.ChartData) (*sql.Rows, func(), error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	rows, err := retrieveBlockFees(ctx, pgb.db, charts)
	if err != nil {
		return nil, cancel, fmt.Errorf("chartBlocks: %w", pgb.replaceCancelError(err))
	}
	return rows, cancel, nil
}

func (pgb *ChainDB) mutilchainBlockFees(charts *cache.MutilchainChartData) (*sql.Rows, func(), error) {
	// TODO when handler sync all blockchain data to DB, uncomment
	_, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	// rows, err := retrieveMutilchainBlockFees(ctx, pgb.db, charts)
	// if err != nil {
	// 	return nil, cancel, fmt.Errorf("chartBlocks: %w", pgb.replaceCancelError(err))
	// }
	return nil, cancel, nil
}

// appendAnonymitySet sets or updates a series of per-block privacy
// participation. This is the Fetcher half of a pair that make up a
// cache.ChartUpdater. The Appender half is appendPrivacyParticipation.
func (pgb *ChainDB) privacyParticipation(charts *cache.ChartData) (*sql.Rows, func(), error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	rows, err := retrievePrivacyParticipation(ctx, pgb.db, charts)
	if err != nil {
		return nil, cancel, fmt.Errorf("privacyParticipation: %w", pgb.replaceCancelError(err))
	}
	return rows, cancel, nil
}

// anonymitySet ensures that all data is available to update the anonymity set
// charts, first checking if the necessary data is already cached, and querying
// the DB only if necessary. If the data is already cached, a nil *sql.Rows is
// returned with a nil error as a signal to the appender. This is the Fetcher
// half of a pair that make up a cache.ChartUpdater. The Appender half is
// appendAnonymitySet.
func (pgb *ChainDB) anonymitySet(charts *cache.ChartData) (*sql.Rows, func(), error) {
	// First check if the necessary data is available in mixSetDiffs.
	nextDataHeight := uint32(len(charts.Blocks.AnonymitySet))
	targetDataHeight := uint32(len(charts.Blocks.Height) - 1)

	pgb.mixSetDiffsMtx.Lock()
	defer pgb.mixSetDiffsMtx.Unlock()

	for h := nextDataHeight; h <= targetDataHeight; h++ {
		if _, found := pgb.mixSetDiffs[h]; !found {
			log.Debugf("Mixed set deltas not available at height %d. Querying DB...", h)
			// A DB query is necessary.
			return pgb.retrieveAnonymitySet(int32(nextDataHeight) - 1) // -1 means include genesis
		}
	}

	// appendAnonymitySet has all the data it needs in mixSetDiffs.
	return nil, func() {}, nil
}

// retrieveAnonymitySet fetches the mixed output fund/spend heights and values
// for outputs funded after bestHeight. To include all blocks including genesis
// use -1 for bestHeight.
func (pgb *ChainDB) retrieveAnonymitySet(bestHeight int32) (*sql.Rows, func(), error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	rows, err := pgb.db.QueryContext(ctx, internal.SelectMixedVouts, bestHeight)
	if err != nil {
		return nil, cancel, fmt.Errorf("chartBlocks: %w", pgb.replaceCancelError(err))
	}
	return rows, cancel, nil
}

// appendAnonymitySet appends the result from anonymitySet to the provided
// ChartData. If rows is nil, the cached mixSetDiffs are used. This is the
// Appender half of a pair that make up a cache.ChartUpdater.
func (pgb *ChainDB) appendAnonymitySet(charts *cache.ChartData, rows *sql.Rows) error {
	if rows != nil {
		// DB query in progress. Scan the Rows.
		return appendAnonymitySet(charts, rows)
	}

	// dbFallback := func(best uint32) error {
	// 	rows, cancel, err := pgb.retrieveAnonymitySet(best)
	// 	defer cancel()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	return appendAnonymitySet(charts, rows)
	// }

	// Update with pgb.mixSetDiffs up to the length of charts.Blocks.Height.
	nextDataHeight := uint32(len(charts.Blocks.AnonymitySet))
	targetDataHeight := uint32(len(charts.Blocks.Height) - 1)

	pgb.mixSetDiffsMtx.Lock()
	defer pgb.mixSetDiffsMtx.Unlock()

	nextSets := make([]uint64, 0, targetDataHeight-nextDataHeight+1)
	var lastSet int64
	if nextDataHeight > 0 {
		lastSet = int64(charts.Blocks.AnonymitySet[nextDataHeight-1])
	}
	for h := nextDataHeight; h <= targetDataHeight; h++ {
		setDiff, found := pgb.mixSetDiffs[h]
		if !found {
			// log.Errorf("mix set change for height %d not found, falling back to DB query", h)
			// return dbFallback(nextDataHeight-1)
			return fmt.Errorf("mix set delta for height %d not found", h)
		}
		delete(pgb.mixSetDiffs, h)

		lastSet += setDiff
		nextSets = append(nextSets, uint64(lastSet))
		// Only append after we are sure we have all the data, but the Fetcher
		// (anonymitySet) should have already verified that we do.
	}

	charts.Blocks.AnonymitySet = append(charts.Blocks.AnonymitySet, nextSets...)

	return nil
}

// poolStats sets or updates a series of per-height ticket pool statistics.
// This is the Fetcher half of a pair that make up a cache.ChartUpdater. The
// Appender half is appendPoolStats.
func (pgb *ChainDB) poolStats(charts *cache.ChartData) (*sql.Rows, func(), error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)

	rows, err := retrievePoolStats(ctx, pgb.db, charts)
	if err != nil {
		return nil, cancel, fmt.Errorf("chartBlocks: %w", pgb.replaceCancelError(err))
	}
	return rows, cancel, nil
}

// PowerlessTickets fetches all missed and expired tickets, sorted by revocation
// status.
func (pgb *ChainDB) PowerlessTickets() (*apitypes.PowerlessTickets, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	return retrievePowerlessTickets(ctx, pgb.db)
}

// SetVinsMainchainByBlock first retrieves for all transactions in the specified
// block the vin_db_ids and vout_db_ids arrays, along with mainchain status,
// from the transactions table, and then sets the is_mainchain flag in the vins
// table for each row of vins in the vin_db_ids array. The returns are the
// number of vins updated, the vin row IDs array, the vouts row IDs array, and
// an error value.
func (pgb *ChainDB) SetVinsMainchainByBlock(blockHash string) (int64, []dbtypes.UInt64Array, []dbtypes.UInt64Array, error) {
	// The queries in this function should not timeout or (probably) canceled,
	// so use a background context.
	ctx := context.Background()

	// Get vins DB IDs from the transactions table, for each tx in the block.
	onlyRegularTxns := false
	vinDbIDsBlk, voutDbIDsBlk, areMainchain, err :=
		RetrieveTxnsVinsVoutsByBlock(ctx, pgb.db, blockHash, onlyRegularTxns)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("unable to retrieve vin data for block %s: %w", blockHash, err)
	}

	// Set the is_mainchain flag for each vin.
	vinsUpdated, err := pgb.setVinsMainchainForMany(vinDbIDsBlk, areMainchain)
	return vinsUpdated, vinDbIDsBlk, voutDbIDsBlk, err
}

func (pgb *ChainDB) setVinsMainchainForMany(vinDbIDsBlk []dbtypes.UInt64Array, areMainchain []bool) (int64, error) {
	var rowsUpdated int64
	// each transaction
	for it, vs := range vinDbIDsBlk {
		// each vin
		numUpd, err := pgb.setVinsMainchainOneTxn(vs, areMainchain[it])
		if err != nil {
			return rowsUpdated, err
		}
		rowsUpdated += numUpd
	}
	return rowsUpdated, nil
}

func (pgb *ChainDB) setVinsMainchainOneTxn(vinDbIDs dbtypes.UInt64Array,
	isMainchain bool) (int64, error) {
	var rowsUpdated int64

	// each vin
	for _, vinDbID := range vinDbIDs {
		result, err := pgb.db.Exec(internal.SetIsMainchainByVinID,
			vinDbID, isMainchain)
		if err != nil {
			return rowsUpdated, fmt.Errorf("db ID %d not found: %w", vinDbID, err)
		}

		c, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}

		rowsUpdated += c
	}

	return rowsUpdated, nil
}

// PkScriptByVinID retrieves the pkScript and script version for the row of the
// vouts table corresponding to the previous output of the vin specified by row
// ID of the vins table.
func (pgb *ChainDB) PkScriptByVinID(id uint64) (pkScript []byte, ver uint16, err error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	pks, ver, err := RetrievePkScriptByVinID(ctx, pgb.db, id)
	return pks, ver, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) MutilchainPkScriptByVinID(id uint64, chainType string) (pkScript []byte, ver uint16, err error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	pks, ver, err := RetrieveMutilchainPkScriptByVinID(ctx, pgb.db, id, chainType)
	return pks, ver, pgb.replaceCancelError(err)
}

// PkScriptByVoutID retrieves the pkScript and script version for the row of the
// vouts table specified by the row ID id.
func (pgb *ChainDB) PkScriptByVoutID(id uint64) (pkScript []byte, ver uint16, err error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	pks, ver, err := RetrievePkScriptByVoutID(ctx, pgb.db, id)
	return pks, ver, pgb.replaceCancelError(err)
}

// VinsForTx returns a slice of dbtypes.VinTxProperty values for each vin
// referenced by the transaction dbTx, along with the pkScript and script
// version for the corresponding previous outpoints.
func (pgb *ChainDB) VinsForTx(dbTx *dbtypes.Tx) ([]dbtypes.VinTxProperty, []string, []uint16, error) {
	// Retrieve the pkScript and script version for the previous outpoint of
	// each vin.
	prevPkScripts := make([]string, 0, len(dbTx.VinDbIds))
	versions := make([]uint16, 0, len(dbTx.VinDbIds))
	for _, id := range dbTx.VinDbIds {
		pkScript, ver, err := pgb.PkScriptByVinID(id)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("PkScriptByVinID: %w", err)
		}
		prevPkScripts = append(prevPkScripts, hex.EncodeToString(pkScript))
		versions = append(versions, ver)
	}

	// Retrieve the vins row data.
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	vins, err := RetrieveVinsByIDs(ctx, pgb.db, dbTx.VinDbIds)
	if err != nil {
		err = fmt.Errorf("RetrieveVinsByIDs: %w", err)
	}
	return vins, prevPkScripts, versions, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) MutilchainVinsForTx(dbTx *dbtypes.Tx, chainType string) ([]dbtypes.VinTxProperty, []string, []uint16, error) {
	// Retrieve the pkScript and script version for the previous outpoint of
	// each vin.
	prevPkScripts := make([]string, 0, len(dbTx.VinDbIds))
	versions := make([]uint16, 0, len(dbTx.VinDbIds))
	for _, id := range dbTx.VinDbIds {
		pkScript, ver, err := pgb.MutilchainPkScriptByVinID(id, chainType)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("PkScriptByVinID: %w", err)
		}
		prevPkScripts = append(prevPkScripts, hex.EncodeToString(pkScript))
		versions = append(versions, ver)
	}

	// Retrieve the vins row data.
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	vins, err := RetrieveMutilchainVinsByIDs(ctx, pgb.db, dbTx.VinDbIds, chainType)
	if err != nil {
		err = fmt.Errorf("RetrieveVinsByIDs: %w", err)
	}
	return vins, prevPkScripts, versions, pgb.replaceCancelError(err)
}

// VoutsForTx returns a slice of dbtypes.Vout values for each vout referenced by
// the transaction dbTx.
func (pgb *ChainDB) VoutsForTx(dbTx *dbtypes.Tx) ([]dbtypes.Vout, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	vouts, err := RetrieveVoutsByIDs(ctx, pgb.db, dbTx.VoutDbIds)
	return vouts, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) MutilchainVoutsForTx(dbTx *dbtypes.Tx, chainType string) ([]dbtypes.Vout, error) {
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	vouts, err := RetrieveMutilchainVoutsByIDs(ctx, pgb.db, dbTx.VoutDbIds, chainType)
	return vouts, pgb.replaceCancelError(err)
}

func (pgb *ChainDB) TipToSideChain(mainRoot string) (tipHash string, blocksMoved int64) {
	tipHash = pgb.BestBlockHashStr()
	addresses := make(map[string]struct{})
	var txnsUpdated, vinsUpdated, votesUpdated, ticketsUpdated, treasuryTxnsUpdates, addrsUpdated int64
	for tipHash != mainRoot {
		// 1. Block. Set is_mainchain=false on the tip block, return hash of
		// previous block.
		now := time.Now()
		previousHash, err := SetMainchainByBlockHash(pgb.db, tipHash, false)
		if err != nil {
			log.Errorf("Failed to set block %s as a sidechain block: %v",
				tipHash, err)
		}
		blocksMoved++
		log.Debugf("SetMainchainByBlockHash: %v", time.Since(now))

		// 2. Transactions. Set is_mainchain=false on all transactions in the
		// tip block, returning only the number of transactions updated.
		now = time.Now()
		rowsUpdated, _, err := UpdateTransactionsMainchain(pgb.db, tipHash, false)
		if err != nil {
			log.Errorf("Failed to set transactions in block %s as sidechain: %v",
				tipHash, err)
		}
		txnsUpdated += rowsUpdated
		log.Debugf("UpdateTransactionsMainchain: %v", time.Since(now))

		// 3. Vouts. For all transactions in this block, locate any vouts that
		// reference them in vouts.spend_tx_row_id, and unset spend_tx_row_id.
		voutsUnset, err := clearVoutAllSpendTxRowIDs(pgb.db, tipHash)
		if err != nil {
			log.Errorf("clearVoutAllSpendTxRowIDs for block %s: %v", tipHash, err)
		}
		log.Debugf("Unset spend_tx_row_id for %d vouts previously spent by "+
			"transactions in orphaned block %s", voutsUnset, tipHash)

		// 4. Vins. Set is_mainchain=false on all vins, returning the number of
		// vins updated, the vins table row IDs, and the vouts table row IDs.
		now = time.Now()
		rowsUpdated, vinDbIDsBlk, voutDbIDsBlk, err := pgb.SetVinsMainchainByBlock(tipHash) // isMainchain from transactions table
		if err != nil {
			log.Errorf("Failed to set vins in block %s as sidechain: %v",
				tipHash, err)
		}
		vinsUpdated += rowsUpdated
		log.Debugf("SetVinsMainchainByBlock: %v", time.Since(now))

		// 5. Addresses. Set valid_mainchain=false on all addresses rows
		// corresponding to the spending transactions specified by the vins DB
		// row IDs, and the funding transactions specified by the vouts DB row
		// IDs. The IDs come for free via RetrieveTxnsVinsVoutsByBlock.
		now = time.Now()
		addrs, numAddrSpending, numAddrFunding, err := UpdateAddressesMainchainByIDs(pgb.db,
			vinDbIDsBlk, voutDbIDsBlk, false)
		if err != nil {
			log.Errorf("Failed to set addresses rows in block %s as sidechain: %v",
				tipHash, err)
		}
		addrsUpdated += numAddrSpending + numAddrFunding
		log.Debugf("UpdateAddressesMainchainByIDs: %v", time.Since(now))
		for _, addr := range addrs {
			addresses[addr] = struct{}{}
		}

		// 6. Votes. Sets is_mainchain=false on all votes in the tip block.
		now = time.Now()
		rowsUpdated, err = UpdateVotesMainchain(pgb.db, tipHash, false)
		if err != nil {
			log.Errorf("Failed to set votes in block %s as sidechain: %v",
				tipHash, err)
		}
		votesUpdated += rowsUpdated
		log.Debugf("UpdateVotesMainchain: %v", time.Since(now))

		// 7. Tickets. Sets is_mainchain=false on all tickets in the tip block.
		now = time.Now()
		rowsUpdated, err = UpdateTicketsMainchain(pgb.db, tipHash, false)
		if err != nil {
			log.Errorf("Failed to set tickets in block %s as sidechain: %v",
				tipHash, err)
		}
		ticketsUpdated += rowsUpdated
		log.Debugf("UpdateTicketsMainchain: %v", time.Since(now))

		// 8. Treasury. Sets is_mainchain=false on all entries in the tip block.
		now = time.Now()
		rowsUpdated, err = UpdateTreasuryMainchain(pgb.db, tipHash, false)
		if err != nil {
			log.Errorf("Failed to set tickets in block %s as sidechain: %v",
				tipHash, err)
		}
		treasuryTxnsUpdates += rowsUpdated
		log.Debugf("UpdateTreasuryMainchain: %v", time.Since(now))

		// move on to next block
		tipHash = previousHash

		pgb.bestBlock.mtx.Lock()
		pgb.bestBlock.height, err = pgb.BlockHeight(tipHash)
		if err != nil {
			log.Errorf("Failed to retrieve block height for %s", tipHash)
		}
		pgb.bestBlock.hash = tipHash
		pgb.bestBlock.mtx.Unlock()
	}

	if len(addresses) > 0 {
		addrs := make([]string, 0, len(addresses))
		for addr := range addresses {
			addrs = append(addrs, addr)
		}
		numCleared := pgb.AddressCache.Clear(addrs)
		log.Debugf("Cleared cache of %d of %d addresses in orphaned transactions.", numCleared, len(addrs))
	}

	log.Debugf("Reorg orphaned: %d blocks, %d txns, %d vins, %d addresses, %d votes, %d tickets, %d treasury txns",
		blocksMoved, txnsUpdated, vinsUpdated, addrsUpdated, votesUpdated, ticketsUpdated, treasuryTxnsUpdates)

	return
}

// StoreBlock processes the input wire.MsgBlock, and saves to the data tables.
// The number of vins and vouts stored are returned.
func (pgb *ChainDB) StoreBlock(msgBlock *wire.MsgBlock, isValid, isMainchain,
	updateExistingRecords, updateAddressesSpendingInfo bool,
	chainWork string) (numVins int64, numVouts int64, numAddresses int64, err error) {

	blockHash := msgBlock.BlockHash()

	// winningTickets is only set during initial chain sync.
	// Retrieve it from the stakeDB.
	var tpi *apitypes.TicketPoolInfo
	var winningTickets []string
	if isMainchain {
		var found bool
		tpi, found = pgb.stakeDB.PoolInfo(blockHash)
		if !found {
			err = fmt.Errorf("TicketPoolInfo not found for block %s", blockHash.String())
			return
		}
		if tpi.Height != msgBlock.Header.Height {
			err = fmt.Errorf("TicketPoolInfo height mismatch. expected %d. found %d", msgBlock.Header.Height, tpi.Height)
			return
		}
		winningTickets = tpi.Winners
	}

	// Convert the wire.MsgBlock to a dbtypes.Block.
	dbBlock := dbtypes.MsgBlockToDBBlock(msgBlock, pgb.chainParams, chainWork, winningTickets)

	// Get the previous winners (stake DB pool info cache has this info). If the
	// previous block is side chain, stakedb will not have the
	// winners/validators. Since Validators are only used to identify misses in
	// InsertVotes, we will leave the Validators empty and assume there are no
	// misses. If this block becomes main chain at some point via a
	// reorganization, its table entries will be updated appropriately, which
	// will include inserting any misses since the stakeDB will then include the
	// block, thus allowing the winning tickets to be known at that time.
	// TODO: Somehow verify reorg operates as described when switching manually
	// imported side chain blocks over to main chain.
	prevBlockHash := msgBlock.Header.PrevBlock

	var winners []string
	if isMainchain && !bytes.Equal(zeroHash[:], prevBlockHash[:]) {
		lastTpi, found := pgb.stakeDB.PoolInfo(prevBlockHash)
		if !found {
			err = fmt.Errorf("stakedb.PoolInfo failed for block %s", blockHash)
			return
		}
		winners = lastTpi.Winners
	}

	// Wrap the message block with newly winning tickets and the tickets
	// expected to vote in this block (on the previous block).
	MsgBlockPG := &MsgBlockPG{
		MsgBlock:       msgBlock,
		WinningTickets: winningTickets,
		Validators:     winners,
	}

	// Extract transactions and their vouts, and insert vouts into their pg table,
	// returning their DB PKs, which are stored in the corresponding transaction
	// data struct. Insert each transaction once they are updated with their
	// vouts' IDs, returning the transaction PK ID, which are stored in the
	// containing block data struct.

	// regular transactions
	resChanReg := make(chan storeTxnsResult)
	go func() {
		resChanReg <- pgb.storeBlockTxnTree(MsgBlockPG, wire.TxTreeRegular,
			pgb.chainParams, isValid, isMainchain, updateExistingRecords,
			updateAddressesSpendingInfo)
	}()

	// stake transactions
	resChanStake := make(chan storeTxnsResult)
	go func() {
		resChanStake <- pgb.storeBlockTxnTree(MsgBlockPG, wire.TxTreeStake,
			pgb.chainParams, isValid, isMainchain, updateExistingRecords,
			updateAddressesSpendingInfo)
	}()

	if dbBlock.Height%5000 == 0 {
		log.Debugf("UTXO cache size: %d", pgb.utxoCache.Size())
	}

	resReg := <-resChanReg
	resStk := <-resChanStake
	if resStk.err != nil {
		if resReg.err == nil {
			err = resStk.err
			numVins = resReg.numVins
			numVouts = resReg.numVouts
			numAddresses = resReg.numAddresses
			return
		}
		err = errors.New(resReg.Error() + ", " + resStk.Error())
		return
	} else if resReg.err != nil {
		err = resReg.err
		numVins = resStk.numVins
		numVouts = resStk.numVouts
		numAddresses = resStk.numAddresses
		return
	}

	numVins = resStk.numVins + resReg.numVins
	numVouts = resStk.numVouts + resReg.numVouts
	numAddresses = resStk.numAddresses + resReg.numAddresses
	dbBlock.TxDbIDs = resReg.txDbIDs
	dbBlock.STxDbIDs = resStk.txDbIDs

	if isMainchain {
		pgb.mixSetDiffsMtx.Lock()
		pgb.mixSetDiffs[msgBlock.Header.Height] = resReg.mixSetDelta + resStk.mixSetDelta
		pgb.mixSetDiffsMtx.Unlock()
	}

	// Merge the affected addresses, which are to be purged from the cache.
	affectedAddresses := resReg.addresses
	for ad := range resStk.addresses {
		affectedAddresses[ad] = struct{}{}
	}
	if txhelpers.IsTreasuryActive(pgb.chainParams.Net, int64(dbBlock.Height)) {
		if _, devChange := affectedAddresses[pgb.devAddress]; devChange {
			log.Infof("Transaction affecting legacy treasury detected.")
		}
	}
	// Put them in a slice.
	addresses := make([]string, 0, len(affectedAddresses))
	for ad := range affectedAddresses {
		addresses = append(addresses, ad)
	}

	// Store the block now that it has all if its transaction row IDs.
	var blockDbID uint64
	blockDbID, err = InsertBlock(pgb.db, dbBlock, isValid, isMainchain, pgb.dupChecks)
	if err != nil {
		log.Error("InsertBlock:", err)
		return
	}
	pgb.lastBlock[blockHash] = blockDbID

	// Insert the block in the block_chain table with the previous block hash
	// and an empty string for the next block hash, which may be updated when a
	// new block extends this chain.
	err = InsertBlockPrevNext(pgb.db, blockDbID, dbBlock.Hash,
		dbBlock.PreviousHash, "")
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		log.Error("InsertBlockPrevNext:", err)
		return
	}

	// Update the previous block's next block hash in the block_chain table with
	// this block's hash as it is next. If the current block's votes
	// invalidated/disapproved the previous block, also update the is_valid
	// columns for the previous block's entries in the following tables: blocks,
	// vins, addresses, and transactions.
	err = pgb.UpdateLastBlock(msgBlock, isMainchain)
	if err != nil && !errors.Is(err, sql.ErrNoRows) && !errors.Is(err, dbtypes.ErrNoResult) {
		err = fmt.Errorf("UpdateLastBlock: %w", err)
		return
	}

	if isMainchain {
		// Update best block height and hash.
		pgb.bestBlock.mtx.Lock()
		pgb.bestBlock.height = int64(dbBlock.Height)
		pgb.bestBlock.hash = dbBlock.Hash
		pgb.bestBlock.mtx.Unlock()

		// Insert the block stats.
		if tpi != nil {
			err = InsertBlockStats(pgb.db, blockDbID, tpi)
			if err != nil {
				err = fmt.Errorf("InsertBlockStats: %w", err)
				return
			}
		}

		// Update the best block in the meta table.
		err = SetDBBestBlock(pgb.db, dbBlock.Hash, int64(dbBlock.Height))
		if err != nil {
			err = fmt.Errorf("SetDBBestBlock: %w", err)
			return
		}
	}

	// If not in batch sync, lazy update the dev fund balance, and expire cache
	// data for the affected addresses.
	if !pgb.InBatchSync {
		if err = pgb.FreshenAddressCaches(true, addresses); err != nil {
			log.Warnf("FreshenAddressCaches: %v", err)
		}
	}

	return
}

// SetDBBestBlock stores ChainDB's BestBlock data in the meta table. UNUSED
func (pgb *ChainDB) SetDBBestBlock() error {
	pgb.bestBlock.mtx.RLock()
	bbHash, bbHeight := pgb.bestBlock.hash, pgb.bestBlock.height
	pgb.bestBlock.mtx.RUnlock()
	return SetDBBestBlock(pgb.db, bbHash, bbHeight)
}

// UpdateLastBlock set the previous block's next block hash in the block_chain
// table with this block's hash as it is next. If the current block's votes
// invalidated/disapproved the previous block, it also updates the is_valid
// columns for the previous block's entries in the following tables: blocks,
// vins, addresses, and transactions. If the previous block is not on the same
// chain as this block (as indicated by isMainchain), no updates are performed.
func (pgb *ChainDB) UpdateLastBlock(msgBlock *wire.MsgBlock, isMainchain bool) error {
	// Only update if last was not genesis, which is not in the table (implied).
	lastBlockHash := msgBlock.Header.PrevBlock
	if lastBlockHash == zeroHash {
		return nil
	}

	// Ensure previous block has the same main/sidechain status. If the
	// current block being added is side chain, do not invalidate the
	// mainchain block or any of its components, or update the block_chain
	// table to point to this block.
	if !isMainchain { // only check when current block is side chain
		_, lastIsMainchain, err := pgb.BlockFlagsNoCancel(lastBlockHash.String())
		if err != nil {
			log.Errorf("Unable to determine status of previous block %v: %v",
				lastBlockHash, err)
			return nil // do not return an error, but this should not happen
		}
		// Do not update previous block data if it is not the same blockchain
		// branch. i.e. A side chain block does not invalidate a main chain block.
		if lastIsMainchain != isMainchain {
			log.Debugf("Previous block %v is on the main chain, while current "+
				"block %v is on a side chain. Not updating main chain parent.",
				lastBlockHash, msgBlock.BlockHash())
			return nil
		}
	}

	// Attempt to find the row id of the block hash in cache.
	lastBlockDbID, ok := pgb.lastBlock[lastBlockHash]
	if !ok {
		log.Debugf("The previous block %s for block %s not found in cache, "+
			"looking it up.", lastBlockHash, msgBlock.BlockHash())
		var err error
		lastBlockDbID, err = pgb.BlockChainDbIDNoCancel(lastBlockHash.String())
		if err != nil {
			return fmt.Errorf("unable to locate block %s in block_chain table: %w",
				lastBlockHash, err)
		}
	}

	// Update the previous block's next block hash in the block_chain table.
	err := UpdateBlockNext(pgb.db, lastBlockDbID, msgBlock.BlockHash().String())
	if err != nil {
		return fmt.Errorf("UpdateBlockNext: %w", err)
	}

	// If the previous block is invalidated by this one: (1) update it's
	// is_valid flag in the blocks table if needed, and (2) flag all the vins,
	// transactions, and addresses table rows from the previous block's
	// transactions as invalid. Do nothing otherwise since blocks' transactions
	// are initially added as valid.
	lastIsValid := msgBlock.Header.VoteBits&1 != 0
	if !lastIsValid {
		// Update the is_valid flag in the blocks table.
		log.Infof("Previous block %s was DISAPPROVED by stakeholders.", lastBlockHash)
		err := UpdateLastBlockValid(pgb.db, lastBlockDbID, lastIsValid)
		if err != nil {
			return fmt.Errorf("UpdateLastBlockValid: %w", err)
		}

		// For the transactions invalidated by this block, locate any vouts that
		// reference them in vouts.spend_tx_row_id, and unset spend_tx_row_id.
		voutsUnset, err := clearVoutRegularSpendTxRowIDs(pgb.db, lastBlockHash.String())
		if err != nil {
			return fmt.Errorf("clearVoutRegularSpendTxRowIDs: %w", err)
		}
		log.Debugf("Unset spend_tx_row_id for %d vouts previously spent by "+
			"regular transactions in invalidated block %s", voutsUnset, lastBlockHash)

		// Update the is_valid flag for the last block's vins.
		err = UpdateLastVins(pgb.db, lastBlockHash.String(), lastIsValid, isMainchain)
		if err != nil {
			return fmt.Errorf("UpdateLastVins: %w", err)
		}

		// Update the is_valid flag for the last block's regular transactions.
		_, _, err = UpdateTransactionsValid(pgb.db, lastBlockHash.String(), lastIsValid)
		if err != nil {
			return fmt.Errorf("UpdateTransactionsValid: %w", err)
		}

		// Update addresses table for last block's regular transactions.
		// So slow without indexes:
		//  Update on addresses  (cost=0.00..1012201.53 rows=1 width=181)
		// 		->  Seq Scan on addresses  (cost=0.00..1012201.53 rows=1 width=181)
		// 		Filter: ((NOT is_funding) AND (tx_vin_vout_row_id = 13241234))
		addrs, err := UpdateLastAddressesValid(pgb.db, lastBlockHash.String(), lastIsValid)
		if err != nil {
			return fmt.Errorf("UpdateLastAddressesValid: %w", err)
		}
		if len(addrs) > 0 {
			numCleared := pgb.AddressCache.Clear(addrs)
			log.Debugf("Cleared cache of %d of %d addresses in disapproved transactions.", numCleared, len(addrs))
		}

		// NOTE: Updating the tickets, votes, misses, and treasury tables is not
		// necessary since the stake tree is not subject to stakeholder
		// approval.
	}

	return nil
}

// storeTxnsResult is the type of object sent back from the goroutines wrapping
// storeBlockTxnTree in StoreBlock.
type storeTxnsResult struct {
	numVins, numVouts, numAddresses, fees, totalSent int64
	txDbIDs                                          []uint64
	err                                              error
	addresses                                        map[string]struct{}
	mixSetDelta                                      int64
}

func (r *storeTxnsResult) Error() string {
	return r.err.Error()
}

// MsgBlockPG extends wire.MsgBlock with the winning tickets from the block,
// WinningTickets, and the tickets from the previous block that may vote on this
// block's validity, Validators.
type MsgBlockPG struct {
	*wire.MsgBlock
	WinningTickets []string
	Validators     []string
}

// storeTxns inserts all vins, vouts, and transactions. The VoutDbIds and
// VinDbIds fields of each Tx in the input txns slice are set upon insertion of
// vouts and vins, respectively. The Vouts fields are also set to the
// corresponding Vout slice from the vouts input argument. For each transaction,
// a []AddressRow is created while inserting the vouts. The [][]AddressRow is
// returned. The row IDs of the inserted transactions in the transactions table
// is returned in txDbIDs []uint64.
func (pgb *ChainDB) storeTxns(txns []*dbtypes.Tx, vouts [][]*dbtypes.Vout, vins []dbtypes.VinTxPropertyARRAY,
	updateExistingRecords bool) (dbAddressRows [][]dbtypes.AddressRow, txDbIDs []uint64, totalAddressRows, numOuts, numIns int, err error) {
	// vins, vouts, and transactions inserts in atomic DB transaction
	var dbTx *sql.Tx
	dbTx, err = pgb.db.Begin()
	if err != nil {
		err = fmt.Errorf("failed to begin database transaction: %w", err)
		return
	}

	checked, doUpsert := pgb.dupChecks, updateExistingRecords

	var voutStmt *sql.Stmt
	voutStmt, err = dbTx.Prepare(internal.MakeVoutInsertStatement(checked, doUpsert))
	if err != nil {
		_ = dbTx.Rollback()
		err = fmt.Errorf("failed to prepare vout insert statement: %w", err)
		return
	}
	defer voutStmt.Close()

	var vinStmt *sql.Stmt
	vinStmt, err = dbTx.Prepare(internal.MakeVinInsertStatement(checked, doUpsert))
	if err != nil {
		_ = dbTx.Rollback()
		err = fmt.Errorf("failed to prepare vin insert statement: %w", err)
		return
	}
	defer vinStmt.Close()

	// dbAddressRows contains the data added to the address table, arranged as
	// [tx_i][addr_j], transactions paying to different numbers of addresses.
	dbAddressRows = make([][]dbtypes.AddressRow, len(txns))

	for it, Tx := range txns {
		// Insert vouts, and collect AddressRows to add to address table for
		// each output.
		Tx.VoutDbIds, dbAddressRows[it], err = InsertVoutsStmt(voutStmt,
			vouts[it], pgb.dupChecks, updateExistingRecords)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			err = fmt.Errorf("failure in InsertVoutsStmt: %w", err)
			_ = dbTx.Rollback()
			return
		}
		totalAddressRows += len(dbAddressRows[it])
		numOuts += len(Tx.VoutDbIds)
		if errors.Is(err, sql.ErrNoRows) || len(vouts[it]) != len(Tx.VoutDbIds) {
			log.Warnf("Incomplete Vout insert.")
		}

		// Insert vins
		Tx.VinDbIds, err = InsertVinsStmt(vinStmt, vins[it], pgb.dupChecks,
			updateExistingRecords)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			err = fmt.Errorf("failure in InsertVinsStmt: %w", err)
			_ = dbTx.Rollback()
			return
		}
		numIns += len(Tx.VinDbIds)

		// Return the transactions vout slice.
		Tx.Vouts = vouts[it]
	}

	// Get the tx PK IDs for storage in the blocks, tickets, and votes table.
	txDbIDs, err = InsertTxnsDbTxn(dbTx, txns, pgb.dupChecks, updateExistingRecords)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		err = fmt.Errorf("failure in InsertTxnsDbTxn: %w", err)
		return
	}

	if err = dbTx.Commit(); err != nil {
		err = fmt.Errorf("failed to commit transaction: %w", err)
	}
	return
}

// storeBlockTxnTree stores the transactions of a given block.
func (pgb *ChainDB) storeBlockTxnTree(msgBlock *MsgBlockPG, txTree int8,
	chainParams *chaincfg.Params, isValid, isMainchain bool,
	updateExistingRecords, updateAddressesSpendingInfo bool) storeTxnsResult {
	// For the given block and transaction tree, extract the transactions, vins,
	// and vouts. Note that each txn in dbTransactions has IsValid set according
	// to the isValid flag for the block and the tree of the transaction itself,
	// where TxTreeStake transactions are never invalidated.
	height := int64(msgBlock.Header.Height)
	isStake := txTree == wire.TxTreeStake
	dbTransactions, dbTxVouts, dbTxVins := dbtypes.ExtractBlockTransactions(
		msgBlock.MsgBlock, txTree, chainParams, isValid, isMainchain)

	// The transactions' VinDbIds are not yet set, but update the UTXO cache
	// without it so we can check the mixed status of stake transaction inputs
	// without missing prevouts generated by txns mined in the same block.
	pgb.updateUtxoCache(dbTxVouts, dbTransactions)

	// Check the previous outputs funding the stake transactions and certain
	// regular transactions, tagging the new outputs as mixed if all of the
	// previous outputs were mixed.
txns:
	for it, tx := range dbTransactions {
		if tx.MixCount > 0 {
			continue
		}

		// This only applies to stake transactions that spend the mixed outputs.
		if txhelpers.TxIsRegular(int(tx.TxType)) {
			continue
		}

		for iv := range dbTxVins[it] {
			vin := &dbTxVins[it][iv]
			if txhelpers.IsZeroHashStr(vin.PrevTxHash) {
				continue
			}
			utxo := pgb.utxoCache.Peek(vin.PrevTxHash, vin.PrevTxIndex)
			if utxo == nil {
				log.Tracef("Uncached UTXO %s:%d. Looking it up in the DB.", vin.PrevTxHash, vin.PrevTxIndex)
				var err error
				utxo, err = retrieveTxOutData(pgb.db, vin.PrevTxHash, vin.PrevTxIndex, int8(vin.PrevTxTree))
				if utxo == nil || err != nil {
					log.Warnf("Unable to find load UTXO data for %s:%d. Error: %v",
						vin.PrevTxHash, vin.PrevTxIndex, err)
					continue txns // fallback to an RPC?
				}
				// Remember this for insertSpendingAddressRow.
				pgb.utxoCache.Set(vin.PrevTxHash, vin.PrevTxIndex,
					utxo.VoutDbID, utxo.Addresses, utxo.Value, utxo.Mixed)
			}
			if !utxo.Mixed {
				continue txns
			}
		}

		log.Tracef("Tagging outputs of txn %s (%s) as mixed.", tx.TxID,
			txhelpers.TxTypeToString(int(tx.TxType)))
		for _, vout := range dbTxVouts[it] {
			vout.Mixed = true
		}

		//pgb.updateUtxoCache([][]*dbtypes.Vout{dbTxVouts[it]}, []*dbtypes.Tx{tx})
	}

	var mixDiff int64
	if isMainchain {
		for it, tx := range dbTransactions {
			if !tx.IsValid {
				continue
			}

			for _, vout := range dbTxVouts[it] {
				if !vout.Mixed {
					continue
				}
				mixDiff += int64(vout.Value)
			}
		}
	}

	// Store the transactions, vins, and vouts. This sets the VoutDbIds,
	// VinDbIds, and Vouts fields of each Tx in the dbTransactions slice.
	dbAddressRows, txDbIDs, totalAddressRows, numOuts, numIns, err :=
		pgb.storeTxns(dbTransactions, dbTxVouts, dbTxVins, updateExistingRecords)
	if err != nil {
		return storeTxnsResult{err: err}
	}

	// The return value, containing counts of inserted vins/vouts/txns, and an
	// error value.
	txRes := storeTxnsResult{
		numVins:  int64(numIns),
		numVouts: int64(numOuts),
		txDbIDs:  txDbIDs,
	}

	// Flatten the address rows into a single slice, and update the utxoCache
	// (again, now that vin DB IDs are set in dbTransactions).
	var dbAddressRowsFlat []*dbtypes.AddressRow
	var wg sync.WaitGroup
	processAddressRows := func() {
		dbAddressRowsFlat = pgb.flattenAddressRows(dbAddressRows, dbTransactions)
		wg.Done()
	}
	updateUTXOCache := func() {
		pgb.updateUtxoCache(dbTxVouts, dbTransactions)
		wg.Done()
	}
	// Do this concurrently with stake transaction data insertion.
	wg.Add(2)
	go processAddressRows()
	go updateUTXOCache()

	// For a side chain block, set Validators to an empty slice so that there
	// will be no misses even if there are less than 5 votes. Any Validators
	// that do not match a spent ticket hash in InsertVotes are considered
	// misses. By listing no required validators, there are no misses. For side
	// chain blocks, this is acceptable and necessary because the misses table
	// does not record the block hash or main/side chain status.
	if !isMainchain {
		msgBlock.Validators = []string{}
	}

	// If processing stake transactions, insert tickets, votes, and misses. Also
	// update pool status and spending information in tickets table pertaining
	// to the new votes, revokes, misses, and expires.
	if isStake {
		// Tickets: Insert new (unspent) tickets
		newTicketDbIDs, newTicketTx, err := InsertTickets(pgb.db, dbTransactions, txDbIDs,
			pgb.dupChecks, updateExistingRecords)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			log.Error("InsertTickets:", err)
			txRes.err = err
			return txRes
		}

		// Cache the unspent ticket DB row IDs and and their hashes. Needed do
		// efficiently update their spend status later.
		for it, tdbid := range newTicketDbIDs {
			pgb.unspentTicketCache.Set(newTicketTx[it].TxID, tdbid)
		}

		// Votes: insert votes and misses (tickets that did not vote when
		// called). Return the ticket hash of all misses, which may include
		// revokes at this point. Unrevoked misses are identified when updating
		// ticket spend info below.

		// voteDbIDs, voteTxns, spentTicketHashes, ticketDbIDs, missDbIDs, err := ...
		var missesHashIDs map[string]uint64
		_, _, _, _, missesHashIDs, err = InsertVotes(pgb.db, dbTransactions, txDbIDs,
			pgb.unspentTicketCache, msgBlock, pgb.dupChecks, updateExistingRecords,
			pgb.chainParams, pgb.ChainInfo())
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			log.Error("InsertVotes:", err)
			txRes.err = err
			return txRes
		}

		// Treasury txns.
		err = InsertTreasuryTxns(pgb.db, dbTransactions, pgb.dupChecks, updateExistingRecords)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			log.Error("InsertTreasuryTxns:", err)
			txRes.err = err
			return txRes
		}

		// Get information for transactions spending tickets (votes and
		// revokes), and the ticket DB row IDs themselves. Also return tickets
		// table row IDs for newly spent tickets, if we are updating them as we
		// go (SetSpendingForTickets). CollectTicketSpendDBInfo uses ChainDB's
		// ticket DB row ID cache (unspentTicketCache), and immediately expires
		// any found entries for a main chain block.
		spendingTxDbIDs, spendTypes, spentTicketHashes, ticketDbIDs, err :=
			pgb.CollectTicketSpendDBInfo(dbTransactions, txDbIDs,
				msgBlock.MsgBlock.STransactions, isMainchain)
		if err != nil {
			log.Error("CollectTicketSpendDBInfo:", err)
			txRes.err = err
			return txRes
		}

		// Get a consistent view of the stake node at its present height.
		pgb.stakeDB.LockStakeNode()

		// Classify and record the height of each ticket spend (vote or revoke).
		// For revokes, further distinguish miss or expire.
		revokes := make(map[string]uint64)
		blockHeights := make([]int64, len(spentTicketHashes))
		poolStatuses := make([]dbtypes.TicketPoolStatus, len(spentTicketHashes))
		for iv := range spentTicketHashes {
			blockHeights[iv] = height

			// Vote or revoke
			switch spendTypes[iv] {
			case dbtypes.TicketVoted:
				poolStatuses[iv] = dbtypes.PoolStatusVoted
			case dbtypes.TicketRevoked:
				revokes[spentTicketHashes[iv]] = ticketDbIDs[iv]
				// Revoke reason
				h, err0 := chainhash.NewHashFromStr(spentTicketHashes[iv])
				if err0 != nil {
					log.Errorf("Invalid hash %v", spentTicketHashes[iv])
					continue // no info about spent ticket!
				}
				expired := pgb.stakeDB.BestNode.ExistsExpiredTicket(*h)
				if !expired {
					poolStatuses[iv] = dbtypes.PoolStatusMissed
				} else {
					poolStatuses[iv] = dbtypes.PoolStatusExpired
				}
			}
		}

		// Update tickets table with spending info.
		_, err = SetSpendingForTickets(pgb.db, ticketDbIDs, spendingTxDbIDs,
			blockHeights, spendTypes, poolStatuses)
		if err != nil {
			log.Error("SetSpendingForTickets:", err)
		}

		// Unspent not-live tickets are also either expired or missed.

		// Missed but not revoked
		var unspentMissedTicketHashes []string
		var missStatuses []dbtypes.TicketPoolStatus
		unspentMisses := make(map[string]struct{})
		// missesHashIDs refers to lottery winners that did not vote.
		for miss := range missesHashIDs {
			if _, ok := revokes[miss]; !ok {
				// unrevoked miss
				unspentMissedTicketHashes = append(unspentMissedTicketHashes, miss)
				unspentMisses[miss] = struct{}{}
				missStatuses = append(missStatuses, dbtypes.PoolStatusMissed)
			}
		}

		// Expired but not revoked
		unspentEnM := make([]string, len(unspentMissedTicketHashes))
		// Start with the unspent misses and append unspent expires to get
		// "unspent expired and missed".
		copy(unspentEnM, unspentMissedTicketHashes)
		unspentExpiresAndMisses := pgb.stakeDB.BestNode.MissedByBlock()
		for _, missHash := range unspentExpiresAndMisses {
			// MissedByBlock includes tickets that missed votes or expired (and
			// which may be revoked in this block); we just want the expires,
			// and not the revoked ones. Screen each ticket from MissedByBlock
			// for the actual unspent expires.
			if pgb.stakeDB.BestNode.ExistsExpiredTicket(missHash) {
				emHash := missHash.String()
				// Next check should not be unnecessary. Make sure not in
				// unspent misses from above, and not just revoked.
				_, justMissed := unspentMisses[emHash] // should be redundant
				_, justRevoked := revokes[emHash]      // exclude if revoked
				if !justMissed && !justRevoked {
					unspentEnM = append(unspentEnM, emHash) //nolint:makezero
					missStatuses = append(missStatuses, dbtypes.PoolStatusExpired)
				}
			}
		}

		// Release the stake node.
		pgb.stakeDB.UnlockStakeNode()

		// Locate the row IDs of the unspent expired and missed tickets. Do not
		// expire the cache entry.
		unspentEnMRowIDs := make([]uint64, len(unspentEnM))
		for iu := range unspentEnM {
			t, err0 := pgb.unspentTicketCache.TxnDbID(unspentEnM[iu], false)
			if err0 != nil {
				txRes.err = fmt.Errorf("failed to retrieve ticket %s DB ID: %w",
					unspentEnM[iu], err0)
				return txRes
			}
			unspentEnMRowIDs[iu] = t
		}

		// Update status of the unspent expired and missed tickets.
		_, err = SetPoolStatusForTickets(pgb.db,
			unspentEnMRowIDs, missStatuses)
		if err != nil {
			log.Errorf("SetPoolStatusForTicketsByHash: %v", err)
		}
	} // isStake

	treasuryActive := txhelpers.IsTreasuryActive(pgb.chainParams.Net, height)

	wg.Wait()

	// Begin a database transaction to insert spending address rows, and (if
	// updateAddressesSpendingInfo) update matching_tx_hash in corresponding
	// funding rows.
	dbTx, err := pgb.db.Begin()
	if err != nil {
		txRes.err = fmt.Errorf("unable to begin database transaction: %w", err)
		return txRes
	}

	// Insert each new funding AddressRow, absent MatchingTxHash (spending txn
	// since these new address rows are *funding*).
	_, err = InsertAddressRowsDbTx(dbTx, dbAddressRowsFlat, pgb.dupChecks, updateExistingRecords)
	if err != nil {
		_ = dbTx.Rollback()
		log.Error("InsertAddressRows:", err)
		txRes.err = err
		return txRes
	}
	txRes.numAddresses = int64(totalAddressRows)
	txRes.addresses = make(map[string]struct{})
	for _, ad := range dbAddressRowsFlat {
		if treasuryActive && ad.Address == pgb.devAddress {
			log.Debugf("Transaction paying to legacy treasury: %v", ad.TxHash)
		}
		txRes.addresses[ad.Address] = struct{}{}
	}

	for it, tx := range dbTransactions {
		// vins array for this transaction
		txVins := dbTxVins[it]
		txDbID := txDbIDs[it] // for the newly-spent TXOs in the vouts table
		voutDbIDs := make([]int64, 0, len(txVins))
		var spendLegacyTreasury bool

		for iv := range txVins {
			// Transaction that spends an outpoint paying to >=0 addresses
			vin := &txVins[iv]

			// Skip coinbase inputs (they are new coins and thus have no
			// previous outpoint funding them).
			if bytes.Equal(zeroHashStringBytes, []byte(vin.PrevTxHash)) {
				continue
			}

			// Insert spending txn data in addresses table, and updated spend
			// status for the previous outpoints' rows in the same table and in
			// the vouts table.
			vinDbID := tx.VinDbIds[iv]
			spendingTxHash := vin.TxID     // == tx.TxID
			spendingTxIndex := vin.TxIndex // == iv ?

			// Attempt to retrieve cached data for this now-spent TXO. A
			// successful get will delete the entry from the cache.
			utxoData, ok := pgb.utxoCache.Get(vin.PrevTxHash, vin.PrevTxIndex)
			if !ok {
				log.Tracef("Data for that utxo (%s:%d) wasn't cached! Vouts table will be queried.",
					vin.PrevTxHash, vin.PrevTxIndex)
			}
			fromAddrs, _, voutDbID, mixedVout, err := insertSpendingAddressRow(dbTx,
				vin.PrevTxHash, vin.PrevTxIndex, int8(vin.PrevTxTree),
				spendingTxHash, spendingTxIndex, vinDbID, utxoData, pgb.dupChecks,
				updateExistingRecords, tx.IsMainchainBlock, tx.IsValid,
				vin.TxType, updateAddressesSpendingInfo, tx.BlockTime)
			if err != nil {
				txRes.err = fmt.Errorf("insertSpendingAddressRow: %w + %v (rollback)",
					err, dbTx.Rollback())
				return txRes
			}
			txRes.numAddresses += int64(len(fromAddrs))
			for i := range fromAddrs {
				if treasuryActive && !spendLegacyTreasury && fromAddrs[i] == pgb.devAddress {
					spendLegacyTreasury = true
				}
				txRes.addresses[fromAddrs[i]] = struct{}{}
			}
			voutDbIDs = append(voutDbIDs, voutDbID)

			if mixedVout && tx.IsValid && isMainchain {
				mixDiff -= vin.ValueIn
			}
		}

		if spendLegacyTreasury {
			log.Debugf("Transaction spending from legacy treasury: %v", tx.TxID)
		}

		// NOTE: vouts.spend_tx_row_id is not updated if this is a side chain
		// block or if the transaction is stake-invalidated. Spending
		// information for extended side chain transaction outputs must still be
		// done via addresses.matching_tx_hash.
		if tx.IsValid && isMainchain && len(voutDbIDs) > 0 {
			// Set spend_tx_row_id for each prevout consumed by this txn.
			err = setSpendingForVouts(dbTx, voutDbIDs, txDbID)
			if err != nil {
				txRes.err = fmt.Errorf(`setSpendingForVouts: %w + %v (rollback)`,
					err, dbTx.Rollback())
				return txRes
			}
		}
	}

	// Scan for swap transactions. Only scan regular txn tree, and if we are
	// currently processing the regular tree.
	var txnsSwapScan []*wire.MsgTx
	if !isStake {
		txnsSwapScan = msgBlock.Transactions[1:] // skip the coinbase
	}
	for _, tx := range txnsSwapScan {
		// This will only identify the redeem and refund txns, unlike the use of
		// TxAtomicSwapsInfo in API and explorer calls.
		swapTxns, err := txhelpers.MsgTxAtomicSwapsInfo(tx, nil, pgb.chainParams)
		if err != nil {
			log.Warnf("MsgTxAtomicSwapsInfo: %v", err)
			continue
		}
		if swapTxns == nil || swapTxns.Found == "" {
			continue
		}
		for _, red := range swapTxns.Redemptions {
			err = InsertSwap(pgb.db, pgb.ctx, pgb.Client, height, red, false)
			if err != nil {
				log.Errorf("InsertSwap: %v", err)
			}
		}
		for _, ref := range swapTxns.Refunds {
			err = InsertSwap(pgb.db, pgb.ctx, pgb.Client, height, ref, true)
			if err != nil {
				log.Errorf("InsertSwap: %v", err)
			}
		}
	}

	txRes.err = dbTx.Commit()
	txRes.mixSetDelta = mixDiff

	return txRes
}

func (pgb *ChainDB) updateUtxoCache(dbVouts [][]*dbtypes.Vout, txns []*dbtypes.Tx) {
	for it, tx := range txns {
		utxos := make([]*dbtypes.UTXO, 0, tx.NumVout)
		for iv, vout := range dbVouts[it] {
			// Do not store zero-value output data.
			if vout.Value == 0 {
				continue
			}

			// Allow tx.VoutDbIds to be unset.
			voutDbID := int64(-1)
			if len(tx.VoutDbIds) == len(dbVouts[it]) {
				voutDbID = int64(tx.VoutDbIds[iv])
			}

			utxos = append(utxos, &dbtypes.UTXO{
				TxHash:  vout.TxHash,
				TxIndex: vout.TxIndex,
				UTXOData: dbtypes.UTXOData{
					Addresses: vout.ScriptPubKeyData.Addresses,
					Value:     int64(vout.Value),
					Mixed:     vout.Mixed,
					VoutDbID:  voutDbID,
				},
			})
		}

		// Store each output of this transaction in the UTXO cache.
		for _, utxo := range utxos {
			pgb.utxoCache.Set(utxo.TxHash, utxo.TxIndex, utxo.VoutDbID, utxo.Addresses, utxo.Value, utxo.Mixed)
		}
	}
}

func (pgb *ChainDB) flattenAddressRows(dbAddressRows [][]dbtypes.AddressRow, txns []*dbtypes.Tx) []*dbtypes.AddressRow {
	var totalAddressRows int
	for it := range dbAddressRows {
		for ia := range dbAddressRows[it] {
			if dbAddressRows[it][ia].Value > 0 {
				totalAddressRows++
			}
		}
	}

	dbAddressRowsFlat := make([]*dbtypes.AddressRow, 0, totalAddressRows)

	for it, tx := range txns {
		// Store txn block time and mainchain validity status in AddressRows, and
		// set IsFunding to true since InsertVouts is supplying the AddressRows.
		validMainChainTx := tx.IsMainchainBlock && (tx.IsValid || tx.Tree == wire.TxTreeStake)

		// A UTXO may have multiple addresses associated with it, so check each
		// addresses table row for multiple entries for the same output of this
		// txn. This can only happen if the output's pkScript is a P2PK or P2PKH
		// multisignature script. This does not refer to P2SH, where a single
		// address corresponds to the redeem script hash even though the script
		// may be a multi-signature script.
		for ia := range dbAddressRows[it] {
			// Transaction that pays to the address
			dba := &dbAddressRows[it][ia]

			// Do not store zero-value output data.
			if dba.Value == 0 {
				continue
			}

			// Set addresses row fields not set by InsertVouts: TxBlockTime,
			// IsFunding, ValidMainChain, and MatchingTxHash. Only
			// MatchingTxHash goes unset initially, later set by
			// insertAddrSpendingTxUpdateMatchedFunding (called by
			// SetSpendingForFundingOP below, and other places).
			dba.TxBlockTime = tx.BlockTime
			dba.IsFunding = true // from vouts
			dba.ValidMainChain = validMainChainTx

			// Funding tx hash, vout id, value, and address are already assigned
			// by InsertVouts. Only the block time and is_funding was needed.
			dbAddressRowsFlat = append(dbAddressRowsFlat, dba)
		}
	}
	return dbAddressRowsFlat
}

// CollectTicketSpendDBInfo processes the stake transactions in msgBlock, which
// correspond to the transaction data in dbTxns, and extracts data for votes and
// revokes, including the spent ticket hash and DB row ID.
func (pgb *ChainDB) CollectTicketSpendDBInfo(dbTxns []*dbtypes.Tx, txDbIDs []uint64,
	msgTxns []*wire.MsgTx, isMainchain bool) (spendingTxDbIDs []uint64, spendTypes []dbtypes.TicketSpendType,
	ticketHashes []string, ticketDbIDs []uint64, err error) {
	// This only makes sense for stake transactions. Check that the number of
	// dbTxns equals the number of STransactions in msgBlock.
	// msgTxns := msgBlock.STransactions
	if len(msgTxns) != len(dbTxns) {
		err = fmt.Errorf("number of stake transactions (%d) not as expected (%d)",
			len(msgTxns), len(dbTxns))
		return
	}

	for i, tx := range dbTxns {
		// Filter for votes and revokes only.
		var stakeSubmissionVinInd int
		var spendType dbtypes.TicketSpendType
		switch tx.TxType {
		case int16(stake.TxTypeSSGen):
			spendType = dbtypes.TicketVoted
			stakeSubmissionVinInd = 1
		case int16(stake.TxTypeSSRtx):
			spendType = dbtypes.TicketRevoked
		default:
			continue
		}

		// Ensure the transactions in dbTxns and msgBlock.STransactions correspond.
		msgTx := msgTxns[i]
		if tx.TxID != msgTx.CachedTxHash().String() {
			err = fmt.Errorf("txid of dbtypes.Tx does not match that of msgTx")
			return
		} // comment this check

		if stakeSubmissionVinInd >= len(msgTx.TxIn) {
			log.Warnf("Invalid vote or ticket with %d inputs", len(msgTx.TxIn))
			continue
		}

		spendTypes = append(spendTypes, spendType)

		// vote/revoke row ID in *transactions* table
		spendingTxDbIDs = append(spendingTxDbIDs, txDbIDs[i])

		// ticket hash
		ticketHash := msgTx.TxIn[stakeSubmissionVinInd].PreviousOutPoint.Hash.String()
		ticketHashes = append(ticketHashes, ticketHash)

		// ticket's row ID in *tickets* table
		expireEntries := isMainchain // expire all cache entries for main chain blocks
		t, err0 := pgb.unspentTicketCache.TxnDbID(ticketHash, expireEntries)
		if err0 != nil {
			err = fmt.Errorf("failed to retrieve ticket %s DB ID: %w", ticketHash, err0)
			return
		}
		ticketDbIDs = append(ticketDbIDs, t)
	}
	return
}

// UpdateSpendingInfoInAllAddresses completely rebuilds the matching transaction
// columns for funding rows of the addresses table. This is intended to be use
// after syncing all other tables and creating their indexes, particularly the
// indexes on the vins table, and the addresses table index on the funding tx
// columns. This can be used instead of using updateAddressesSpendingInfo=true
// with storeBlockTxnTree, which will update these addresses table columns too,
// but much more slowly for a number of reasons (that are well worth
// investigating BTW!).
func (pgb *ChainDB) UpdateSpendingInfoInAllAddresses(barLoad chan *dbtypes.ProgressBarLoad) (int64, error) {
	heightDB, err := pgb.HeightDB()
	if err != nil {
		return 0, fmt.Errorf("DBBestBlock: %w", err)
	}

	tStart := time.Now()

	chunk := int64(10000)
	var rowsTouched int64
	for i := int64(0); i <= heightDB; i += chunk {
		end := i + chunk
		if end > heightDB+1 {
			end = heightDB + 1
		}
		log.Infof("Updating address rows for blocks [%d,%d]...", i, end-1)
		res, err := pgb.db.Exec(internal.UpdateAllAddressesMatchingTxHashRange, i, end)
		if err != nil {
			return 0, err
		}
		N, err := res.RowsAffected()
		if err != nil {
			return 0, err
		}
		rowsTouched += N

		if barLoad != nil {
			timeTakenPerBlock := (time.Since(tStart).Seconds() / float64(end-i))
			barLoad <- &dbtypes.ProgressBarLoad{
				From:      i,
				To:        heightDB,
				Msg:       addressesSyncStatusMsg,
				BarID:     dbtypes.AddressesTableSync,
				Timestamp: int64(timeTakenPerBlock * float64(heightDB-i)),
			}

			tStart = time.Now()
		}
	}

	// Signal the completion of the sync to the status page.
	if barLoad != nil {
		barLoad <- &dbtypes.ProgressBarLoad{
			From:  heightDB,
			To:    heightDB,
			Msg:   addressesSyncStatusMsg,
			BarID: dbtypes.AddressesTableSync,
		}
	}

	return rowsTouched, nil
}

// UpdateSpendingInfoInAllTickets reviews all votes and revokes and sets this
// spending info in the tickets table.
func (pgb *ChainDB) UpdateSpendingInfoInAllTickets() (int64, error) {
	// The queries in this function should not timeout or (probably) canceled,
	// so use a background context.
	ctx := context.Background()

	// Get the full list of votes (DB IDs and heights), and spent ticket hashes
	allVotesDbIDs, allVotesHeights, ticketDbIDs, err :=
		RetrieveAllVotesDbIDsHeightsTicketDbIDs(ctx, pgb.db)
	if err != nil {
		log.Errorf("RetrieveAllVotesDbIDsHeightsTicketDbIDs: %v", err)
		return 0, err
	}

	// To update spending info in tickets table, get the spent tickets' DB
	// row IDs and block heights.
	spendTypes := make([]dbtypes.TicketSpendType, len(ticketDbIDs))
	for iv := range ticketDbIDs {
		spendTypes[iv] = dbtypes.TicketVoted
	}
	poolStatuses := ticketpoolStatusSlice(dbtypes.PoolStatusVoted, len(ticketDbIDs))

	// Update tickets table with spending info from new votes
	var totalTicketsUpdated int64
	totalTicketsUpdated, err = SetSpendingForTickets(pgb.db, ticketDbIDs,
		allVotesDbIDs, allVotesHeights, spendTypes, poolStatuses)
	if err != nil {
		log.Warn("SetSpendingForTickets:", err)
	}

	// Revokes

	revokeIDs, _, revokeHeights, vinDbIDs, err := RetrieveAllRevokes(ctx, pgb.db)
	if err != nil {
		log.Errorf("RetrieveAllRevokes: %v", err)
		return 0, err
	}

	revokedTicketHashes := make([]string, len(vinDbIDs))
	for i, vinDbID := range vinDbIDs {
		revokedTicketHashes[i], err = RetrieveFundingTxByVinDbID(ctx, pgb.db, vinDbID)
		if err != nil {
			log.Errorf("RetrieveFundingTxByVinDbID: %v", err)
			return 0, err
		}
	}

	revokedTicketDbIDs, err := RetrieveTicketIDsByHashes(ctx, pgb.db, revokedTicketHashes)
	if err != nil {
		log.Errorf("RetrieveTicketIDsByHashes: %v", err)
		return 0, err
	}

	poolStatuses = ticketpoolStatusSlice(dbtypes.PoolStatusMissed, len(revokedTicketHashes))
	pgb.stakeDB.LockStakeNode()
	for ih := range revokedTicketHashes {
		rh, _ := chainhash.NewHashFromStr(revokedTicketHashes[ih])
		if pgb.stakeDB.BestNode.ExistsExpiredTicket(*rh) {
			poolStatuses[ih] = dbtypes.PoolStatusExpired
		}
	}
	pgb.stakeDB.UnlockStakeNode()

	// To update spending info in tickets table, get the spent tickets' DB
	// row IDs and block heights.
	spendTypes = make([]dbtypes.TicketSpendType, len(revokedTicketDbIDs))
	for iv := range revokedTicketDbIDs {
		spendTypes[iv] = dbtypes.TicketRevoked
	}

	// Update tickets table with spending info from new votes
	var revokedTicketsUpdated int64
	revokedTicketsUpdated, err = SetSpendingForTickets(pgb.db, revokedTicketDbIDs,
		revokeIDs, revokeHeights, spendTypes, poolStatuses)
	if err != nil {
		log.Warn("SetSpendingForTickets:", err)
	}

	return totalTicketsUpdated + revokedTicketsUpdated, err
}

func ticketpoolStatusSlice(ss dbtypes.TicketPoolStatus, N int) []dbtypes.TicketPoolStatus {
	S := make([]dbtypes.TicketPoolStatus, N)
	for ip := range S {
		S[ip] = ss
	}
	return S
}

// GetChainWork fetches the chainjson.BlockHeaderVerbose and returns only the
// ChainWork attribute as a hex-encoded string, without 0x prefix.
func (pgb *ChainDB) GetChainWork(hash *chainhash.Hash) (string, error) {
	return rpcutils.GetChainWork(pgb.Client, hash)
}

// GenesisStamp returns the stamp of the lowest mainchain block in the database.
func (pgb *ChainDB) GenesisStamp() int64 {
	tDef := dbtypes.NewTimeDefFromUNIX(0)
	// Ignoring error and returning zero time.
	_ = pgb.db.QueryRowContext(pgb.ctx, internal.SelectGenesisTime).Scan(&tDef)
	return tDef.T.Unix()
}

// GetStakeInfoExtendedByHash fetches a apitypes.StakeInfoExtended, containing
// comprehensive data for the state of staking at a given block.
func (pgb *ChainDB) GetStakeInfoExtendedByHash(hashStr string) *apitypes.StakeInfoExtended {
	hash, err := chainhash.NewHashFromStr(hashStr)
	if err != nil {
		log.Errorf("GetStakeInfoExtendedByHash -> NewHashFromStr: %v", err)
		return nil
	}
	msgBlock, err := pgb.Client.GetBlock(pgb.ctx, hash)
	if err != nil {
		log.Errorf("GetStakeInfoExtendedByHash -> GetBlock: %v", err)
		return nil
	}
	block := dcrutil.NewBlock(msgBlock)

	height := msgBlock.Header.Height

	var size, val int64
	var winners []string
	err = pgb.db.QueryRowContext(pgb.ctx, internal.SelectPoolInfo,
		hashStr).Scan(pq.Array(&winners), &val, &size)
	if err != nil {
		log.Errorf("Error retrieving mainchain block with stats for hash %s: %v", hashStr, err)
		return nil
	}

	coin := dcrutil.Amount(val).ToCoin()
	tpi := &apitypes.TicketPoolInfo{
		Height:  height,
		Size:    uint32(size),
		Value:   coin,
		ValAvg:  coin / float64(size),
		Winners: winners,
	}

	windowSize := uint32(pgb.chainParams.StakeDiffWindowSize)
	feeInfo := txhelpers.FeeRateInfoBlock(block)

	return &apitypes.StakeInfoExtended{
		Hash:             hashStr,
		Feeinfo:          *feeInfo,
		StakeDiff:        dcrutil.Amount(msgBlock.Header.SBits).ToCoin(),
		PriceWindowNum:   int(height / windowSize),
		IdxBlockInWindow: int(height%windowSize) + 1,
		PoolInfo:         tpi,
	}
}

// GetStakeInfoExtendedByHeight gets extended stake information for the
// mainchain block at the specified height.
func (pgb *ChainDB) GetStakeInfoExtendedByHeight(height int) *apitypes.StakeInfoExtended {
	hashStr, err := pgb.BlockHash(int64(height))
	if err != nil {
		log.Errorf("GetStakeInfoExtendedByHeight -> BlockHash: %v", err)
		return nil
	}
	return pgb.GetStakeInfoExtendedByHash(hashStr)
}

// GetPoolInfo retrieves the ticket pool statistics at the specified height.
func (pgb *ChainDB) GetPoolInfo(idx int) *apitypes.TicketPoolInfo {
	ticketPoolInfo, err := RetrievePoolInfo(pgb.ctx, pgb.db, int64(idx))
	if err != nil {
		log.Errorf("Unable to retrieve ticket pool info: %v", err)
		return nil
	}
	return ticketPoolInfo
}

// GetPoolInfoByHash retrieves the ticket pool statistics at the specified block
// hash.
func (pgb *ChainDB) GetPoolInfoByHash(hash string) *apitypes.TicketPoolInfo {
	ticketPoolInfo, err := RetrievePoolInfoByHash(pgb.ctx, pgb.db, hash)
	if err != nil {
		log.Errorf("Unable to retrieve ticket pool info: %v", err)
		return nil
	}
	return ticketPoolInfo
}

// GetPoolInfoRange retrieves the ticket pool statistics for a range of block
// heights, as a slice.
func (pgb *ChainDB) GetPoolInfoRange(idx0, idx1 int) []apitypes.TicketPoolInfo {
	ind0 := int64(idx0)
	ind1 := int64(idx1)
	tip := pgb.Height()
	if ind1 > tip || ind0 < 0 {
		log.Errorf("Unable to retrieve ticket pool info for range [%d, %d], tip=%d", idx0, idx1, tip)
		return nil
	}
	ticketPoolInfos, _, err := RetrievePoolInfoRange(pgb.ctx, pgb.db, ind0, ind1)
	if err != nil {
		log.Errorf("Unable to retrieve ticket pool info range: %v", err)
		return nil
	}
	return ticketPoolInfos
}

// GetPoolValAndSizeRange returns the ticket pool size at each block height
// within a given range.
func (pgb *ChainDB) GetPoolValAndSizeRange(idx0, idx1 int) ([]float64, []uint32) {
	ind0 := int64(idx0)
	ind1 := int64(idx1)
	tip := pgb.Height()
	if ind1 > tip || ind0 < 0 {
		log.Errorf("Unable to retrieve ticket pool info for range [%d, %d], tip=%d", idx0, idx1, tip)
		return nil, nil
	}
	poolvals, poolsizes, err := RetrievePoolValAndSizeRange(pgb.ctx, pgb.db, ind0, ind1)
	if err != nil {
		log.Errorf("Unable to retrieve ticket value and size range: %v", err)
		return nil, nil
	}
	return poolvals, poolsizes
}

// ChargePoolInfoCache prepares the stakeDB by querying the database for block
// info.
func (pgb *ChainDB) ChargePoolInfoCache(startHeight int64) error {
	if startHeight < 0 {
		startHeight = 0
	}
	endHeight := pgb.Height()
	if startHeight > endHeight {
		log.Debug("No pool info to load into cache")
		return nil
	}
	tpis, blockHashes, err := RetrievePoolInfoRange(pgb.ctx, pgb.db, startHeight, endHeight)
	if err != nil {
		return err
	}
	log.Debugf("Pre-loading pool info for %d blocks ([%d, %d]) into cache.",
		len(tpis), startHeight, endHeight)
	for i := range tpis {
		hash, err := chainhash.NewHashFromStr(blockHashes[i])
		if err != nil {
			log.Warnf("Invalid block hash: %s", blockHashes[i])
		}
		pgb.stakeDB.SetPoolInfo(*hash, &tpis[i])
	}
	return nil
}

// GetPool retrieves all the live ticket hashes at a given height.
func (pgb *ChainDB) GetPool(idx int64) ([]string, error) {
	hs, err := pgb.stakeDB.PoolDB.Pool(idx)
	if err != nil {
		log.Errorf("Unable to get ticket pool from stakedb: %v", err)
		return nil, err
	}
	hss := make([]string, 0, len(hs))
	for i := range hs {
		hss = append(hss, hs[i].String())
	}
	return hss, nil
}

// DCP0010ActivationHeight indicates the height at which the changesubsidysplit
// agenda will activate, or -1 if it is not determined yet.
func (pgb *ChainDB) DCP0010ActivationHeight() int64 {
	if _, ok := txhelpers.SubsidySplitStakeVer(pgb.chainParams); !ok {
		return 0 // activate at genesis if no deployment defined in chaincfg.Params
	}

	agendaInfo, found := pgb.ChainInfo().AgendaMileStones[chaincfg.VoteIDChangeSubsidySplit]
	if !found {
		log.Warn("The changesubsidysplit agenda is missing.")
		return 0
	}

	switch agendaInfo.Status {
	case dbtypes.ActivatedAgendaStatus, dbtypes.LockedInAgendaStatus:
		return agendaInfo.Activated // rci already added for lockedin
	}
	return -1 // not activated, and no future activation height known
}

// IsDCP0010Active indicates if the "changesubsidysplit" consensus deployment is
// active at the given height according to the current status of the agendas.
func (pgb *ChainDB) IsDCP0010Active(height int64) bool {
	activeHeight := pgb.DCP0010ActivationHeight()
	if activeHeight == -1 {
		return false
	}
	return height >= activeHeight
}

// DCP0011ActivationHeight indicates the height at which the blake3pow
// agenda will activate, or -1 if it is not determined yet.
func (pgb *ChainDB) DCP0011ActivationHeight() int64 {
	if _, ok := txhelpers.Blake3PowStakeVer(pgb.chainParams); !ok {
		return 0 // activate at genesis if no deployment defined in chaincfg.Params
	}

	agendaInfo, found := pgb.ChainInfo().AgendaMileStones[chaincfg.VoteIDBlake3Pow]
	if !found {
		log.Warn("The blake3pow agenda is missing.")
		return 0
	}

	switch agendaInfo.Status {
	case dbtypes.ActivatedAgendaStatus, dbtypes.LockedInAgendaStatus:
		return agendaInfo.Activated // rci already added for lockedin
	}
	return -1 // not activated, and no future activation height known
}

// IsDCP0011Active indicates if the "blake3pow" consensus deployment is
// active at the given height according to the current status of the agendas.
func (pgb *ChainDB) IsDCP0011Active(height int64) bool {
	activeHeight := pgb.DCP0011ActivationHeight()
	if activeHeight == -1 {
		return false
	}
	return height >= activeHeight
}

// DCP0012ActivationHeight indicates the height at which the
// changesubsidysplitr2 agenda will activate, or -1 if it is not determined yet.
func (pgb *ChainDB) DCP0012ActivationHeight() int64 {
	if _, ok := txhelpers.SubsidySplitR2StakeVer(pgb.chainParams); !ok {
		return 0 // activate at genesis if no deployment defined in chaincfg.Params
	}

	agendaInfo, found := pgb.ChainInfo().AgendaMileStones[chaincfg.VoteIDChangeSubsidySplitR2]
	if !found {
		log.Warn("The changesubsidysplitr2 agenda is missing.")
		return 0
	}

	switch agendaInfo.Status {
	case dbtypes.ActivatedAgendaStatus, dbtypes.LockedInAgendaStatus:
		return agendaInfo.Activated // rci already added for lockedin
	}
	return -1 // not activated, and no future activation height known
}

// IsDCP0012Active indicates if the "blake3pow" consensus deployment is
// active at the given height according to the current status of the agendas.
func (pgb *ChainDB) IsDCP0012Active(height int64) bool {
	activeHeight := pgb.DCP0012ActivationHeight()
	if activeHeight == -1 {
		return false
	}
	return height >= activeHeight
}

// CurrentCoinSupply gets the current coin supply as an *apitypes.CoinSupply,
// which additionally contains block info and max supply.
func (pgb *ChainDB) CurrentCoinSupply() (supply *apitypes.CoinSupply) {
	coinSupply, err := pgb.Client.GetCoinSupply(pgb.ctx)
	if err != nil {
		log.Errorf("RPC failure (GetCoinSupply): %v", err)
		return
	}

	dcp0010Height := pgb.DCP0010ActivationHeight()
	dcp0012Height := pgb.DCP0012ActivationHeight()
	hash, height := pgb.BestBlockStr()

	return &apitypes.CoinSupply{
		Height:   height,
		Hash:     hash,
		Mined:    int64(coinSupply),
		Ultimate: txhelpers.UltimateSubsidy(pgb.chainParams, dcp0010Height, dcp0012Height),
	}
}

// GetBlockByHash gets a *wire.MsgBlock for the supplied hex-encoded hash
// string.
func (pgb *ChainDB) GetBlockByHash(hash string) (*wire.MsgBlock, error) {
	blockHash, err := chainhash.NewHashFromStr(hash)
	if err != nil {
		log.Errorf("Invalid block hash %s", hash)
		return nil, err
	}
	return pgb.Client.GetBlock(context.TODO(), blockHash)
}

func (pgb *ChainDB) GetMutilchainBlockHeightByHash(hash string, chainType string) (int64, error) {
	switch chainType {
	case mutilchain.TYPEBTC:
		return pgb.GetBTCBlockByHash(hash)
	case mutilchain.TYPELTC:
		return pgb.GetLTCBlockByHash(hash)
	default:
		return 0, nil
	}
}

func (pgb *ChainDB) GetBTCBlockByHash(hash string) (int64, error) {
	blockHash, err := btc_chainhash.NewHashFromStr(hash)
	if err != nil {
		log.Errorf("BTC: Invalid block hash %s", hash)
		return 0, err
	}
	blockRst, err := pgb.BtcClient.GetBlockVerbose(blockHash)
	if err != nil {
		log.Errorf("BTC: Get msg block failed %s", hash)
		return 0, err
	}
	return blockRst.Height, nil
}

func (pgb *ChainDB) GetLTCBlockByHash(hash string) (int64, error) {
	blockHash, err := ltc_chainhash.NewHashFromStr(hash)
	if err != nil {
		log.Errorf("LTC: Invalid block hash %s", hash)
		return 0, err
	}
	blockRst, err := pgb.LtcClient.GetBlockVerbose(blockHash)
	if err != nil {
		log.Errorf("LTC: Get msg block failed %s", hash)
		return 0, err
	}
	return blockRst.Height, nil
}

// GetHeader fetches the *chainjson.GetBlockHeaderVerboseResult for a given
// block height.
func (pgb *ChainDB) GetHeader(idx int) *chainjson.GetBlockHeaderVerboseResult {
	return rpcutils.GetBlockHeaderVerbose(pgb.Client, int64(idx))
}

// GetBlockHeaderByHash fetches the *chainjson.GetBlockHeaderVerboseResult for
// a given block hash.
func (pgb *ChainDB) GetBlockHeaderByHash(hash string) (*wire.BlockHeader, error) {
	blockHash, err := chainhash.NewHashFromStr(hash)
	if err != nil {
		log.Errorf("Invalid block hash %s", hash)
		return nil, err
	}
	return pgb.Client.GetBlockHeader(context.TODO(), blockHash)
}

// GetBlockHeight returns the height of the block with the specified hash.
func (pgb *ChainDB) GetBlockHeight(hash string) (int64, error) {
	// _, err := chainhash.NewHashFromStr(hash)
	// if err != nil {
	// 	return -1, err
	// }
	ctx, cancel := context.WithTimeout(pgb.ctx, pgb.queryTimeout)
	defer cancel()
	height, err := RetrieveBlockHeight(ctx, pgb.db, hash)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.Errorf("Unexpected error retrieving block height for hash %s: %v", hash, err)
		}
		return -1, pgb.replaceCancelError(err)
	}
	return height, nil
}

// GetAPITransaction gets an *apitypes.Tx for a given transaction ID.
func (pgb *ChainDB) GetAPITransaction(txid *chainhash.Hash) *apitypes.Tx {
	// We're going to be lazy use a node RPC since it gives is block data,
	// confirmations, ticket commitment info decoded, etc. The DB also lacks
	// ScriptSig and Sequence.
	txraw, err := pgb.Client.GetRawTransactionVerbose(pgb.ctx, txid)
	if err != nil {
		log.Errorf("APITransaction failed for %v: %v", txid, err)
		return nil
	}

	msgTx, err := txhelpers.MsgTxFromHex(txraw.Hex)
	if err != nil {
		log.Errorf("Cannot create MsgTx for tx %v: %v", txid.String(), err)
		return nil
	}

	txTree := txhelpers.TxTree(msgTx)

	tx := &apitypes.Tx{
		TxShort: apitypes.TxShort{
			TxID:     txraw.Txid,
			Size:     int32(len(txraw.Hex) / 2),
			Version:  txraw.Version,
			Locktime: txraw.LockTime,
			Expiry:   txraw.Expiry,
			Vin:      make([]apitypes.Vin, len(txraw.Vin)),
			Vout:     make([]apitypes.Vout, len(txraw.Vout)),
			Tree:     txTree,
			Type:     strings.ToLower(txhelpers.TxTypeToString(int(txTree))),
		},
		Confirmations: txraw.Confirmations,
		Block: &apitypes.BlockID{
			BlockHash:   txraw.BlockHash,
			BlockHeight: txraw.BlockHeight,
			BlockIndex:  txraw.BlockIndex,
			Time:        txraw.Time,
			BlockTime:   txraw.Blocktime,
		},
	}

	copy(tx.Vin, txraw.Vin)

	for i := range txraw.Vout {
		vout := &txraw.Vout[i]
		tx.Vout[i].Value = vout.Value
		tx.Vout[i].N = vout.N
		tx.Vout[i].Version = vout.Version
		spk := &tx.Vout[i].ScriptPubKeyDecoded
		spkRaw := &vout.ScriptPubKey
		spk.Asm = spkRaw.Asm
		spk.Version = spkRaw.Version
		spk.Hex = spkRaw.Hex
		pkScript, _ := hex.DecodeString(spkRaw.Hex)
		scriptType := stdscript.DetermineScriptType(spkRaw.Version, pkScript)
		spk.Type = apitypes.NewScriptClass(scriptType).String()
		spk.ReqSigs = spkRaw.ReqSigs
		spk.Addresses = make([]string, len(spkRaw.Addresses))
		copy(spk.Addresses, spkRaw.Addresses)
		if spkRaw.CommitAmt != nil {
			spk.CommitAmt = new(float64)
			*spk.CommitAmt = *spkRaw.CommitAmt
			spk.Type = apitypes.ScriptClassStakeSubCommit.String()
		}
	}

	return tx
}

// GetBTCAPITransaction gets an *apitypes.Tx for a given btc transaction ID.
func (pgb *ChainDB) GetBTCAPITransaction(txid string) (any, error) {
	txHash, err := btc_chainhash.NewHashFromStr(txid)
	if err != nil {
		return nil, err
	}
	txraw, err := pgb.BtcClient.GetRawTransactionVerbose(txHash)
	if err != nil {
		log.Errorf("GetBTCAPITransaction failed for %v: %v", txid, err)
		return nil, err
	}

	return txraw, nil
}

// GetLTCAPITransaction gets an *apitypes.Tx for a given ltc transaction ID.
func (pgb *ChainDB) GetLTCAPITransaction(txid string) (any, error) {
	txHash, err := ltc_chainhash.NewHashFromStr(txid)
	if err != nil {
		return nil, err
	}
	txraw, err := pgb.LtcClient.GetRawTransactionVerbose(txHash)
	if err != nil {
		log.Errorf("GetLTCAPITransaction failed for %v: %v", txid, err)
		return nil, err
	}

	return txraw, nil
}

// GetMultichainTransactionVerbose return verbose of multichain tx
func (pgb *ChainDB) GetMultichainTransactionVerbose(txid, chainType string) (any, error) {
	switch chainType {
	case mutilchain.TYPEBTC:
		return pgb.GetBTCAPITransaction(txid)
	case mutilchain.TYPELTC:
		return pgb.GetLTCAPITransaction(txid)
	}
	return nil, fmt.Errorf("GetMultichainTransactionVerbose chaintype invalid")
}

// GetTrimmedTransaction gets a *apitypes.TrimmedTx for a given transaction ID.
func (pgb *ChainDB) GetTrimmedTransaction(txid *chainhash.Hash) *apitypes.TrimmedTx {
	tx := pgb.GetAPITransaction(txid)
	if tx == nil {
		return nil
	}
	return &apitypes.TrimmedTx{
		TxID:     tx.TxID,
		Version:  tx.Version,
		Locktime: tx.Locktime,
		Expiry:   tx.Expiry,
		Vin:      tx.Vin,
		Vout:     tx.Vout,
	}
}

// GetVoteInfo attempts to decode the vote bits of a SSGen transaction. If the
// transaction is not a valid SSGen, the VoteInfo output will be nil. Depending
// on the stake version with which dcrdata is compiled with (chaincfg.Params),
// the Choices field of VoteInfo may be a nil slice even if the votebits were
// set for a previously-valid agenda.
func (pgb *ChainDB) GetVoteInfo(txhash *chainhash.Hash) (*apitypes.VoteInfo, error) {
	tx, err := pgb.Client.GetRawTransaction(context.TODO(), txhash)
	if err != nil {
		log.Errorf("GetRawTransaction failed for: %v", txhash)
		return nil, nil
	}

	validation, version, bits, choices, tspendVotes, err := txhelpers.SSGenVoteChoices(tx.MsgTx(), pgb.chainParams)
	if err != nil {
		return nil, err
	}
	vinfo := &apitypes.VoteInfo{
		Validation: apitypes.BlockValidation{
			Hash:     validation.Hash.String(),
			Height:   validation.Height,
			Validity: validation.Validity,
		},
		Version: version,
		Bits:    bits,
		Choices: choices,
		TSpends: apitypes.ConvertTSpendVotes(tspendVotes),
	}
	return vinfo, nil
}

// GetVoteVersionInfo requests stake version info from the dcrd RPC server
func (pgb *ChainDB) GetVoteVersionInfo(ver uint32) (*chainjson.GetVoteInfoResult, error) {
	return pgb.Client.GetVoteInfo(context.TODO(), ver)
}

// GetStakeVersions requests the output of the getstakeversions RPC, which gets
// stake version information and individual vote version information starting at the
// given block and for count-1 blocks prior.
func (pgb *ChainDB) GetStakeVersions(blockHash string, count int32) (*chainjson.GetStakeVersionsResult, error) {
	return pgb.Client.GetStakeVersions(context.TODO(), blockHash, count)
}

// GetStakeVersionsLatest requests the output of the getstakeversions RPC for
// just the current best block.
func (pgb *ChainDB) GetStakeVersionsLatest() (*chainjson.StakeVersions, error) {
	txHash := pgb.BestBlockHashStr()
	stkVers, err := pgb.GetStakeVersions(txHash, 1)
	if err != nil || stkVers == nil || len(stkVers.StakeVersions) == 0 {
		return nil, err
	}
	stkVer := stkVers.StakeVersions[0]
	return &stkVer, nil
}

// GetAllTxIn gets all transaction inputs, as a slice of *apitypes.TxIn, for a
// given transaction ID.
func (pgb *ChainDB) GetAllTxIn(txid *chainhash.Hash) []*apitypes.TxIn {
	tx, err := pgb.Client.GetRawTransaction(context.TODO(), txid)
	if err != nil {
		log.Errorf("Unknown transaction %s", txid)
		return nil
	}

	allTxIn0 := tx.MsgTx().TxIn
	allTxIn := make([]*apitypes.TxIn, len(allTxIn0))
	for i := range allTxIn {
		txIn := &apitypes.TxIn{
			PreviousOutPoint: apitypes.OutPoint{
				Hash:  allTxIn0[i].PreviousOutPoint.Hash.String(),
				Index: allTxIn0[i].PreviousOutPoint.Index,
				Tree:  allTxIn0[i].PreviousOutPoint.Tree,
			},
			Sequence:        allTxIn0[i].Sequence,
			ValueIn:         dcrutil.Amount(allTxIn0[i].ValueIn).ToCoin(),
			BlockHeight:     allTxIn0[i].BlockHeight,
			BlockIndex:      allTxIn0[i].BlockIndex,
			SignatureScript: hex.EncodeToString(allTxIn0[i].SignatureScript),
		}
		allTxIn[i] = txIn
	}

	return allTxIn
}

// GetAllTxOut gets all transaction outputs, as a slice of *apitypes.TxOut, for
// a given transaction ID.
func (pgb *ChainDB) GetAllTxOut(txid *chainhash.Hash) []*apitypes.TxOut {
	// Get the TxRawResult since it provides Asm and CommitAmt for all the
	// output scripts, but we could extract that info too.
	tx, err := pgb.Client.GetRawTransactionVerbose(context.TODO(), txid)
	if err != nil {
		log.Warnf("Unknown transaction %s", txid)
		return nil
	}

	txouts := tx.Vout
	allTxOut := make([]*apitypes.TxOut, 0, len(txouts))
	for i := range txouts {
		// chainjson.Vout and apitypes.TxOut are the same except for N.
		spk := &tx.Vout[i].ScriptPubKey
		pkScript, _ := hex.DecodeString(spk.Hex) // if error, empty script, non-std class
		scriptClass := stdscript.DetermineScriptType(spk.Version, pkScript)
		apiScriptClass := apitypes.NewScriptClass(scriptClass)
		if apiScriptClass == apitypes.ScriptClassNullData && spk.CommitAmt != nil {
			apiScriptClass = apitypes.ScriptClassStakeSubCommit
		}
		allTxOut = append(allTxOut, &apitypes.TxOut{
			Value:   txouts[i].Value,
			Version: txouts[i].Version,
			ScriptPubKeyDecoded: apitypes.ScriptPubKey{
				Asm:       spk.Asm,
				Hex:       spk.Hex,
				Version:   spk.Version,
				ReqSigs:   spk.ReqSigs,
				Type:      apiScriptClass.String(),
				Addresses: spk.Addresses,
				CommitAmt: spk.CommitAmt,
			},
		})
	}

	return allTxOut
}

// GetStakeDiffEstimates gets an *apitypes.StakeDiff, which is a combo of
// chainjson.EstimateStakeDiffResult and chainjson.GetStakeDifficultyResult
func (pgb *ChainDB) GetStakeDiffEstimates() *apitypes.StakeDiff {
	stakeDiff, err := pgb.Client.GetStakeDifficulty(pgb.ctx)
	if err != nil {
		log.Errorf("GetStakeDifficulty: %v", err)
		return nil
	}
	estStakeDiff, err := pgb.Client.EstimateStakeDiff(pgb.ctx, nil)
	if err != nil {
		log.Errorf("EstimateStakeDiff: %v", err)
		return nil
	}

	height := pgb.MPC.GetHeight()
	winSize := uint32(pgb.chainParams.StakeDiffWindowSize)

	return &apitypes.StakeDiff{
		GetStakeDifficultyResult: *stakeDiff,
		Estimates:                *estStakeDiff,
		IdxBlockInWindow:         int(height%winSize) + 1,
		PriceWindowNum:           int(height / winSize),
	}
}

// GetSummary returns the *apitypes.BlockDataBasic for a given block height.
func (pgb *ChainDB) GetSummary(idx int) *apitypes.BlockDataBasic {
	blockSummary, err := pgb.BlockSummary(int64(idx))
	if err != nil {
		log.Errorf("Unable to retrieve block summary: %v", err)
		return nil
	}

	return blockSummary
}

// BlockSummary returns basic block data for block ind.
func (pgb *ChainDB) BlockSummary(ind int64) (*apitypes.BlockDataBasic, error) {
	// First try the block summary cache.
	usingBlockCache := pgb.BlockCache != nil && pgb.BlockCache.IsEnabled()
	if usingBlockCache {
		if bd := pgb.BlockCache.GetBlockSummary(ind); bd != nil {
			// Cache hit!
			return bd, nil
		}
		// Cache miss necessitates a DB query.
	}

	bd, err := RetrieveBlockSummary(pgb.ctx, pgb.db, ind)
	if err != nil {
		return nil, err
	}

	if usingBlockCache {
		// This is a cache miss since hits return early.
		err = pgb.BlockCache.StoreBlockSummary(bd)
		if err != nil {
			log.Warnf("Failed to cache summary for block at %d: %v", ind, err)
			// Do not return the error.
		}
	}

	return bd, nil
}

// GetSummaryRange returns the *apitypes.BlockDataBasic for a range of block
// heights.
func (pgb *ChainDB) GetSummaryRange(idx0, idx1 int) []*apitypes.BlockDataBasic {
	summaries, err := pgb.BlockSummaryRange(int64(idx0), int64(idx1))
	if err != nil {
		log.Errorf("Unable to retrieve block summaries: %v", err)
		return nil
	}
	return summaries
}

// BlockSummaryRange returns the *apitypes.BlockDataBasic for a range of block
// height.
func (pgb *ChainDB) BlockSummaryRange(idx0, idx1 int64) ([]*apitypes.BlockDataBasic, error) {
	return RetrieveBlockSummaryRange(pgb.ctx, pgb.db, idx0, idx1)
}

// GetSummaryRangeStepped returns the []*apitypes.BlockDataBasic for a given
// block height.
func (pgb *ChainDB) GetSummaryRangeStepped(idx0, idx1, step int) []*apitypes.BlockDataBasic {
	summaries, err := pgb.BlockSummaryRangeStepped(int64(idx0), int64(idx1), int64(step))
	if err != nil {
		log.Errorf("Unable to retrieve block summaries: %v", err)
		return nil
	}

	return summaries
}

// BlockSummaryRangeStepped returns the []*apitypes.BlockDataBasic for every
// step'th block in a specified range.
func (pgb *ChainDB) BlockSummaryRangeStepped(idx0, idx1, step int64) ([]*apitypes.BlockDataBasic, error) {
	return RetrieveBlockSummaryRangeStepped(pgb.ctx, pgb.db, idx0, idx1, step)
}

// GetSummaryByHash returns a *apitypes.BlockDataBasic for a given hex-encoded
// block hash. If withTxTotals is true, the TotalSent and MiningFee fields will
// be set, but it's costly because it requires a GetBlockVerboseByHash RPC call.
func (pgb *ChainDB) GetSummaryByHash(hash string, withTxTotals bool) *apitypes.BlockDataBasic {
	blockSummary, err := pgb.BlockSummaryByHash(hash)
	if err != nil {
		log.Errorf("Unable to retrieve block summary: %v", err)
		return nil
	}

	if withTxTotals {
		data := pgb.GetBlockVerboseByHash(hash, true)
		if data == nil {
			log.Error("Unable to get block for block hash " + hash)
			return nil
		}

		var totalFees, totalOut dcrutil.Amount
		for i := range data.RawTx {
			rawTx := &data.RawTx[i]
			msgTx, err := txhelpers.MsgTxFromHex(rawTx.Hex)
			if err != nil {
				log.Errorf("Unable to decode transaction: %v", err)
				return nil
			}
			// Do not compute fee for coinbase transaction.
			prev := rawTx.Vin[0].Txid
			if i == 0 && prev == "" || txhelpers.IsZeroHashStr(prev) {
				fee, _ := txhelpers.TxFeeRate(msgTx)
				totalFees += fee
			}
			totalOut += txhelpers.TotalOutFromMsgTx(msgTx)
		}
		for i := range data.RawSTx {
			msgTx, err := txhelpers.MsgTxFromHex(data.RawSTx[i].Hex)
			if err != nil {
				log.Errorf("Unable to decode transaction: %v", err)
				return nil
			}
			fee, _ := txhelpers.TxFeeRate(msgTx)
			totalFees += fee
			totalOut += txhelpers.TotalOutFromMsgTx(msgTx)
		}

		miningFee := int64(totalFees)
		blockSummary.MiningFee = &miningFee
		totalSent := int64(totalOut)
		blockSummary.TotalSent = &totalSent
	}

	return blockSummary
}

// BlockSummaryByHash makes a *apitypes.BlockDataBasic, checking the BlockCache
// first before querying the database.
func (pgb *ChainDB) BlockSummaryByHash(hash string) (*apitypes.BlockDataBasic, error) {
	// First try the block summary cache.
	usingBlockCache := pgb.BlockCache != nil && pgb.BlockCache.IsEnabled()
	if usingBlockCache {
		if bd := pgb.BlockCache.GetBlockSummaryByHash(hash); bd != nil {
			// Cache hit!
			return bd, nil
		}
		// Cache miss necessitates a DB query.
	}

	bd, err := RetrieveBlockSummaryByHash(pgb.ctx, pgb.db, hash)
	if err != nil {
		return nil, err
	}

	if usingBlockCache {
		// This is a cache miss since hits return early.
		err = pgb.BlockCache.StoreBlockSummary(bd)
		if err != nil {
			log.Warnf("Failed to cache summary for block %s: %v", hash, err)
			// Do not return the error.
		}
	}

	return bd, nil
}

// GetBestBlockSummary retrieves data for the best block in the DB. If there are
// no blocks in the table (yet), a nil pointer is returned.
func (pgb *ChainDB) GetBestBlockSummary() *apitypes.BlockDataBasic {
	// Attempt to retrieve height of best block in DB.
	dbBlkHeight, err := pgb.HeightDB()
	if err != nil {
		log.Errorf("GetBlockSummaryHeight failed: %v", err)
		return nil
	}

	// Empty table is not an error.
	if dbBlkHeight == -1 {
		return nil
	}

	// Retrieve the block data.
	blockSummary, err := pgb.BlockSummary(dbBlkHeight)
	if err != nil {
		log.Errorf("Unable to retrieve block %d summary: %v", dbBlkHeight, err)
		return nil
	}

	return blockSummary
}

// GetBlockSize returns the block size in bytes for the block at a given block
// height.
func (pgb *ChainDB) GetBlockSize(idx int) (int32, error) {
	blockSize, err := pgb.BlockSize(int64(idx))
	if err != nil {
		log.Errorf("Unable to retrieve block %d size: %v", idx, err)
		return -1, err
	}
	return blockSize, nil
}

// GetBlockSizeRange gets the block sizes in bytes for an inclusive range of
// block heights.
func (pgb *ChainDB) GetBlockSizeRange(idx0, idx1 int) ([]int32, error) {
	blockSizes, err := pgb.BlockSizeRange(int64(idx0), int64(idx1))
	if err != nil {
		log.Errorf("Unable to retrieve block size range: %v", err)
		return nil, err
	}
	return blockSizes, nil
}

// BlockSize return the size of block at height ind.
func (pgb *ChainDB) BlockSize(ind int64) (int32, error) {
	// First try the block summary cache.
	usingBlockCache := pgb.BlockCache != nil && pgb.BlockCache.IsEnabled()
	if usingBlockCache {
		sz := pgb.BlockCache.GetBlockSize(ind)
		if sz != -1 {
			// Cache hit!
			return sz, nil
		}
		// Cache miss necessitates a DB query.
	}

	tip := pgb.Height()
	if ind > tip || ind < 0 {
		return -1, fmt.Errorf("Cannot retrieve block size %d, have height %d",
			ind, tip)
	}

	return RetrieveBlockSize(pgb.ctx, pgb.db, ind)
}

// BlockSizeRange returns an array of block sizes for block range ind0 to ind1
func (pgb *ChainDB) BlockSizeRange(ind0, ind1 int64) ([]int32, error) {
	tip := pgb.Height()
	if ind1 > tip || ind0 < 0 {
		return nil, fmt.Errorf("Cannot retrieve block size range [%d,%d], have height %d",
			ind0, ind1, tip)
	}
	return RetrieveBlockSizeRange(pgb.ctx, pgb.db, ind0, ind1)
}

// GetSDiff gets the stake difficulty in DCR for a given block height.
func (pgb *ChainDB) GetSDiff(idx int) float64 {
	sdiff, err := RetrieveSDiff(pgb.ctx, pgb.db, int64(idx))
	if err != nil {
		log.Errorf("Unable to retrieve stake difficulty: %v", err)
		return -1
	}
	return sdiff
}

// GetSBitsByHash gets the stake difficulty in DCR for a given block height.
func (pgb *ChainDB) GetSBitsByHash(hash string) int64 {
	sbits, err := RetrieveSBitsByHash(pgb.ctx, pgb.db, hash)
	if err != nil {
		log.Errorf("Unable to retrieve stake difficulty: %v", err)
		return -1
	}
	return sbits
}

// GetSDiffRange gets the stake difficulties in DCR for a range of block heights.
func (pgb *ChainDB) GetSDiffRange(idx0, idx1 int) []float64 {
	sdiffs, err := pgb.SDiffRange(int64(idx0), int64(idx1))
	if err != nil {
		log.Errorf("Unable to retrieve stake difficulty range: %v", err)
		return nil
	}
	return sdiffs
}

// SDiffRange returns an array of stake difficulties for block range
// ind0 to ind1.
func (pgb *ChainDB) SDiffRange(ind0, ind1 int64) ([]float64, error) {
	tip := pgb.Height()
	if ind1 > tip || ind0 < 0 {
		return nil, fmt.Errorf("Cannot retrieve sdiff range [%d,%d], have height %d",
			ind0, ind1, tip)
	}
	return RetrieveSDiffRange(pgb.ctx, pgb.db, ind0, ind1)
}

// GetMempoolSSTxSummary returns the current *apitypes.MempoolTicketFeeInfo.
func (pgb *ChainDB) GetMempoolSSTxSummary() *apitypes.MempoolTicketFeeInfo {
	_, feeInfo := pgb.MPC.GetFeeInfoExtra()
	return feeInfo
}

// GetMempoolSSTxFeeRates returns the current mempool stake fee info for tickets
// above height N in the mempool cache.
func (pgb *ChainDB) GetMempoolSSTxFeeRates(N int) *apitypes.MempoolTicketFees {
	height, timestamp, totalFees, fees := pgb.MPC.GetFeeRates(N)
	mpTicketFees := apitypes.MempoolTicketFees{
		Height:   height,
		Time:     timestamp,
		Length:   uint32(len(fees)),
		Total:    uint32(totalFees),
		FeeRates: fees,
	}
	return &mpTicketFees
}

// GetMempoolSSTxDetails returns the current mempool ticket info for tickets
// above height N in the mempool cache.
func (pgb *ChainDB) GetMempoolSSTxDetails(N int) *apitypes.MempoolTicketDetails {
	height, timestamp, totalSSTx, details := pgb.MPC.GetTicketsDetails(N)
	mpTicketDetails := apitypes.MempoolTicketDetails{
		Height:  height,
		Time:    timestamp,
		Length:  uint32(len(details)),
		Total:   uint32(totalSSTx),
		Tickets: []*apitypes.TicketDetails(details),
	}
	return &mpTicketDetails
}

func decPkScript(ver uint16, pkScript []byte, isTicketCommit bool, chainParams *chaincfg.Params) (spkDec apitypes.ScriptPubKey) {
	scriptType, addrs := stdscript.ExtractAddrs(ver, pkScript, chainParams)
	reqSigs := stdscript.DetermineRequiredSigs(ver, pkScript)
	spkDec.Asm, _ = txscript.DisasmString(pkScript)
	spkDec.Hex = hex.EncodeToString(pkScript)
	spkDec.ReqSigs = int32(reqSigs)
	if len(addrs) > 0 {
		spkDec.Addresses = make([]string, len(addrs))
		for i := range addrs {
			spkDec.Addresses[i] = addrs[i].String()
		}
	}

	if !isTicketCommit {
		spkDec.Type = apitypes.NewScriptClass(scriptType).String()
		return
	}

	// If it's a ticket commitment (sstxcommitment), decode the address and
	// amount from the OP_RETURN.
	spkDec.Type = apitypes.ScriptClassStakeSubCommit.String()
	addr, err := stake.AddrFromSStxPkScrCommitment(pkScript, chainParams)
	if err != nil {
		log.Warnf("failed to decode ticket commitment addr for script %x: %v",
			pkScript, err)
	} else {
		spkDec.Addresses = []string{addr.String()}
	}
	amt, err := stake.AmountFromSStxPkScrCommitment(pkScript)
	if err != nil {
		log.Warnf("failed to decode ticket commitment amt output for script %x: %v",
			pkScript, err)
	} else {
		commitAmt := amt.ToCoin()
		spkDec.CommitAmt = &commitAmt
	}

	return
}

// GetAddressTransactionsRawWithSkip returns a slice of apitypes.AddressTxRaw
// objects similar to the raw result of SearchRawTransactionsVerbose, but with
// fewer Vin details, namely the SigScript and Sequence. This powers the
// /address/{addr}/.../raw API endpoints.
func (pgb *ChainDB) GetAddressTransactionsRawWithSkip(addr string, count, skip int) []*apitypes.AddressTxRaw {
	_, err := stdaddr.DecodeAddress(addr, pgb.chainParams) // likely redundant with checks in caller
	if err != nil {
		log.Infof("Invalid address %s: %v", addr, err)
		return nil
	}

	var txs []*apitypes.AddressTxRaw

	// Mempool
	mpTxs, _, err := pgb.mp.UnconfirmedTxnsForAddress(addr)
	if err != nil {
		log.Errorf("GetAddressTransactionsRawWithSkip: UnconfirmedTxnsForAddress %s: %v", addr, err)
		return nil
	}
	for hash, mpTx := range mpTxs.TxnsStore {
		tx := mpTx.Tx
		txType := stake.DetermineTxType(tx)

		vins := make([]apitypes.VinShort, len(tx.TxIn))
		for i, txIn := range tx.TxIn {
			vins[i] = apitypes.VinShort{
				Txid: txIn.PreviousOutPoint.Hash.String(),
				Vout: txIn.PreviousOutPoint.Index,
				Tree: txIn.PreviousOutPoint.Tree,
				// Unconfirmed, so no BlockHeight or BlockIndex set.
			}
			vin := &vins[i]
			if i == 0 && vin.Txid == string(zeroHashStringBytes) {
				switch txType {
				case stake.TxTypeRegular:
					vin.Coinbase = true
				case stake.TxTypeSSGen:
					vin.Stakebase = true
				case stake.TxTypeTSpend:
					vin.TreasurySpend = true
				case stake.TxTypeTreasuryBase:
					vin.Treasurybase = true
				}
			}
			if txIn.ValueIn > 0 {
				vin.AmountIn = dcrutil.Amount(txIn.ValueIn).ToCoin()
			}
		}
		vouts := make([]apitypes.Vout, len(tx.TxOut))
		for i, txOut := range tx.TxOut {
			isTicketCommit := txType == stake.TxTypeSStx && (i%2 != 0)
			vouts[i] = apitypes.Vout{
				Value:               dcrutil.Amount(txOut.Value).ToCoin(),
				N:                   uint32(i),
				Version:             txOut.Version,
				ScriptPubKeyDecoded: decPkScript(txOut.Version, txOut.PkScript, isTicketCommit, pgb.chainParams),
			}
		}
		txs = append(txs, &apitypes.AddressTxRaw{
			Size:     int32(tx.SerializeSize()),
			TxID:     hash.String(),
			Version:  int32(tx.Version),
			Locktime: tx.LockTime,
			Type:     int32(txType),
			Vin:      vins,
			Vout:     vouts,
			Time:     apitypes.NewTimeAPIFromUNIX(mpTx.MemPoolTime),
		})
	}

	_, height := pgb.BestBlock() // for confirmations

	ctx, cancel := context.WithTimeout(pgb.ctx, 10*time.Second)
	defer cancel()

	// vins
	rows, err := pgb.db.QueryContext(ctx, internal.SelectVinsForAddress, addr, count, skip)
	if err != nil {
		log.Errorf("GetAddressTransactionsRawWithSkip: SelectVinsForAddress %s: %v", addr, err)
		return nil
	}
	defer rows.Close()

	type vinIndexed struct {
		*apitypes.VinShort
		idx int32
	}
	vins := make(map[string][]*vinIndexed)
	for rows.Next() {
		var txid string // redeeming tx
		var idx int32
		var vin apitypes.VinShort
		var val int64
		if err = rows.Scan(&txid, &idx, &vin.Txid, &vin.Vout, &vin.Tree, &val,
			&vin.BlockHeight, &vin.BlockIndex); err != nil {
			log.Errorf("GetAddressTransactionsRawWithSkip: SelectVinsForAddress %s: %v", addr, err)
			return nil
		}

		if val > 0 {
			vin.AmountIn = dcrutil.Amount(val).ToCoin()
		}
		// Coinbase, Stakebase, etc. booleans are set on TxType detection below.
		vins[txid] = append(vins[txid], &vinIndexed{&vin, idx})
	}

	// tx
	rows, err = pgb.db.QueryContext(ctx, internal.SelectAddressTxns, addr, count, skip)
	if err != nil {
		log.Errorf("GetAddressTransactionsRawWithSkip: SelectAddressTxns %s: %v", addr, err)
		return nil
	}
	defer rows.Close()

	txns := make(map[string]*apitypes.AddressTxRaw)

	for rows.Next() {
		var tx apitypes.AddressTxRaw
		var blockHeight int64
		var numVins, numVouts int32
		// var vinDbIDs, voutDbIDs pq.Int64Array
		if err = rows.Scan(&tx.TxID, &tx.BlockHash, &blockHeight,
			&tx.Time.S, &tx.Version, &tx.Locktime, &tx.Size, &tx.Type, &numVins, &numVouts /*, &vinDbIDs, &voutDbIDs*/); err != nil {
			log.Errorf("GetAddressTransactionsRawWithSkip: Scan %s: %v", addr, err)
			return nil
		}
		tx.Blocktime = &tx.Time
		tx.Confirmations = height - blockHeight + 1

		txns[tx.TxID] = &tx

		txVins, found := vins[tx.TxID]
		if found {
			sort.Slice(txVins, func(i, j int) bool {
				return txVins[i].idx < txVins[j].idx
			})
			tx.Vin = make([]apitypes.VinShort, 0, numVins)
			for i, vin := range txVins {
				if i == 0 && vin.Txid == string(zeroHashStringBytes) {
					switch stake.TxType(tx.Type) {
					case stake.TxTypeRegular:
						vin.Coinbase = true
					case stake.TxTypeSSGen:
						vin.Stakebase = true
					case stake.TxTypeTSpend:
						vin.TreasurySpend = true
					case stake.TxTypeTreasuryBase:
						vin.Treasurybase = true
					}
				}
				tx.Vin = append(tx.Vin, *vin.VinShort)
			}
		} else {
			tx.Vin = []apitypes.VinShort{} // [] instead of null
		}

		// vouts need the tx type to set ticket commitment amount

		txs = append(txs, &tx)
	}

	// vouts
	rows, err = pgb.db.QueryContext(ctx, internal.SelectVoutsForAddress, addr, count, skip)
	if err != nil {
		log.Errorf("GetAddressTransactionsRawWithSkip: SelectVoutsForAddress %s: %v", addr, err)
		return nil
	}
	defer rows.Close()

	for rows.Next() {
		var txid string // funding tx
		var vout apitypes.Vout
		var val int64
		var pkScript []byte
		if err = rows.Scan(&val, &txid, &vout.N, &vout.Version, &pkScript); err != nil {
			log.Errorf("GetAddressTransactionsRawWithSkip: SelectVoutsForAddress %s: %v", addr, err)
			return nil
		}

		if val > 0 {
			vout.Value = dcrutil.Amount(val).ToCoin()
		}

		tx, found := txns[txid]
		if !found {
			log.Errorf("Tx %v for vout not found.", txid)
			continue
		}

		isTicketCommit := stake.TxType(tx.Type) == stake.TxTypeSStx && (vout.N%2 != 0)
		vout.ScriptPubKeyDecoded = decPkScript(vout.Version, pkScript, isTicketCommit, pgb.chainParams)
		tx.Vout = append(tx.Vout, vout)
	}

	for _, tx := range txs {
		sort.Slice(tx.Vout, func(i, j int) bool {
			return tx.Vout[i].N < tx.Vout[j].N
		})
	}

	return txs
}

// GetMempoolPriceCountTime retrieves from mempool: the ticket price, the number
// of tickets in mempool, the time of the first ticket.
func (pgb *ChainDB) GetMempoolPriceCountTime() *apitypes.PriceCountTime {
	return pgb.MPC.GetTicketPriceCountTime(int(pgb.chainParams.MaxFreshStakePerBlock))
}

// GetChainParams is a getter for the current network parameters.
func (pgb *ChainDB) GetChainParams() *chaincfg.Params {
	return pgb.chainParams
}

// GetChainParams is a getter for the current network parameters.
func (pgb *ChainDB) GetBTCChainParams() *btc_chaincfg.Params {
	return pgb.btcChainParams
}

func (pgb *ChainDB) GetLTCChainParams() *ltc_chaincfg.Params {
	return pgb.ltcChainParams
}

// GetBlockVerbose fetches the *chainjson.GetBlockVerboseResult for a given
// block height. Optionally include verbose transactions.
func (pgb *ChainDB) GetBlockVerbose(idx int, verboseTx bool) *chainjson.GetBlockVerboseResult {
	block := rpcutils.GetBlockVerbose(pgb.Client, int64(idx), verboseTx)
	return block
}

func (pgb *ChainDB) GetLTCBlockVerbose(idx int) *ltcjson.GetBlockVerboseResult {
	block := ltcrpcutils.GetBlockVerbose(pgb.LtcClient, int64(idx))
	return block
}

func (pgb *ChainDB) GetLTCBlockVerboseTx(idx int) *ltcjson.GetBlockVerboseTxResult {
	block := ltcrpcutils.GetBlockVerboseTx(pgb.LtcClient, int64(idx))
	return block
}

func (pgb *ChainDB) GetBTCBlockVerbose(idx int) *btcjson.GetBlockVerboseResult {
	block := btcrpcutils.GetBlockVerbose(pgb.BtcClient, int64(idx))
	return block
}

func (pgb *ChainDB) GetDaemonMutilchainBlockHash(idx int64, chainType string) (string, error) {
	switch chainType {
	case mutilchain.TYPELTC:
		hashObj, err := pgb.LtcClient.GetBlockHash(idx)
		if err != nil {
			return "", err
		}
		return hashObj.String(), nil
	case mutilchain.TYPEBTC:
		hashObj, err := pgb.BtcClient.GetBlockHash(idx)
		if err != nil {
			return "", err
		}
		return hashObj.String(), nil
	default:
		return pgb.GetBlockHash(idx)
	}
}

func (pgb *ChainDB) GetBTCBlockVerboseTx(idx int) *btcjson.GetBlockVerboseTxResult {
	block := btcrpcutils.GetBlockVerboseTx(pgb.BtcClient, int64(idx))
	return block
}

func sumOutsTxRawResult(txs []chainjson.TxRawResult) (sum float64) {
	for _, tx := range txs {
		for _, vout := range tx.Vout {
			sum += vout.Value
		}
	}
	return
}

func makeExplorerBlockBasic(data *chainjson.GetBlockVerboseResult, params *chaincfg.Params) *exptypes.BlockBasic {
	index := dbtypes.CalculateWindowIndex(data.Height, params.StakeDiffWindowSize)

	total := sumOutsTxRawResult(data.RawTx) + sumOutsTxRawResult(data.RawSTx)

	numReg := len(data.RawTx)

	block := &exptypes.BlockBasic{
		IndexVal:       index,
		Height:         data.Height,
		Hash:           data.Hash,
		Version:        data.Version,
		Size:           data.Size,
		Valid:          true, // we do not know this, TODO with DB v2
		MainChain:      true,
		Voters:         data.Voters,
		Transactions:   numReg,
		FreshStake:     data.FreshStake,
		Revocations:    uint32(data.Revocations),
		TxCount:        uint32(data.FreshStake+data.Revocations) + uint32(numReg) + uint32(data.Voters),
		BlockTime:      exptypes.NewTimeDefFromUNIX(data.Time),
		FormattedBytes: humanize.Bytes(uint64(data.Size)),
		Total:          total,
		BlockTimeUnix:  data.Time,
	}

	return block
}

func makeBTCExplorerBlockBasic(data *btcjson.GetBlockVerboseTxResult) *exptypes.BlockBasic {
	total := float64(0)
	for _, tx := range data.RawTx {
		for _, vout := range tx.Vout {
			total += vout.Value
		}
	}
	numReg := len(data.RawTx)

	block := &exptypes.BlockBasic{
		Height:         data.Height,
		Hash:           data.Hash,
		Version:        data.Version,
		Size:           data.Size,
		Valid:          true, // we do not know this, TODO with DB v2
		MainChain:      true,
		Transactions:   numReg,
		TxCount:        uint32(numReg),
		BlockTime:      exptypes.NewTimeDefFromUNIX(data.Time),
		BlockTimeUnix:  data.Time,
		FormattedBytes: humanize.Bytes(uint64(data.Size)),
		Total:          total,
	}

	return block
}

func makeLTCExplorerBlockBasicFromTxResult(data *ltcjson.GetBlockVerboseTxResult, params *ltc_chaincfg.Params) *exptypes.BlockBasic {
	total := float64(0)
	for _, tx := range data.RawTx {
		for _, vout := range tx.Vout {
			total += vout.Value
		}
	}
	numReg := len(data.RawTx)

	block := &exptypes.BlockBasic{
		Height:         data.Height,
		Hash:           data.Hash,
		Version:        data.Version,
		Size:           data.Size,
		Valid:          true, // we do not know this, TODO with DB v2
		MainChain:      true,
		Transactions:   numReg,
		TxCount:        uint32(numReg),
		BlockTime:      exptypes.NewTimeDefFromUNIX(data.Time),
		BlockTimeUnix:  data.Time,
		FormattedBytes: humanize.Bytes(uint64(data.Size)),
		Total:          total,
	}

	return block
}

func makeLTCExplorerBlockBasic(data *ltcjson.GetBlockVerboseTxResult) *exptypes.BlockBasic {
	total := float64(0)
	for _, tx := range data.RawTx {
		for _, vout := range tx.Vout {
			total += vout.Value
		}
	}
	numReg := len(data.RawTx)

	block := &exptypes.BlockBasic{
		Height:         data.Height,
		Hash:           data.Hash,
		Version:        data.Version,
		Size:           data.Size,
		Valid:          true, // we do not know this, TODO with DB v2
		MainChain:      true,
		Transactions:   numReg,
		TxCount:        uint32(numReg),
		BlockTime:      exptypes.NewTimeDefFromUNIX(data.Time),
		BlockTimeUnix:  data.Time,
		FormattedBytes: humanize.Bytes(uint64(data.Size)),
		Total:          total,
	}

	return block
}

func makeExplorerTxBasic(data *chainjson.TxRawResult, ticketPrice int64, msgTx *wire.MsgTx, params *chaincfg.Params) (*exptypes.TxBasic, stake.TxType) {
	txType := txhelpers.DetermineTxType(msgTx)

	tx := &exptypes.TxBasic{
		TxID:          data.Txid,
		Type:          txhelpers.TxTypeToString(int(txType)),
		Version:       data.Version,
		FormattedSize: humanize.Bytes(uint64(len(data.Hex) / 2)),
		Total:         txhelpers.TotalVout(data.Vout).ToCoin(),
	}
	tx.Fee, tx.FeeRate = txhelpers.TxFeeRate(msgTx)

	v0 := &data.Vin[0]
	switch {
	case v0.IsCoinBase():
		tx.Fee, tx.FeeRate = 0, 0
		tx.Coinbase = true
	case v0.Treasurybase:
		tx.Treasurybase = true
	case v0.IsTreasurySpend():
		// fmt.Printf("treasury spend: %v\n", data.Txid)
	}

	// Votes need VoteInfo set. Regular txns need to be screened for mixes.
	switch txType {
	case stake.TxTypeSSGen:
		validation, version, bits, choices, tspendVotes, err := txhelpers.SSGenVoteChoices(msgTx, params)
		if err != nil {
			log.Debugf("Cannot get vote choices for %s", tx.TxID)
			return tx, txType
		}
		tx.VoteInfo = &exptypes.VoteInfo{
			Validation: exptypes.BlockValidation{
				Hash:     validation.Hash.String(),
				Height:   validation.Height,
				Validity: validation.Validity,
			},
			Version: version,
			Bits:    bits,
			Choices: choices,
			TSpends: exptypes.ConvertTSpendVotes(tspendVotes),
		}
	case stake.TxTypeRegular:
		_, mixDenom, mixCount := txhelpers.IsMixTx(msgTx)
		if mixCount == 0 {
			_, mixDenom, mixCount = txhelpers.IsMixedSplitTx(msgTx,
				int64(txhelpers.DefaultRelayFeePerKb), ticketPrice)
		}
		tx.MixCount = mixCount
		tx.MixDenom = mixDenom
	}

	return tx, txType
}

func makeBTCExplorerTxBasic(data *btcjson.TxRawResult) *exptypes.TxBasic {
	tx := &exptypes.TxBasic{
		TxID:          data.Txid,
		Version:       int32(data.Version),
		FormattedSize: humanize.Bytes(uint64(len(data.Hex) / 2)),
		Total:         txhelpers.TotalBTCVout(data.Vout).ToBTC(),
	}
	return tx
}

func makeLTCExplorerTxBasic(data *ltcjson.TxRawResult) *exptypes.TxBasic {
	tx := &exptypes.TxBasic{
		TxID:          data.Txid,
		Version:       int32(data.Version),
		FormattedSize: humanize.Bytes(uint64(len(data.Hex) / 2)),
		Total:         txhelpers.TotalLTCVout(data.Vout).ToBTC(),
	}
	return tx
}

func trimmedTxInfoFromMsgTx(txraw *chainjson.TxRawResult, ticketPrice int64, msgTx *wire.MsgTx, params *chaincfg.Params) (*exptypes.TrimmedTxInfo, stake.TxType) {
	txBasic, txType := makeExplorerTxBasic(txraw, ticketPrice, msgTx, params)

	var voteValid bool
	if txBasic.VoteInfo != nil {
		voteValid = txBasic.VoteInfo.Validation.Validity
	}

	tx := &exptypes.TrimmedTxInfo{
		TxBasic:   txBasic,
		Fees:      txBasic.Fee.ToCoin(),
		VinCount:  len(txraw.Vin),
		VoutCount: len(txraw.Vout),
		VoteValid: voteValid,
	}
	return tx, txType
}

func trimmedBTCTxInfoFromMsgTx(txraw *btcjson.TxRawResult, msgTx *btcwire.MsgTx, params *btc_chaincfg.Params) *exptypes.TrimmedTxInfo {
	txBasic := makeBTCExplorerTxBasic(txraw)

	tx := &exptypes.TrimmedTxInfo{
		TxBasic:   txBasic,
		VinCount:  len(txraw.Vin),
		VoutCount: len(txraw.Vout),
	}
	return tx
}

func trimmedLTCTxInfoFromMsgTx(txraw *ltcjson.TxRawResult, msgTx *ltcwire.MsgTx, params *ltc_chaincfg.Params) *exptypes.TrimmedTxInfo {
	txBasic := makeLTCExplorerTxBasic(txraw)

	tx := &exptypes.TrimmedTxInfo{
		TxBasic:   txBasic,
		VinCount:  len(txraw.Vin),
		VoutCount: len(txraw.Vout),
	}
	return tx
}

// BlockSubsidy gets the *chainjson.GetBlockSubsidyResult for the given height
// and number of voters, which can be fewer than the network parameter allows.
func (pgb *ChainDB) BlockSubsidy(height int64, voters uint16) *chainjson.GetBlockSubsidyResult {
	ssv := standalone.SSVOriginal
	if pgb.IsDCP0012Active(height) {
		ssv = standalone.SSVDCP0012
	} else if pgb.IsDCP0010Active(height) {
		ssv = standalone.SSVDCP0010
	}
	work, stake, tax := txhelpers.RewardsAtBlock(height, voters, pgb.chainParams, ssv)
	stake *= int64(voters)
	return &chainjson.GetBlockSubsidyResult{
		PoW:       work,
		PoS:       stake,
		Developer: tax,
		Total:     work + stake + tax,
	}
}

// GetExplorerBlock gets a *exptypes.Blockinfo for the specified block.
func (pgb *ChainDB) GetExplorerBlock(hash string) *exptypes.BlockInfo {
	// This function is quit expensive, and it is used by multiple
	// BlockDataSavers, so remember the BlockInfo generated for this block hash.
	// This also disallows concurrently calling this function for the same block
	// hash.
	pgb.lastExplorerBlock.Lock()
	if pgb.lastExplorerBlock.hash == hash {
		defer pgb.lastExplorerBlock.Unlock()
		return pgb.lastExplorerBlock.blockInfo
	}
	pgb.lastExplorerBlock.Unlock()

	data := pgb.GetBlockVerboseByHash(hash, true)
	if data == nil {
		log.Error("Unable to get block for block hash " + hash)
		return nil
	}

	b := makeExplorerBlockBasic(data, pgb.chainParams)

	// Explorer Block Info
	block := &exptypes.BlockInfo{
		BlockBasic:            b,
		Confirmations:         data.Confirmations,
		PoWHash:               b.Hash,
		StakeRoot:             data.StakeRoot,
		MerkleRoot:            data.MerkleRoot,
		Nonce:                 data.Nonce,
		VoteBits:              data.VoteBits,
		FinalState:            data.FinalState,
		PoolSize:              data.PoolSize,
		Bits:                  data.Bits,
		SBits:                 data.SBits,
		Difficulty:            data.Difficulty,
		ExtraData:             data.ExtraData,
		StakeVersion:          data.StakeVersion,
		PreviousHash:          data.PreviousHash,
		NextHash:              data.NextHash,
		StakeValidationHeight: pgb.chainParams.StakeValidationHeight,
		Subsidy:               pgb.BlockSubsidy(b.Height, b.Voters),
	}

	if data.PoWHash != "" {
		block.PoWHash = data.PoWHash
	} else if pgb.IsDCP0011Active(b.Height) {
		blockChainHash, err := chainhash.NewHashFromStr(data.Hash)
		if err != nil {
			log.Errorf("error parsing hash for block %d (hash=%s): %v", data.Height, data.Hash, err)
			return nil
		}

		// Get the block header
		header, err := pgb.Client.GetBlockHeader(pgb.ctx, blockChainHash)
		if err != nil {
			log.Errorf("failed to fetch header for block hash %s: %v", data.Hash, err)
			return nil
		}

		block.PoWHash = header.PowHashV2().String()
	}

	votes := make([]*exptypes.TrimmedTxInfo, 0, block.Voters)
	revocations := make([]*exptypes.TrimmedTxInfo, 0, block.Revocations)
	tickets := make([]*exptypes.TrimmedTxInfo, 0, block.FreshStake)

	var treasury []*exptypes.TrimmedTxInfo
	// treasuryActive := txhelpers.IsTreasuryActive(pgb.chainParams.Net, b.Height)

	sbits, _ := dcrutil.NewAmount(block.SBits) // sbits==0 for err!=nil
	ticketPrice := int64(sbits)

	for i := range data.RawSTx {
		tx := &data.RawSTx[i]
		msgTx, err := txhelpers.MsgTxFromHex(tx.Hex)
		if err != nil {
			log.Errorf("Unknown transaction %s: %v", tx.Txid, err)
			return nil
		}
		stx, txType := trimmedTxInfoFromMsgTx(tx, ticketPrice, msgTx, pgb.chainParams)
		switch txType {
		case stake.TxTypeSSGen:
			// Fees for votes should be zero, but if the transaction was created
			// with unmatched inputs/outputs then the remainder becomes a fee.
			// Account for this possibility by calculating the fee for votes as
			// well.
			if stx.Fee > 0 {
				log.Tracef("Vote with fee: %d atoms, %.8f DCR", int64(stx.Fee), stx.Fees)
			}
			votes = append(votes, stx)
		case stake.TxTypeSStx:
			tickets = append(tickets, stx)
		case stake.TxTypeSSRtx:
			revocations = append(revocations, stx)
		case stake.TxTypeTAdd, stake.TxTypeTSpend, stake.TxTypeTreasuryBase:
			treasury = append(treasury, stx)
		}
	}

	var totalMixed int64

	txs := make([]*exptypes.TrimmedTxInfo, 0, block.Transactions)
	txIds := make([]string, 0)
	for i := range data.RawTx {
		tx := &data.RawTx[i]
		msgTx, err := txhelpers.MsgTxFromHex(tx.Hex)
		if err != nil {
			log.Errorf("Unknown transaction %s: %v", tx.Txid, err)
			return nil
		}

		exptx, _ := trimmedTxInfoFromMsgTx(tx, ticketPrice, msgTx, pgb.chainParams) // maybe pass tree
		for i := range tx.Vin {
			if tx.Vin[i].IsCoinBase() {
				exptx.Fee, exptx.FeeRate, exptx.Fees = 0.0, 0.0, 0.0
			}
		}
		// check swaps tx
		exptx.SwapsType = pgb.GetSwapType(exptx.TxID)
		if exptx.SwapsType != "" {
			exptx.SwapsTypeDisplay = utils.GetSwapTypeDisplay(exptx.SwapsType)
		}
		txs = append(txs, exptx)
		txIds = append(txIds, exptx.TxID)
		totalMixed += int64(exptx.MixCount) * exptx.MixDenom
	}

	block.Tx = txs
	block.Txids = txIds
	block.Treasury = treasury
	block.Votes = votes
	block.Revs = revocations
	block.Tickets = tickets
	block.TotalMixed = totalMixed

	sortTx := func(txs []*exptypes.TrimmedTxInfo) {
		sort.Slice(txs, func(i, j int) bool {
			return txs[i].Total > txs[j].Total
		})
	}

	sortTx(block.Tx)
	sortTx(block.Treasury)
	sortTx(block.Votes)
	sortTx(block.Revs)
	sortTx(block.Tickets)

	getTotalFee := func(txs []*exptypes.TrimmedTxInfo) (total dcrutil.Amount) {
		for _, tx := range txs {
			// Coinbase transactions have no fee. The fee should be zero already
			// (as in makeExplorerTxBasic), but intercept coinbase just in case.
			// Note that this does not include stakebase transactions (votes),
			// which can have a fee but are not required to.
			if tx.Coinbase {
				continue
			}
			if tx.Fee < 0 {
				log.Warnf("Negative fees should not happen! %v, %v", tx.TxID, tx.Fee)
			}
			total += tx.Fee
		}
		return
	}
	getTotalSent := func(txs []*exptypes.TrimmedTxInfo) (total dcrutil.Amount) {
		for _, tx := range txs {
			amt, err := dcrutil.NewAmount(tx.Total)
			if err != nil {
				continue
			}
			total += amt
		}
		return
	}
	block.TotalSent = (getTotalSent(block.Tx) + getTotalSent(block.Treasury) + getTotalSent(block.Revs) +
		getTotalSent(block.Tickets) + getTotalSent(block.Votes)).ToCoin()
	block.MiningFee = (getTotalFee(block.Tx) + getTotalFee(block.Treasury) + getTotalFee(block.Revs) +
		getTotalFee(block.Tickets) + getTotalFee(block.Votes)).ToCoin()

	pgb.lastExplorerBlock.Lock()
	pgb.lastExplorerBlock.hash = hash
	pgb.lastExplorerBlock.blockInfo = block
	pgb.lastExplorerBlock.difficulties = make(map[int64]float64) // used by the Difficulty method
	pgb.lastExplorerBlock.Unlock()

	return block
}

// GetBlockSwapGroupFullData return group swaps list from block txs
func (pgb *ChainDB) GetBlockSwapGroupFullData(blockTxs []string) ([]*dbtypes.AtomicSwapFullData, error) {
	result := make([]*dbtypes.SimpleGroupInfo, 0)
	rows, err := pgb.db.QueryContext(pgb.ctx, internal.SelectGroupTxsFromTxs, pq.Array(blockTxs))
	if err != nil {
		log.Errorf("Get group txs from block txs failed: %v", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var groupTx string
		var targetToken sql.NullString
		err = rows.Scan(&groupTx, &targetToken)
		if err != nil {
			return nil, err
		}
		targetTokenString := ""
		if targetToken.Valid {
			targetTokenString = targetToken.String
		}
		result = append(result, &dbtypes.SimpleGroupInfo{
			ContractTx:  groupTx,
			TargetToken: targetTokenString,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return pgb.GetSwapDataByContractTxs(result)
}

// GetBlockSwapGroupFullData return group swaps list from block txs
func (pgb *ChainDB) GetMultichainBlockSwapGroupFullData(blockTxs []string, chainType string) ([]*dbtypes.AtomicSwapFullData, error) {
	result := make([]*dbtypes.SimpleGroupInfo, 0)
	rows, err := pgb.db.QueryContext(pgb.ctx, fmt.Sprintf(internal.SelectMultichainGroupTxsFromTxs, chainType), pq.Array(blockTxs))
	if err != nil {
		log.Errorf("Get group txs from block txs failed: %v", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var groupTx string
		err = rows.Scan(&groupTx)
		if err != nil {
			return nil, err
		}
		result = append(result, &dbtypes.SimpleGroupInfo{
			ContractTx:  groupTx,
			TargetToken: chainType,
		})
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return pgb.GetSwapDataByContractTxs(result)
}

func (pgb *ChainDB) GetSwapFullData(txid, swapType string) ([]*dbtypes.AtomicSwapFullData, error) {
	result := make([]*dbtypes.SimpleGroupInfo, 0)
	var query string
	if swapType != utils.CONTRACT_TYPE {
		query = internal.SelectGroupTxBySpendTx
	} else {
		query = internal.SelectTargetTokenOfContract
	}

	// get contract simple info
	var targetToken sql.NullString
	var groupTx string
	err := pgb.db.QueryRow(query, txid).Scan(&targetToken, &groupTx)
	if err != nil {
		return nil, err
	}
	targetTokenString := ""
	if targetToken.Valid {
		targetTokenString = targetToken.String
	}
	result = append(result, &dbtypes.SimpleGroupInfo{
		ContractTx:  groupTx,
		TargetToken: targetTokenString,
	})
	return pgb.GetSwapDataByContractTxs(result)
}

// GetMultichainSwapType return multichain swap type
func (pgb *ChainDB) GetMultichainSwapType(txid, chainType string) (string, error) {
	// check swapType is empty
	var swapType string
	err := pgb.db.QueryRow(fmt.Sprintf(internal.SelectMultichainSwapType, chainType), txid).Scan(&swapType)
	if err != nil {
		return "", err
	}
	if swapType == "" {
		return "", fmt.Errorf("GetMultichainSwapInfoData get swap type failed")
	}
	return swapType, nil
}

// GetMultichainSwapInfoData return swap info data for multichain
func (pgb *ChainDB) GetMultichainSwapInfoData(txid, chainType string) (swapsInfo *txhelpers.TxAtomicSwaps, err error) {
	// check swapType is empty
	var swapType string
	swapType, err = pgb.GetMultichainSwapType(txid, chainType)
	if err != nil {
		return nil, err
	}
	// Get contract txs with
	rows, err := pgb.db.QueryContext(pgb.ctx, fmt.Sprintf(internal.SelectMultichainSwapInfoRows, chainType), txid)
	if err != nil {
		return nil, err
	}
	swapsInfo = &txhelpers.TxAtomicSwaps{
		Found: utils.GetSwapTypeFound(swapType),
	}
	swapInfoMap := make(map[uint32]*txhelpers.AtomicSwap)
	defer rows.Close()
	for rows.Next() {
		var infoRow dbtypes.TokenAtomicSwapData
		var dcrContractTx string
		var scretHash []byte
		err = rows.Scan(&infoRow.ContractTx, &dcrContractTx, &infoRow.ContractVout, &infoRow.SpendTx,
			&infoRow.SpendVin, &infoRow.SpendHeight, &infoRow.ContractAddress, &infoRow.Value, &scretHash, &infoRow.Secret,
			&infoRow.Locktime)
		if err != nil {
			return
		}
		var mapIndex uint32
		if swapType == utils.CONTRACT_TYPE {
			mapIndex = infoRow.ContractVout
		} else {
			mapIndex = infoRow.SpendVin
		}
		amount := btcutil.Amount(infoRow.Value)
		contractAddr, recipientAddr, refundAddr, contractScript, isRefund, cInfoerr := pgb.GetMultichainContractInfo(infoRow.SpendTx, chainType, infoRow.SpendVin)
		if cInfoerr != nil {
			err = cInfoerr
			return
		}
		swapInfoMap[mapIndex] = &txhelpers.AtomicSwap{
			ContractTxRef:     infoRow.ContractTx,
			Contract:          fmt.Sprintf("%x", contractScript),
			ContractValue:     amount.ToBTC(),
			ContractAddress:   contractAddr,
			RecipientAddress:  recipientAddr,
			RefundAddress:     refundAddr,
			Locktime:          infoRow.Locktime,
			SecretHash:        hex.EncodeToString(scretHash),
			Secret:            hex.EncodeToString(infoRow.Secret),
			FormattedLocktime: time.Unix(infoRow.Locktime, 0).UTC().Format(utils.TimeFmt),
			IsRefund:          isRefund,
			SpendTxInput:      fmt.Sprintf("%s:%d", infoRow.SpendTx, infoRow.SpendVin),
		}
		// get contract data
	}
	err = rows.Err()
	switch swapType {
	case utils.CONTRACT_TYPE:
		swapsInfo.Contracts = swapInfoMap
	case utils.REDEMPTION_TYPE:
		swapsInfo.Redemptions = swapInfoMap
	case utils.REFUND_TYPE:
		swapsInfo.Refunds = swapInfoMap
	}
	return
}

func (pgb *ChainDB) GetMultichainContractInfo(spendTx, chainType string, spendVin uint32) (contractAddr, recipientAddr,
	refundAddr string, contractScript []byte, isRefund bool, err error) {
	switch chainType {
	case mutilchain.TYPEBTC:
		return pgb.GetBTCContractInfo(spendTx, spendVin)
	case mutilchain.TYPELTC:
		return pgb.GetLTCContractInfo(spendTx, spendVin)
	}
	return
}

func (pgb *ChainDB) GetBTCContractInfo(spendTx string, spendVin uint32) (contractAddr, recipientAddr,
	refundAddr string, contractScript []byte, isRefund bool, err error) {
	var tx *btcutil.Tx
	tx, err = pgb.GetBTCTransactionByHash(spendTx)
	if err != nil {
		return
	}
	if len(tx.MsgTx().TxIn) <= int(spendVin) {
		err = fmt.Errorf("BTC: spend vin invalid")
		return
	}
	var contractData *btctxhelper.AtomicSwapContractPushes
	contractData, contractScript, _, isRefund, err = btctxhelper.ExtractSwapDataFromWitness(tx.MsgTx().TxIn[spendVin].Witness, pgb.btcChainParams)
	if err != nil {
		return
	}
	contractAddr = contractData.ContractAddress.String()
	recipientAddr = contractData.RecipientAddress.String()
	refundAddr = contractData.RefundAddress.String()
	return
}

func (pgb *ChainDB) GetLTCContractInfo(spendTx string, spendVin uint32) (contractAddr, recipientAddr,
	refundAddr string, contractScript []byte, isRefund bool, err error) {
	var tx *ltcutil.Tx
	tx, err = pgb.GetLTCTransactionByHash(spendTx)
	if err != nil {
		return
	}
	if len(tx.MsgTx().TxIn) <= int(spendVin) {
		err = fmt.Errorf("LTC: spend vin invalid")
		return
	}
	var contractData *ltctxhelper.AtomicSwapContractPushes
	contractData, contractScript, _, isRefund, err = ltctxhelper.ExtractSwapDataFromWitness(tx.MsgTx().TxIn[spendVin].Witness, pgb.ltcChainParams)
	if err != nil {
		return
	}
	contractAddr = contractData.ContractAddress.String()
	recipientAddr = contractData.RecipientAddress.String()
	refundAddr = contractData.RefundAddress.String()
	return
}

// GetMultichainSwapFullData return swap full data with multichain spendtx/contractx
func (pgb *ChainDB) GetMultichainSwapFullData(txid, swapTypeInput, chainType string) (*dbtypes.AtomicSwapFullData, string, error) {
	// check swapType is empty
	swapType := swapTypeInput
	if swapType == "" {
		err := pgb.db.QueryRow(fmt.Sprintf(internal.SelectMultichainSwapType, chainType), txid).Scan(&swapType)
		if err != nil {
			return nil, "", err
		}
	}
	result := make([]*dbtypes.SimpleGroupInfo, 0)
	var query string
	if swapType == utils.CONTRACT_TYPE {
		query = fmt.Sprintf(internal.SelectDecredContractTxsFromMultichainContractTx, chainType)
	} else {
		query = fmt.Sprintf(internal.SelectDecredContractTxsFromMultichainSpendTx, chainType)
	}
	// get contract tx list from spend_tx
	var groupTx string
	err := pgb.db.QueryRow(query, txid).Scan(&groupTx)
	if err != nil {
		return nil, "", err
	}
	// check isRefund of decred contract tx
	result = append(result, &dbtypes.SimpleGroupInfo{
		ContractTx:  groupTx,
		TargetToken: chainType,
	})
	swaps, err := pgb.GetSwapDataByContractTxs(result)
	if err != nil {
		return nil, "", err
	}
	if len(swaps) == 0 {
		return nil, "", fmt.Errorf("Get swaps info failed")
	}
	return swaps[0], swapType, nil
}

func (pgb *ChainDB) GetSwapDataByContractTxs(groupInfos []*dbtypes.SimpleGroupInfo) (result []*dbtypes.AtomicSwapFullData, err error) {
	for _, contractTx := range groupInfos {
		var swapItem *dbtypes.AtomicSwapFullData
		swapItem, err = pgb.GetContractSwapDataByGroup(contractTx.ContractTx, contractTx.TargetToken)
		if err != nil {
			return
		}
		result = append(result, swapItem)
	}
	return
}

func (pgb *ChainDB) GetSwapType(txid string) string {
	var isContract, isTarget, isRefund bool
	err := pgb.db.QueryRow(internal.CheckSwapsType, txid).Scan(&isContract, &isTarget, &isRefund)
	if err != nil {
		return ""
	}
	if isContract {
		return utils.CONTRACT_TYPE
	}
	if !isTarget {
		return ""
	}
	if isTarget && isRefund {
		return utils.REFUND_TYPE
	}
	return utils.REDEMPTION_TYPE
}

// GetLTCExplorerBlock gets a *exptypes.Blockinfo for the specified ltc block.
func (pgb *ChainDB) GetLTCExplorerBlock(hash string) *exptypes.BlockInfo {
	pgb.ltcLastExplorerBlock.Lock()
	if pgb.ltcLastExplorerBlock.hash == hash {
		defer pgb.ltcLastExplorerBlock.Unlock()
		return pgb.ltcLastExplorerBlock.blockInfo
	}
	pgb.ltcLastExplorerBlock.Unlock()

	data := pgb.GetLTCBlockVerboseTxByHash(hash)
	if data == nil {
		log.Error("Unable to get block for block hash " + hash)
		return nil
	}

	b := makeLTCExplorerBlockBasicFromTxResult(data, pgb.ltcChainParams)

	// Explorer Block Info
	block := &exptypes.BlockInfo{
		BlockBasic:    b,
		Confirmations: data.Confirmations,
		PoWHash:       b.Hash,
		Nonce:         data.Nonce,
		Bits:          data.Bits,
		Difficulty:    data.Difficulty,
		PreviousHash:  data.PreviousHash,
		NextHash:      data.NextHash,
	}

	txs := make([]*exptypes.TrimmedTxInfo, 0, block.Transactions)
	txids := make([]string, 0)
	for i := range data.RawTx {
		tx := &data.RawTx[i]
		msgTx, err := txhelpers.MsgLTCTxFromHex(tx.Hex, int32(tx.Version))
		if err != nil {
			log.Errorf("Unknown transaction %s: %v", tx.Txid, err)
			return nil
		}

		exptx := trimmedLTCTxInfoFromMsgTx(tx, msgTx, pgb.ltcChainParams) // maybe pass tree
		txs = append(txs, exptx)
		txids = append(txids, exptx.TxID)
	}
	block.Tx = txs
	block.Txids = txids
	sortTx := func(txs []*exptypes.TrimmedTxInfo) {
		sort.Slice(txs, func(i, j int) bool {
			return txs[i].Total > txs[j].Total
		})
	}

	sortTx(block.Tx)

	getTotalSent := func(txs []*exptypes.TrimmedTxInfo) (total ltcutil.Amount) {
		for _, tx := range txs {
			amt, err := ltcutil.NewAmount(tx.Total)
			if err != nil {
				continue
			}
			total += amt
		}
		return
	}
	block.TotalSent = getTotalSent(block.Tx).ToBTC()

	pgb.ltcLastExplorerBlock.Lock()
	pgb.ltcLastExplorerBlock.hash = hash
	pgb.ltcLastExplorerBlock.blockInfo = block
	pgb.ltcLastExplorerBlock.difficulties = make(map[int64]float64) // used by the Difficulty method
	pgb.ltcLastExplorerBlock.Unlock()
	swapsData, err := pgb.GetMultichainBlockSwapGroupFullData(block.Txids, mutilchain.TYPELTC)
	if err != nil {
		log.Errorf("%s: Get swaps full data for block txs failed: %v", mutilchain.TYPELTC, err)
		block.GroupSwaps = make([]*dbtypes.AtomicSwapFullData, 0)
	} else {
		block.GroupSwaps = swapsData
	}
	return block
}

func (pgb *ChainDB) GetMutilchainExplorerBlock(hash, chainType string) *exptypes.BlockInfo {
	var blockInfo *exptypes.BlockInfo
	switch chainType {
	case mutilchain.TYPEBTC:
		blockInfo = pgb.GetBTCExplorerBlock(hash)
	case mutilchain.TYPELTC:
		blockInfo = pgb.GetLTCExplorerBlock(hash)
	default:
		return &exptypes.BlockInfo{}
	}
	return blockInfo
}

// GetBTCExplorerBlock gets a *exptypes.Blockinfo for the specified btc block.
func (pgb *ChainDB) GetBTCExplorerBlock(hash string) *exptypes.BlockInfo {
	pgb.btcLastExplorerBlock.Lock()
	if pgb.btcLastExplorerBlock.hash == hash {
		defer pgb.btcLastExplorerBlock.Unlock()
		return pgb.btcLastExplorerBlock.blockInfo
	}
	pgb.btcLastExplorerBlock.Unlock()

	data := pgb.GetBTCBlockVerboseTxByHash(hash)
	if data == nil {
		log.Error("Unable to get block for block hash " + hash)
		return nil
	}

	b := makeBTCExplorerBlockBasic(data)

	// Explorer Block Info
	block := &exptypes.BlockInfo{
		BlockBasic:    b,
		Confirmations: data.Confirmations,
		PoWHash:       b.Hash,
		Nonce:         data.Nonce,
		Bits:          data.Bits,
		Difficulty:    data.Difficulty,
		PreviousHash:  data.PreviousHash,
		NextHash:      data.NextHash,
	}

	txs := make([]*exptypes.TrimmedTxInfo, 0, block.Transactions)
	txIds := make([]string, 0)
	for i := range data.RawTx {
		tx := &data.RawTx[i]
		msgTx, err := txhelpers.MsgBTCTxFromHex(tx.Hex, int32(tx.Version))
		if err != nil {
			log.Errorf("Unknown transaction %s: %v", tx.Txid, err)
			return nil
		}

		exptx := trimmedBTCTxInfoFromMsgTx(tx, msgTx, pgb.btcChainParams) // maybe pass tree
		txs = append(txs, exptx)
		txIds = append(txIds, exptx.TxID)
	}
	block.Tx = txs
	block.Txids = txIds
	sortTx := func(txs []*exptypes.TrimmedTxInfo) {
		sort.Slice(txs, func(i, j int) bool {
			return txs[i].Total > txs[j].Total
		})
	}

	sortTx(block.Tx)

	getTotalSent := func(txs []*exptypes.TrimmedTxInfo) (total btcutil.Amount) {
		for _, tx := range txs {
			amt, err := btcutil.NewAmount(tx.Total)
			if err != nil {
				continue
			}
			total += amt
		}
		return
	}
	block.TotalSent = getTotalSent(block.Tx).ToBTC()

	pgb.btcLastExplorerBlock.Lock()
	pgb.btcLastExplorerBlock.hash = hash
	pgb.btcLastExplorerBlock.blockInfo = block
	pgb.btcLastExplorerBlock.difficulties = make(map[int64]float64) // used by the Difficulty method
	pgb.btcLastExplorerBlock.Unlock()
	swapsData, err := pgb.GetMultichainBlockSwapGroupFullData(block.Txids, mutilchain.TYPEBTC)
	if err != nil {
		log.Errorf("%s: Get swaps full data for block txs failed: %v", mutilchain.TYPEBTC, err)
		block.GroupSwaps = make([]*dbtypes.AtomicSwapFullData, 0)
	} else {
		block.GroupSwaps = swapsData
	}
	return block
}

// GetExplorerBlocks creates an slice of exptypes.BlockBasic beginning at start
// and decreasing in block height to end, not including end.
func (pgb *ChainDB) GetLTCExplorerBlocks(start int, end int) []*exptypes.BlockBasic {
	if start < end {
		return nil
	}
	summaries := make([]*exptypes.BlockBasic, 0, start-end)
	for i := start; i > end; i-- {
		data := pgb.GetLTCBlockVerboseTx(i)
		block := new(exptypes.BlockBasic)
		if data != nil {
			block = makeLTCExplorerBlockBasic(data)
		}
		summaries = append(summaries, block)
	}
	return summaries
}

// GetExplorerBlocks creates an slice of exptypes.BlockBasic beginning at start
// and decreasing in block height to end, not including end.
func (pgb *ChainDB) GetBTCExplorerBlocks(start int, end int) []*exptypes.BlockBasic {
	if start < end {
		return nil
	}
	summaries := make([]*exptypes.BlockBasic, 0, start-end)
	for i := start; i > end; i-- {
		data := pgb.GetBTCBlockVerboseTx(i)
		block := new(exptypes.BlockBasic)
		if data != nil {
			block = makeBTCExplorerBlockBasic(data)
		}
		summaries = append(summaries, block)
	}
	return summaries
}

// GetExplorerBlocks creates an slice of exptypes.BlockBasic beginning at start
// and decreasing in block height to end, not including end.
func (pgb *ChainDB) GetExplorerBlocks(start int, end int) []*exptypes.BlockBasic {
	if start < end {
		return nil
	}
	summaries := make([]*exptypes.BlockBasic, 0, start-end)
	for i := start; i > end; i-- {
		data := pgb.GetBlockVerbose(i, true)
		block := new(exptypes.BlockBasic)
		if data != nil {
			block = makeExplorerBlockBasic(data, pgb.chainParams)
		}
		summaries = append(summaries, block)
	}
	return summaries
}

// GetExplorerBlockBasic return block basic information by height
func (pgb *ChainDB) GetExplorerBlockBasic(height int) *exptypes.BlockBasic {
	data := pgb.GetBlockVerbose(height, true)
	block := new(exptypes.BlockBasic)
	if data != nil {
		block = makeExplorerBlockBasic(data, pgb.chainParams)
	}
	return block
}

// txWithTicketPrice is a way to perform getrawtransaction and if the
// transaction is unconfirmed, getstakedifficulty, while the chain server's best
// block remains unchanged. If the transaction is confirmed, the ticket price is
// queryied from ChainDB's database. This is an ugly solution to atomic RPCs.
func (pgb *ChainDB) txWithTicketPrice(txhash *chainhash.Hash) (*chainjson.TxRawResult, int64, error) {
	// If the transaction is unconfirmed, the RPC client must provide the ticket
	// price. Ensure the best block does not change between calls to
	// getrawtransaction and getstakedifficulty.
	ctx, cancel := context.WithTimeout(pgb.ctx, 10*time.Second)
	defer cancel()
	blockHash, _, err := pgb.Client.GetBestBlock(ctx)
	if err != nil {
		return nil, 0, fmt.Errorf("GetBestBlock failed: %w", err)
	}

	var txraw *chainjson.TxRawResult
	var ticketPrice int64
	for {
		txraw, err = pgb.Client.GetRawTransactionVerbose(ctx, txhash)
		if err != nil {
			return nil, 0, fmt.Errorf("GetRawTransactionVerbose failed for %v: %w", txhash, err)
		}

		if txraw.Confirmations > 0 {
			return txraw, pgb.GetSBitsByHash(txraw.BlockHash), nil
		}

		sdiffRes, err := pgb.Client.GetStakeDifficulty(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("GetStakeDifficulty failed: %w", err)
		}

		blockHash1, _, err := pgb.Client.GetBestBlock(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("GetBestBlock failed: %w", err)
		}

		sdiff, _ := dcrutil.NewAmount(sdiffRes.CurrentStakeDifficulty) // sdiff==0 for err !=nil
		ticketPrice = int64(sdiff)

		if blockHash.IsEqual(blockHash1) {
			break
		}
		blockHash = blockHash1 // try again
	}

	return txraw, ticketPrice, nil
}

func (pgb *ChainDB) BtcTxResult(txhash *btc_chainhash.Hash) (*btcjson.TxRawResult, int64, error) {
	txraw, err := pgb.BtcClient.GetRawTransactionVerbose(txhash)
	if err != nil {
		return nil, 0, fmt.Errorf("BTC: GetRawTransactionVerbose failed for %v: %w", txhash, err)
	}
	var blockHeight int64
	if txraw.BlockHash != "" {
		blockHeight, _ = pgb.GetMutilchainBlockHeightByHash(txraw.BlockHash, mutilchain.TYPEBTC)
	}
	return txraw, blockHeight, nil
}

func (pgb *ChainDB) LtcTxResult(txhash *ltc_chainhash.Hash) (*ltcjson.TxRawResult, int64, error) {
	txraw, err := pgb.LtcClient.GetRawTransactionVerbose(txhash)
	if err != nil {
		return nil, 0, fmt.Errorf("LTC: GetRawTransactionVerbose failed for %v: %w", txhash, err)
	}
	var blockHeight int64
	if txraw.BlockHash != "" {
		blockHeight, _ = pgb.GetMutilchainBlockHeightByHash(txraw.BlockHash, mutilchain.TYPELTC)
	}
	return txraw, blockHeight, nil
}

func (pgb *ChainDB) GetLTCExplorerTx(txid string) *exptypes.TxInfo {
	if pgb.LtcClient == nil {
		return &exptypes.TxInfo{}
	}
	txhash, err := ltc_chainhash.NewHashFromStr(txid)
	if err != nil {
		log.Errorf("Invalid transaction hash %s", txid)
		return nil
	}

	txraw, blockHeight, err := pgb.LtcTxResult(txhash)
	if err != nil {
		log.Errorf("Mutilchain Tx Info: %v", err)
		return nil
	}

	msgTx, err := txhelpers.LTCMsgTxFromHex(txraw.Hex, int32(txraw.Version))
	if err != nil {
		log.Errorf("Cannot create MsgTx for tx %v: %v", txhash, err)
		return nil
	}

	txBasic := makeLTCExplorerTxBasic(txraw)
	tx := &exptypes.TxInfo{
		TxBasic:       txBasic,
		BlockHash:     txraw.BlockHash,
		BlockHeight:   blockHeight,
		Confirmations: int64(txraw.Confirmations),
		Time:          exptypes.NewTimeDefFromUNIX(txraw.Time),
	}

	// tree := txType stake.TxTypeRegular
	var totalVin float64
	inputs := make([]exptypes.MutilchainVin, 0, len(txraw.Vin))
	for i := range txraw.Vin {
		vin := &txraw.Vin[i]
		// The addresses are may only be obtained by decoding the previous
		// output's pkscript.
		var addresses []string
		// The vin amount is now correct in most cases, but get it from the
		// previous output anyway and compare the values for information.
		valueIn, _ := ltcutil.NewAmount(float64(vin.Vout))
		// Do not attempt to look up prevout if it is a coinbase or stakebase
		// input, which does not spend a previous output.
		prevOut := &msgTx.TxIn[i].PreviousOutPoint
		if !txhelpers.IsLTCZeroHash(prevOut.Hash) {
			// Store the vin amount for comparison.
			valueIn0 := valueIn

			addresses, valueIn, err = txhelpers.LTCOutPointAddresses(
				prevOut, pgb.LtcClient, pgb.ltcChainParams)
			if err != nil {
				log.Warnf("Failed to get outpoint address from txid: %v", err)
				continue
			}
			// See if getrawtransaction had correct vin amounts. It should
			// except for votes on side chain blocks.
			if valueIn != valueIn0 {
				log.Debugf("vin amount in: prevout RPC = %v, vin's amount = %v",
					valueIn, valueIn0)
			}
		}

		// Assemble and append this vin.
		coinIn := valueIn.ToBTC()
		totalVin += coinIn
		vinBlockHeight := int64(0)
		vinHash, err := ltc_chainhash.NewHashFromStr(vin.Txid)
		if err != nil {
			log.Errorf("LTC: Invalid vin transaction hash %s", vin.Txid)
		} else {
			txraw, err := pgb.LtcClient.GetRawTransactionVerbose(vinHash)
			if err != nil {
				log.Errorf("LTC: GetRawTransactionVerbose failed for %v: %w", vinHash, err)
			} else {
				vinBlockHeight, _ = pgb.GetMutilchainBlockHeightByHash(txraw.BlockHash, mutilchain.TYPELTC)
			}
		}
		inputs = append(inputs, exptypes.MutilchainVin{
			Txid:            vin.Txid,
			Coinbase:        vin.Coinbase,
			Vout:            vin.Vout,
			Sequence:        vin.Sequence,
			Witness:         vin.Witness,
			Addresses:       addresses,
			FormattedAmount: humanize.Commaf(coinIn),
			Index:           uint32(i),
			AmountIn:        coinIn,
			BlockHeight:     vinBlockHeight,
		})
	}
	tx.MutilchainVin = inputs

	CoinbaseMaturityInHours := (pgb.ltcChainParams.TargetTimePerBlock.Hours() * float64(pgb.ltcChainParams.CoinbaseMaturity))
	tx.MaturityTimeTill = ((float64(pgb.ltcChainParams.CoinbaseMaturity) -
		float64(tx.Confirmations)) / float64(pgb.ltcChainParams.CoinbaseMaturity)) * CoinbaseMaturityInHours

	outputs := make([]exptypes.Vout, 0, len(txraw.Vout))
	var totalVout float64
	for i, vout := range txraw.Vout {
		// Determine spent status with gettxout, including mempool.
		txout, err := pgb.LtcClient.GetTxOut(txhash, uint32(i), true)
		if err != nil {
			log.Warnf("Failed to determine if tx out is spent for output %d of tx %s: %v", i, txid, err)
		}
		var opReturn string
		var opTAdd bool
		if strings.HasPrefix(vout.ScriptPubKey.Asm, "OP_RETURN") {
			opReturn = vout.ScriptPubKey.Asm
		} else {
			opTAdd = strings.HasPrefix(vout.ScriptPubKey.Asm, "OP_TADD")
		}
		// Get a consistent script class string from dbtypes.ScriptClass.
		outputs = append(outputs, exptypes.Vout{
			Addresses:       vout.ScriptPubKey.Addresses,
			Amount:          vout.Value,
			FormattedAmount: humanize.Commaf(vout.Value),
			OP_RETURN:       opReturn,
			OP_TADD:         opTAdd,
			Spent:           txout == nil,
			Index:           vout.N,
		})
		totalVout += vout.Value
	}
	tx.Vout = outputs

	// Initialize the spending transaction slice for safety.
	tx.SpendingTxns = make([]exptypes.TxInID, len(outputs))
	tx.FeeCoin = totalVin - totalVout
	return tx
}

func (pgb *ChainDB) GetMutilchainMempoolTxTime(txid string, chainType string) int64 {
	switch chainType {
	case mutilchain.TYPEBTC:
		mempoolTxsMap, err := pgb.BtcClient.GetRawMempoolVerbose()
		if err == nil {
			mempoolTx, exist := mempoolTxsMap[txid]
			if exist {
				return mempoolTx.Time
			}
		}
	case mutilchain.TYPELTC:
		mempoolTxsMap, err := pgb.LtcClient.GetRawMempoolVerbose()
		if err == nil {
			mempoolTx, exist := mempoolTxsMap[txid]
			if exist {
				return mempoolTx.Time
			}
		}
	}
	return int64(0)
}

func (pgb *ChainDB) GetBTCExplorerTx(txid string) *exptypes.TxInfo {
	if pgb.BtcClient == nil {
		return &exptypes.TxInfo{}
	}
	txhash, err := btc_chainhash.NewHashFromStr(txid)
	if err != nil {
		log.Errorf("Invalid transaction hash %s", txid)
		return nil
	}

	txraw, blockHeight, err := pgb.BtcTxResult(txhash)
	if err != nil {
		log.Errorf("Mutilchain Tx Info: %v", err)
		return nil
	}

	msgTx, err := txhelpers.BTCMsgTxFromHex(txraw.Hex, int32(txraw.Version))
	if err != nil {
		log.Errorf("Cannot create MsgTx for tx %v: %v", txhash, err)
		return nil
	}

	txBasic := makeBTCExplorerTxBasic(txraw)
	tx := &exptypes.TxInfo{
		TxBasic:       txBasic,
		BlockHash:     txraw.BlockHash,
		BlockHeight:   blockHeight,
		Confirmations: int64(txraw.Confirmations),
		Time:          exptypes.NewTimeDefFromUNIX(txraw.Time),
	}

	// tree := txType stake.TxTypeRegular
	var totalVin float64
	inputs := make([]exptypes.MutilchainVin, 0, len(txraw.Vin))
	for i := range txraw.Vin {
		vin := &txraw.Vin[i]
		// The addresses are may only be obtained by decoding the previous
		// output's pkscript.
		var addresses []string
		// The vin amount is now correct in most cases, but get it from the
		// previous output anyway and compare the values for information.
		valueIn, _ := btcutil.NewAmount(float64(vin.Vout))
		// Do not attempt to look up prevout if it is a coinbase or stakebase
		// input, which does not spend a previous output.
		prevOut := &msgTx.TxIn[i].PreviousOutPoint
		if !txhelpers.IsBTCZeroHash(prevOut.Hash) {
			// Store the vin amount for comparison.
			valueIn0 := valueIn

			addresses, valueIn, err = txhelpers.BTCOutPointAddresses(
				prevOut, pgb.BtcClient, pgb.btcChainParams)
			if err != nil {
				log.Warnf("Failed to get outpoint address from txid: %v", err)
				continue
			}
			// See if getrawtransaction had correct vin amounts. It should
			// except for votes on side chain blocks.
			if valueIn != valueIn0 {
				log.Debugf("vin amount in: prevout RPC = %v, vin's amount = %v",
					valueIn, valueIn0)
			}
		}

		// Assemble and append this vin.
		coinIn := valueIn.ToBTC()
		totalVin += coinIn
		vinBlockHeight := int64(0)
		vinHash, err := btc_chainhash.NewHashFromStr(vin.Txid)
		if err != nil {
			log.Errorf("BTC: Invalid vin transaction hash %s", vin.Txid)
		} else {
			txraw, err := pgb.BtcClient.GetRawTransactionVerbose(vinHash)
			if err != nil {
				log.Errorf("BTC: GetRawTransactionVerbose failed for %v: %w", vinHash, err)
			} else {
				vinBlockHeight, _ = pgb.GetMutilchainBlockHeightByHash(txraw.BlockHash, mutilchain.TYPEBTC)
			}
		}
		inputs = append(inputs, exptypes.MutilchainVin{
			Txid:            vin.Txid,
			Coinbase:        vin.Coinbase,
			Vout:            vin.Vout,
			Sequence:        vin.Sequence,
			Witness:         vin.Witness,
			Addresses:       addresses,
			FormattedAmount: humanize.Commaf(coinIn),
			Index:           uint32(i),
			AmountIn:        coinIn,
			BlockHeight:     vinBlockHeight,
		})
	}
	tx.MutilchainVin = inputs

	CoinbaseMaturityInHours := (pgb.btcChainParams.TargetTimePerBlock.Hours() * float64(pgb.btcChainParams.CoinbaseMaturity))
	tx.MaturityTimeTill = ((float64(pgb.btcChainParams.CoinbaseMaturity) -
		float64(tx.Confirmations)) / float64(pgb.btcChainParams.CoinbaseMaturity)) * CoinbaseMaturityInHours

	outputs := make([]exptypes.Vout, 0, len(txraw.Vout))
	var totalVout float64
	for i, vout := range txraw.Vout {
		// Determine spent status with gettxout, including mempool.
		txout, err := pgb.BtcClient.GetTxOut(txhash, uint32(i), true)
		if err != nil {
			log.Warnf("Failed to determine if tx out is spent for output %d of tx %s: %v", i, txid, err)
		}
		var opReturn string
		var opTAdd bool
		if strings.HasPrefix(vout.ScriptPubKey.Asm, "OP_RETURN") {
			opReturn = vout.ScriptPubKey.Asm
		} else {
			opTAdd = strings.HasPrefix(vout.ScriptPubKey.Asm, "OP_TADD")
		}
		// Get a consistent script class string from dbtypes.ScriptClass.
		outputs = append(outputs, exptypes.Vout{
			Addresses:       vout.ScriptPubKey.Addresses,
			Amount:          vout.Value,
			FormattedAmount: humanize.Commaf(vout.Value),
			OP_RETURN:       opReturn,
			OP_TADD:         opTAdd,
			Spent:           txout == nil,
			Index:           vout.N,
			Type:            vout.ScriptPubKey.Type,
		})
		totalVout += vout.Value
	}
	tx.Vout = outputs
	// Initialize the spending transaction slice for safety.
	tx.SpendingTxns = make([]exptypes.TxInID, len(outputs))
	tx.FeeCoin = totalVin - totalVout
	return tx
}

func (pgb *ChainDB) GetMutilchainExplorerTx(txid string, chainType string) *exptypes.TxInfo {
	switch chainType {
	case mutilchain.TYPEBTC:
		return pgb.GetBTCExplorerTx(txid)
	case mutilchain.TYPELTC:
		return pgb.GetLTCExplorerTx(txid)
	default:
		return pgb.GetExplorerTx(txid)
	}
}

// GetExplorerTx creates a *exptypes.TxInfo for the transaction with the given
// ID.
func (pgb *ChainDB) GetExplorerTx(txid string) *exptypes.TxInfo {
	txhash, err := chainhash.NewHashFromStr(txid)
	if err != nil {
		log.Errorf("Invalid transaction hash %s", txid)
		return nil
	}

	txraw, ticketPrice, err := pgb.txWithTicketPrice(txhash)
	if err != nil {
		log.Errorf("txWithTicketPrice: %v", err)
		return nil
	}

	msgTx, err := txhelpers.MsgTxFromHex(txraw.Hex)
	if err != nil {
		log.Errorf("Cannot create MsgTx for tx %v: %v", txhash, err)
		return nil
	}

	txBasic, txType := makeExplorerTxBasic(txraw, ticketPrice, msgTx, pgb.chainParams)
	tx := &exptypes.TxInfo{
		TxBasic:       txBasic,
		BlockHeight:   txraw.BlockHeight,
		BlockIndex:    txraw.BlockIndex,
		BlockHash:     txraw.BlockHash,
		Confirmations: txraw.Confirmations,
		Time:          exptypes.NewTimeDefFromUNIX(txraw.Time),
	}

	// tree := txType stake.TxTypeRegular

	inputs := make([]exptypes.Vin, 0, len(txraw.Vin))
	for i := range txraw.Vin {
		vin := &txraw.Vin[i]
		// The addresses are may only be obtained by decoding the previous
		// output's pkscript.
		var addresses []string
		// The vin amount is now correct in most cases, but get it from the
		// previous output anyway and compare the values for information.
		valueIn, _ := dcrutil.NewAmount(vin.AmountIn)
		// Do not attempt to look up prevout if it is a coinbase or stakebase
		// input, which does not spend a previous output.
		prevOut := &msgTx.TxIn[i].PreviousOutPoint
		if !txhelpers.IsZeroHash(prevOut.Hash) {
			// Store the vin amount for comparison.
			valueIn0 := valueIn

			addresses, valueIn, err = txhelpers.OutPointAddresses(
				prevOut, pgb.Client, pgb.chainParams)
			if err != nil {
				log.Warnf("Failed to get outpoint address from txid: %v", err)
				continue
			}
			// See if getrawtransaction had correct vin amounts. It should
			// except for votes on side chain blocks.
			if valueIn != valueIn0 {
				log.Debugf("vin amount in: prevout RPC = %v, vin's amount = %v",
					valueIn, valueIn0)
			}
		}

		// For mempool transactions where the vin block height is not set
		// (height 0 for an input that is not a coinbase or stakebase),
		// determine the height at which the input was generated via RPC.
		if tx.BlockHeight == 0 && vin.BlockHeight == 0 &&
			!txhelpers.IsZeroHashStr(vin.Txid) && vin.Txid != "" {
			vinHash, err := chainhash.NewHashFromStr(vin.Txid)
			if err != nil {
				log.Errorf("Failed to translate hash from string: %s", vin.Txid)
			} else {
				// Alt. DB-powered height lookup:
				// vin.BlockHeight = uint32(pgb.TxHeight(vinHash))
				prevTx, err := pgb.Client.GetRawTransactionVerbose(context.TODO(), vinHash)
				if err == nil {
					vin.BlockHeight = uint32(prevTx.BlockHeight)
				} else {
					log.Errorf("Error getting data for previous outpoint of mempool transaction: %v", err)
				}
			}
		}

		// Assemble and append this vin.
		coinIn := valueIn.ToCoin()
		inputs = append(inputs, exptypes.Vin{
			Vin:             vin,
			Addresses:       addresses,
			FormattedAmount: humanize.Commaf(coinIn),
			Index:           uint32(i),
		})
	}
	tx.Vin = inputs

	if isVote := tx.IsVote(); isVote || tx.IsTicket() {
		if tx.Confirmations > 0 && pgb.Height() >=
			(int64(pgb.chainParams.TicketMaturity)+tx.BlockHeight) {
			tx.Mature = "True"
		} else {
			tx.Mature = "False"
			tx.TicketInfo.TicketMaturity = int64(pgb.chainParams.TicketMaturity)
		}

		if isVote {
			if tx.Confirmations < int64(pgb.chainParams.CoinbaseMaturity) {
				tx.VoteFundsLocked = "True"
			} else {
				tx.VoteFundsLocked = "False"
			}
			tx.Maturity = int64(pgb.chainParams.CoinbaseMaturity) + 1 // Add one to reflect < instead of <=
		}
	} else if tx.Type == exptypes.CoinbaseTypeStr || tx.IsTreasurybase() || tx.IsRevocation() ||
		tx.IsTreasuryAdd() || tx.IsTreasurySpend() {
		tx.IsTreasury = tx.IsTreasurybase() || tx.IsTreasuryAdd() || tx.IsTreasurySpend()
		tx.SetFilterTreasuryType()
		if tx.Confirmations < int64(pgb.chainParams.CoinbaseMaturity) {
			tx.Mature = "False"
		} else {
			tx.Mature = "True"
		}
		tx.Maturity = int64(pgb.chainParams.CoinbaseMaturity)

		if tx.IsTreasurySpend() {
			tSpendTally, err := pgb.TSpendVotes(txhash)
			if err != nil {
				log.Errorf("Failed to retrieve vote tally for tspend %v: %v", txid, err)
			}
			tx.TSpendMeta = new(dbtypes.TreasurySpendMetaData)
			tx.TSpendMeta.TreasurySpendVotes = tSpendTally

			tip, err := pgb.GetTip()
			if err != nil {
				log.Errorf("Failed to get the chain tip from the database.: %v", err)
				return nil
			}
			tipHeight := int64(tip.Height)

			totalVotes := tx.TSpendMeta.YesVotes + tx.TSpendMeta.NoVotes
			targetBlockTimeSec := int64(pgb.chainParams.TargetTimePerBlock / time.Second)
			voteStarted := tipHeight >= tx.TSpendMeta.VoteStart

			tx.TSpendMeta.MaxVotes = int64(uint64(pgb.chainParams.TicketsPerBlock) * pgb.chainParams.TreasuryVoteInterval * pgb.chainParams.TreasuryVoteIntervalMultiplier)
			tx.TSpendMeta.QuorumCount = tx.TSpendMeta.MaxVotes * int64(pgb.chainParams.TreasuryVoteQuorumMultiplier) / int64(pgb.chainParams.TreasuryVoteQuorumDivisor)
			tx.TSpendMeta.QuorumAchieved = totalVotes >= tx.TSpendMeta.QuorumCount
			tx.TSpendMeta.TotalVotes = totalVotes

			var maxRemainingBlocks int64
			if !voteStarted {
				maxRemainingBlocks = tx.TSpendMeta.VoteEnd - tx.TSpendMeta.VoteStart
			} else if tx.BlockHeight != 0 && tx.TSpendMeta.VoteEnd > tx.BlockHeight {
				// tspend was short-circuited but we still account for the max
				// remaining votes at the block it the short-circuited.
				maxRemainingBlocks = tx.TSpendMeta.VoteEnd - tx.BlockHeight
			} else if tx.TSpendMeta.VoteEnd > tipHeight {
				maxRemainingBlocks = tx.TSpendMeta.VoteEnd - tipHeight
			}
			maxRemainingVotes := maxRemainingBlocks * int64(pgb.chainParams.TicketsPerBlock)

			requiredYesVotes := (totalVotes + maxRemainingVotes) * int64(pgb.chainParams.TreasuryVoteRequiredMultiplier) / int64(pgb.chainParams.TreasuryVoteRequiredDivisor)
			tx.TSpendMeta.RequiredYesVotes = requiredYesVotes
			tx.TSpendMeta.Approved = tx.TSpendMeta.YesVotes >= requiredYesVotes && tx.TSpendMeta.TotalVotes >= tx.TSpendMeta.QuorumCount
			tx.TSpendMeta.PassPercent = float32(pgb.chainParams.TreasuryVoteRequiredMultiplier) / float32(pgb.chainParams.TreasuryVoteRequiredDivisor)

			if totalVotes > 0 {
				tx.TSpendMeta.Approval = float32(tx.TSpendMeta.YesVotes) / float32(totalVotes)
			}

			secTillVoteStart := (tx.TSpendMeta.VoteStart - tipHeight) * targetBlockTimeSec
			tx.TSpendMeta.VoteStartDate = time.Now().Add(time.Duration(secTillVoteStart) * time.Second).UTC()
			if voteStarted { // started
				voteStartTimeStamp, err := pgb.BlockTimeByHeight(tx.TSpendMeta.VoteStart)
				if err != nil {
					log.Errorf("Error fetching tspend start block time: %v", err)
				}
				tx.TSpendMeta.VoteStartDate = time.Unix(voteStartTimeStamp, 0).UTC()
			}

			secTillVoteEnd := (tx.TSpendMeta.VoteEnd - tipHeight) * targetBlockTimeSec
			tx.TSpendMeta.VoteEndDate = time.Now().Add(time.Duration(secTillVoteEnd) * time.Second).UTC()
			if tx.TSpendMeta.Approved && tx.BlockHeight == 0 { // tspend is approved, may have been short-circuited
				// tspend yet to be mined, will go in next TVI block
				blocksToNextTVI := pgb.chainParams.TreasuryVoteInterval - uint64(tipHeight)%pgb.chainParams.TreasuryVoteInterval
				secsTillNextTVI := blocksToNextTVI * uint64(targetBlockTimeSec)
				tx.TSpendMeta.NextTVITime = time.Now().Add(time.Duration(secsTillNextTVI) * time.Second).UTC()
				tx.TSpendMeta.NextTVI = tipHeight + int64(blocksToNextTVI)
			}
			if tipHeight > tx.TSpendMeta.VoteEnd { // voting has ended
				voteEndTimeStamp, err := pgb.BlockTimeByHeight(tx.TSpendMeta.VoteEnd)
				if err != nil {
					log.Errorf("Error fetching tspend end block time: %v", err)
				}
				tx.TSpendMeta.VoteEndDate = time.Unix(voteEndTimeStamp, 0).UTC()
			}

			if voteStarted {
				// currentVoteEndBlock is the tspend vote end block, the block
				// this tspend was mined or the currect block height if the
				// tspend is still voting.
				currentVoteEndBlock := tx.TSpendMeta.VoteEnd
				if (tx.TSpendMeta.Approved && tx.BlockHeight > 0) && tx.BlockHeight < tx.TSpendMeta.VoteEnd { // short-circuited tspend
					currentVoteEndBlock = tx.BlockHeight
				} else if tx.TSpendMeta.VoteEnd > tipHeight { // still voting
					currentVoteEndBlock = tipHeight
				}

				misses, err := pgb.missedVotesForBlockRange(tx.TSpendMeta.VoteStart, currentVoteEndBlock)
				if err != nil {
					log.Errorf("failed to get missed votes count for tspend voting window: %v", err)
					return nil
				}

				// tx.TSpendMeta.EligibleVotes is the number of actual votes
				// that were cast in the tspend voting window, including votes
				// that did not indicate a tspend choice (aka abstain votes).
				// This is used to calculate vote turnout and give information
				// about the number of eligible votes that were cast in the
				// current voting window.
				tx.TSpendMeta.EligibleVotes = int64(pgb.chainParams.TicketsPerBlock)*(currentVoteEndBlock-tx.TSpendMeta.VoteStart) - misses
			}

			// Retrieve Public Key a.k.a Politieia key.
			var pubKey []byte
			txIn, err := hex.DecodeString(tx.Vin[0].TreasurySpend)
			if err != nil {
				log.Errorf("failed to retrieve pikey: %v", err)
			}

			// The length of the signature script in transaction input 0 must be
			// 100 bytes according to dcp-0006: ([OP_DATA_64] [signature]
			// [OP_DATA_33] [PiKey] [OP_TSPEND] = 1 + 64 + 1 + 33 + 1 = 100)
			if len(txIn) == 100 {
				pubKey = txIn[66 : 66+secp256k1.PubKeyBytesLenCompressed]
			}
			tx.TSpendMeta.PoliteiaKey = hex.EncodeToString(pubKey)
		}
	}

	tree := wire.TxTreeStake
	if txType == stake.TxTypeRegular {
		tree = wire.TxTreeRegular
	}

	CoinbaseMaturityInHours := (pgb.chainParams.TargetTimePerBlock.Hours() * float64(pgb.chainParams.CoinbaseMaturity))
	tx.MaturityTimeTill = ((float64(pgb.chainParams.CoinbaseMaturity) -
		float64(tx.Confirmations)) / float64(pgb.chainParams.CoinbaseMaturity)) * CoinbaseMaturityInHours

	outputs := make([]exptypes.Vout, 0, len(txraw.Vout))
	for i, vout := range txraw.Vout {
		// Determine spent status with gettxout, including mempool.
		txout, err := pgb.Client.GetTxOut(context.TODO(), txhash, uint32(i), tree, true)
		if err != nil {
			log.Warnf("Failed to determine if tx out is spent for output %d of tx %s: %v", i, txid, err)
		}
		var opReturn string
		var opTAdd bool
		if strings.HasPrefix(vout.ScriptPubKey.Asm, "OP_RETURN") {
			opReturn = vout.ScriptPubKey.Asm
		} else {
			opTAdd = strings.HasPrefix(vout.ScriptPubKey.Asm, "OP_TADD")
		}
		// Get a consistent script class string from dbtypes.ScriptClass.
		pkScript, version := msgTx.TxOut[i].PkScript, msgTx.TxOut[i].Version
		scriptClass := dbtypes.NewScriptClass(stdscript.DetermineScriptType(version, pkScript))
		if scriptClass == dbtypes.SCNullData && vout.ScriptPubKey.CommitAmt != nil {
			scriptClass = dbtypes.SCStakeSubCommit
		}
		outputs = append(outputs, exptypes.Vout{
			Addresses:       vout.ScriptPubKey.Addresses,
			Amount:          vout.Value,
			FormattedAmount: humanize.Commaf(vout.Value),
			OP_RETURN:       opReturn,
			OP_TADD:         opTAdd,
			Type:            scriptClass.String(),
			Spent:           txout == nil,
			Index:           vout.N,
			Version:         version,
		})
	}
	tx.Vout = outputs

	// Initialize the spending transaction slice for safety.
	tx.SpendingTxns = make([]exptypes.TxInID, len(outputs))

	return tx
}

// GetTip grabs the highest block stored in the database.
func (pgb *ChainDB) GetTip() (*exptypes.WebBasicBlock, error) {
	tip, err := pgb.getTip()
	if err != nil {
		return nil, err
	}
	blockdata := exptypes.WebBasicBlock{
		Height:      tip.Height,
		Size:        tip.Size,
		Hash:        tip.Hash,
		Difficulty:  tip.Difficulty,
		StakeDiff:   tip.StakeDiff,
		Time:        tip.Time.S.UNIX(),
		NumTx:       tip.NumTx,
		PoolSize:    tip.PoolInfo.Size,
		PoolValue:   tip.PoolInfo.Value,
		PoolValAvg:  tip.PoolInfo.ValAvg,
		PoolWinners: tip.PoolInfo.Winners,
	}
	return &blockdata, nil
}

// getTip returns the last block stored using StoreBlockSummary.
// If no block has been stored yet, it returns the best block in the database.
func (pgb *ChainDB) getTip() (*apitypes.BlockDataBasic, error) {
	pgb.tipMtx.Lock()
	defer pgb.tipMtx.Unlock()
	if pgb.tipSummary != nil && pgb.tipSummary.Hash == pgb.BestBlockHashStr() {
		return pgb.tipSummary, nil
	}
	tip, err := RetrieveLatestBlockSummary(pgb.ctx, pgb.db)
	if err != nil {
		return nil, err
	}
	pgb.tipSummary = tip
	return tip, nil
}

// DecodeRawTransaction creates a *chainjson.TxRawResult from a hex-encoded
// transaction.
func (pgb *ChainDB) DecodeRawTransaction(txhex string) (*chainjson.TxRawResult, error) {
	bytes, err := hex.DecodeString(txhex)
	if err != nil {
		log.Errorf("DecodeRawTransaction failed: %v", err)
		return nil, err
	}
	tx, err := pgb.Client.DecodeRawTransaction(context.TODO(), bytes)
	if err != nil {
		log.Errorf("DecodeRawTransaction failed: %v", err)
		return nil, err
	}
	return tx, nil
}

// TxHeight gives the block height of the transaction id specified
func (pgb *ChainDB) TxHeight(txid *chainhash.Hash) (height int64) {
	// Alt. DB-powered height lookup:
	// txBlocks, _, err := pgb.TransactionBlocks(txid.String())
	// if err != nil {
	// 	log.Errorf("TransactionBlocks failed for: %v", txid)
	// 	return 0
	// }
	// // ordered by valid, mainchain, height
	// for _, block := range txBlocks {
	// 	if block.IsMainchain {
	// 		return int64(txBlocks[0].Height)
	// 	}
	// }
	// return 0
	txraw, err := pgb.Client.GetRawTransactionVerbose(context.TODO(), txid)
	if err != nil {
		log.Errorf("GetRawTransactionVerbose failed for: %v", txid)
		return 0
	}
	height = txraw.BlockHeight
	return
}

func (pgb *ChainDB) GetMutilchainExplorerFullBlocks(chainType string, start, end int) []*exptypes.BlockInfo {
	result := make([]*exptypes.BlockInfo, 0)
	blockInfos, err := RetrieveLastestBlocksInfo(pgb.ctx, pgb.db, chainType, int64(start), int64(end))
	if err != nil {
		return result
	}
	for _, blockInfo := range blockInfos {
		resItem := &exptypes.BlockInfo{
			BlockBasic: &exptypes.BlockBasic{
				Height:        blockInfo.Height,
				BlockTime:     exptypes.NewTimeDef(time.Unix(blockInfo.Time, 0)),
				BlockTimeUnix: blockInfo.Time,
				TxCount:       uint32(blockInfo.TxCount),
			},
			TotalSentSats: blockInfo.Total,
			FeesSats:      blockInfo.Fees,
			TotalInputs:   blockInfo.Inputs,
			TotalOutputs:  blockInfo.Outputs,
			BlockReward:   mutilchain.GetCurrentBlockReward(chainType, pgb.GetSubsidyReductionInterval(chainType), int32(blockInfo.Height)),
		}
		result = append(result, resItem)
	}
	return result
}

func (pgb *ChainDB) GetMultichainMinBlockHeight(chainType string) (int32, error) {
	var minDBHeight int32
	err := pgb.db.QueryRow(mutilchainquery.MakeSelectMinBlockHeight(chainType)).Scan(&minDBHeight)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	}
	return minDBHeight, nil
}

func (pgb *ChainDB) GetSubsidyReductionInterval(chainType string) int32 {
	switch chainType {
	case mutilchain.TYPELTC:
		return pgb.ltcChainParams.SubsidyReductionInterval
	case mutilchain.TYPEBTC:
		return pgb.btcChainParams.SubsidyReductionInterval
	default:
		return 0
	}
}

func (pgb *ChainDB) GetDBBlockDetailInfo(chainType string, height int64) *dbtypes.MutilchainDBBlockInfo {
	dbBlockInfo, err := RetrieveBlockInfo(pgb.ctx, pgb.db, chainType, height)
	if err != nil {
		return nil
	}
	return dbBlockInfo
}

// GetExplorerFullBlocks gets the *exptypes.BlockInfo's for a range of block
// heights.
func (pgb *ChainDB) GetExplorerFullBlocks(start int, end int) []*exptypes.BlockInfo {
	if start < end {
		return nil
	}
	summaries := make([]*exptypes.BlockInfo, 0, start-end)
	for i := start; i > end; i-- {
		data := pgb.GetBlockVerbose(i, true)
		block := new(exptypes.BlockInfo)
		if data != nil {
			block = pgb.GetExplorerBlock(data.Hash)
		}
		summaries = append(summaries, block)
	}
	return summaries
}

// CurrentDifficulty returns the current difficulty from dcrd.
func (pgb *ChainDB) CurrentDifficulty() (float64, error) {
	diff, err := pgb.Client.GetDifficulty(context.TODO())
	if err != nil {
		log.Error("GetDifficulty failed")
		return diff, err
	}
	return diff, nil
}

// Difficulty returns the difficulty for the first block mined after the
// provided UNIX timestamp.
func (pgb *ChainDB) Difficulty(timestamp int64) float64 {
	pgb.lastExplorerBlock.Lock()
	diff, ok := pgb.lastExplorerBlock.difficulties[timestamp]
	pgb.lastExplorerBlock.Unlock()
	if ok {
		return diff
	}

	diff, err := RetrieveDiff(pgb.ctx, pgb.db, timestamp)
	if err != nil {
		log.Errorf("Unable to retrieve difficulty: %v", err)
		return -1
	}
	pgb.lastExplorerBlock.Lock()
	pgb.lastExplorerBlock.difficulties[timestamp] = diff
	pgb.lastExplorerBlock.Unlock()
	return diff
}

func (pgb *ChainDB) BTCDifficulty(timestamp int64) float64 {
	pgb.btcLastExplorerBlock.Lock()
	diff, ok := pgb.btcLastExplorerBlock.difficulties[timestamp]
	pgb.btcLastExplorerBlock.Unlock()
	if ok {
		return diff
	}

	diff, err := RetrieveMutilchainDiff(pgb.ctx, pgb.db, timestamp, mutilchain.TYPEBTC)
	if err != nil {
		log.Errorf("BTC: Unable to retrieve difficulty: %v", err)
		return -1
	}
	pgb.btcLastExplorerBlock.Lock()
	pgb.btcLastExplorerBlock.difficulties[timestamp] = diff
	pgb.btcLastExplorerBlock.Unlock()
	return diff
}

func (pgb *ChainDB) LTCDifficulty(timestamp int64) float64 {
	pgb.ltcLastExplorerBlock.Lock()
	diff, ok := pgb.ltcLastExplorerBlock.difficulties[timestamp]
	pgb.ltcLastExplorerBlock.Unlock()
	if ok {
		return diff
	}

	diff, err := RetrieveMutilchainDiff(pgb.ctx, pgb.db, timestamp, mutilchain.TYPELTC)
	if err != nil {
		log.Errorf("LTC: Unable to retrieve difficulty: %v", err)
		return -1
	}
	pgb.ltcLastExplorerBlock.Lock()
	pgb.ltcLastExplorerBlock.difficulties[timestamp] = diff
	pgb.ltcLastExplorerBlock.Unlock()
	return diff
}

func (pgb *ChainDB) MutilchainGetTransactionCount(chainType string) int64 {
	txCount, err := RetrieveMutilchainTransactionCount(pgb.ctx, pgb.db, chainType)
	if err != nil {
		return 0
	}
	return txCount
}

func (pgb *ChainDB) MutilchainGetBlockchainInfo(chainType string) (*mutilchain.BlockchainInfo, error) {
	difficulty := float64(0)
	switch chainType {
	case mutilchain.TYPEBTC:
		difficulty, _ = btcrpcutils.GetBlockchainDifficulty(pgb.BtcClient)
	case mutilchain.TYPELTC:
		difficulty, _ = ltcrpcutils.GetBlockchainDifficulty(pgb.LtcClient)
	default:
		return nil, fmt.Errorf("%s", "Get size and tx count error. Invalid chain type")
	}
	coinSupply, txCount, err := externalapi.GetOkLinkBlockchainSummaryData(pgb.OkLinkAPIKey, chainType)
	if err != nil {
		log.Errorf("MutilchainGetBlockchainInfo get oklink blockchain summary data failed: %v", err)
	}

	//TODO get blockchaininfo
	return &mutilchain.BlockchainInfo{
		TotalTransactions: txCount,
		CoinSupply:        coinSupply,
		Difficulty:        difficulty,
	}, nil
}

func (pgb *ChainDB) MutilchainGetTotalVoutsCount(chainType string) int64 {
	txCount, err := RetrieveMutilchainVoutsCount(pgb.ctx, pgb.db, chainType)
	if err != nil {
		return 0
	}
	return txCount
}

func (pgb *ChainDB) MutilchainGetTotalAddressesCount(chainType string) int64 {
	txCount, err := RetrieveMutilchainAddressesCount(pgb.ctx, pgb.db, chainType)
	if err != nil {
		return 0
	}
	return txCount
}

func (pgb *ChainDB) GetDecredBlockchainSize() int64 {
	size, err := retrieveBlockchainSize(pgb.ctx, pgb.db)
	if err != nil {
		return 0
	}
	return size
}

func (pgb *ChainDB) GetDecredTotalTransactions() int64 {
	txcount, err := retrieveTotalTransactions(pgb.ctx, pgb.db)
	if err != nil {
		return 0
	}
	return txcount
}

func (pgb *ChainDB) MutilchainDifficulty(timestamp int64, chainType string) float64 {
	switch chainType {
	case mutilchain.TYPEBTC:
		return pgb.BTCDifficulty(timestamp)
	case mutilchain.TYPELTC:
		return pgb.LTCDifficulty(timestamp)
	default:
		return pgb.Difficulty(timestamp)
	}
}

// GetMempool gets all transactions from the mempool for explorer and adds the
// total out for all the txs and vote info for the votes. The returned slice
// will be nil if the GetRawMempoolVerbose RPC fails. A zero-length non-nil
// slice is returned if there are no transactions in mempool. UNUSED?
func (pgb *ChainDB) GetMempool() []exptypes.MempoolTx {
	mempooltxs, err := pgb.Client.GetRawMempoolVerbose(pgb.ctx, chainjson.GRMAll)
	if err != nil {
		log.Errorf("GetRawMempoolVerbose failed: %v", err)
		return nil
	}

	txs := make([]exptypes.MempoolTx, 0, len(mempooltxs))

	for hashStr, tx := range mempooltxs {
		hash, err := chainhash.NewHashFromStr(hashStr)
		if err != nil {
			continue
		}
		txn, err := pgb.Client.GetRawTransaction(pgb.ctx, hash)
		if err != nil {
			log.Errorf("GetRawTransaction: %v", err)
			continue
		}
		msgTx := txn.MsgTx()
		var total int64
		for _, v := range msgTx.TxOut {
			total += v.Value
		}

		txType := txhelpers.DetermineTxType(msgTx)

		var voteInfo *exptypes.VoteInfo
		if txType == stake.TxTypeSSGen {
			validation, version, bits, choices, tspendVotes, err := txhelpers.SSGenVoteChoices(msgTx, pgb.chainParams)
			if err != nil {
				log.Debugf("Cannot get vote choices for %s", hash)
			} else {
				voteInfo = &exptypes.VoteInfo{
					Validation: exptypes.BlockValidation{
						Hash:     validation.Hash.String(),
						Height:   validation.Height,
						Validity: validation.Validity,
					},
					Version:     version,
					Bits:        bits,
					Choices:     choices,
					TicketSpent: msgTx.TxIn[1].PreviousOutPoint.Hash.String(),
					TSpends:     exptypes.ConvertTSpendVotes(tspendVotes),
				}
			}
		}

		fee, feeRate := txhelpers.TxFeeRate(msgTx)

		txs = append(txs, exptypes.MempoolTx{
			TxID:     hashStr,
			Version:  int32(msgTx.Version),
			Fees:     fee.ToCoin(),
			FeeRate:  feeRate.ToCoin(),
			Hash:     hashStr, // dup of TxID!
			Time:     tx.Time,
			Size:     tx.Size,
			TotalOut: dcrutil.Amount(total).ToCoin(),
			Type:     txhelpers.TxTypeToString(int(txType)),
			TypeID:   int(txType),
			VoteInfo: voteInfo,
			Vin:      exptypes.MsgTxMempoolInputs(msgTx),
		})
	}

	return txs
}

// BlockchainInfo retrieves the result of the getblockchaininfo node RPC.
func (pgb *ChainDB) BlockchainInfo() (*chainjson.GetBlockChainInfoResult, error) {
	return pgb.Client.GetBlockChainInfo(pgb.ctx)
}

// UpdateChan creates a channel that will receive height updates. All calls to
// UpdateChan should be completed before blocks start being connected.
func (pgb *ChainDB) UpdateChan() chan uint32 {
	c := make(chan uint32)
	pgb.heightClients = append(pgb.heightClients, c)
	return c
}

// SignalHeight signals the database height to any registered receivers.
// This function is exported so that it can be called once externally after all
// update channel clients have subscribed.
func (pgb *ChainDB) SignalHeight(height uint32) {
	for i, c := range pgb.heightClients {
		select {
		case c <- height:
		case <-time.NewTimer(time.Minute).C:
			log.Criticalf("(*DBDataSaver).SignalHeight: heightClients[%d] timed out. Forcing a shutdown.", i)
			pgb.shutdownDcrdata()
		}
	}
}

func (pgb *ChainDB) SignalLTCHeight(height uint32) {
	for i, c := range pgb.ltcHeightClients {
		select {
		case c <- height:
		case <-time.NewTimer(time.Minute).C:
			log.Criticalf("(*LTCDBDataSaver).SignalLTCHeight: ltcHeightClients[%d] timed out. Forcing a shutdown.", i)
			pgb.shutdownDcrdata()
		}
	}
}

func (pgb *ChainDB) SignalBTCHeight(height uint32) {
	for i, c := range pgb.btcHeightClients {
		select {
		case c <- height:
		case <-time.NewTimer(time.Minute).C:
			log.Criticalf("(*BTCDBDataSaver).SignalBTCHeight: btcHeightClients[%d] timed out. Forcing a shutdown.", i)
			pgb.shutdownDcrdata()
		}
	}
}

func (pgb *ChainDB) SyncBlockTimeWithMinHeight(chainType string, minHeight int32) error {
	height := minHeight - 1
	for height >= 1 {
		hash, time, err := pgb.GetMultichainBlockHashTime(chainType, height)
		if err != nil {
			return err
		}
		// insert to block
		err = pgb.InsertToMultichainBlockSimpleInfo(chainType, hash, int64(height), time)
		if err != nil {
			return err
		}
		height--
	}
	return nil
}

func (pgb *ChainDB) InsertToMultichainBlockSimpleInfo(chainType, hash string, height, heightTime int64) error {
	var id int64
	err := pgb.db.QueryRow(mutilchainquery.MakeInsertSimpleBlockInfo(chainType), hash, height, heightTime, false).Scan(&id)
	return err
}

func (pgb *ChainDB) GetMultichainBlockHashTime(chainType string, height int32) (string, int64, error) {
	switch chainType {
	case mutilchain.TYPELTC:
		return pgb.GetLTCBlockHashTime(height)
	case mutilchain.TYPEBTC:
		return pgb.GetBTCBlockHashTime(height)
	default:
		return "", 0, nil
	}
}

func (pgb *ChainDB) GetBTCBlockHashTime(height int32) (string, int64, error) {
	blockhash, err := pgb.BtcClient.GetBlockHash(int64(height))
	if err != nil {
		return "", 0, err
	}
	blockRst, rstErr := pgb.BtcClient.GetBlock(blockhash)
	if rstErr != nil {
		return "", 0, rstErr
	}
	return blockhash.String(), blockRst.Header.Timestamp.Unix(), nil
}

func (pgb *ChainDB) GetLTCBlockHashTime(height int32) (string, int64, error) {
	blockhash, err := pgb.LtcClient.GetBlockHash(int64(height))
	if err != nil {
		return "", 0, err
	}
	blockRst, rstErr := pgb.LtcClient.GetBlock(blockhash)
	if rstErr != nil {
		return "", 0, rstErr
	}
	return blockhash.String(), blockRst.Header.Timestamp.Unix(), nil
}

func (pgb *ChainDB) MixedUtxosByHeight() (heights, utxoCountReg, utxoValueReg, utxoCountStk, utxoValueStk []int64, err error) {
	var rows *sql.Rows
	rows, err = pgb.db.Query(internal.SelectMixedVouts, -1)
	if err != nil {
		return
	}
	defer rows.Close()

	var vals, fundHeights, spendHeights []int64
	var trees []uint8

	var maxHeight int64
	minHeight := int64(math.MaxInt64)
	var value, fundHeight, spendHeight int64
	for rows.Next() {
		var spendHeightNull sql.NullInt64
		var tree uint8
		err = rows.Scan(&value, &fundHeight, &spendHeightNull, &tree)
		if err != nil {
			return
		}
		vals = append(vals, value)
		fundHeights = append(fundHeights, fundHeight)
		trees = append(trees, tree)
		if spendHeightNull.Valid {
			spendHeight = spendHeightNull.Int64
		} else {
			spendHeight = -1
		}
		spendHeights = append(spendHeights, spendHeight)
		if fundHeight < minHeight {
			minHeight = fundHeight
		}
		if spendHeight > maxHeight {
			maxHeight = spendHeight
		}
	}

	N := maxHeight - minHeight + 1
	heights = make([]int64, N)
	utxoCountReg = make([]int64, N)
	utxoValueReg = make([]int64, N)
	utxoCountStk = make([]int64, N)
	utxoValueStk = make([]int64, N)

	for h := minHeight; h <= maxHeight; h++ {
		i := h - minHeight
		heights[i] = h
		for iu := range vals {
			if h >= fundHeights[iu] && (h < spendHeights[iu] || spendHeights[iu] == -1) {
				if trees[iu] == 0 {
					utxoCountReg[i]++
					utxoValueReg[i] += vals[iu]
				} else {
					utxoCountStk[i]++
					utxoValueStk[i] += vals[iu]
				}
			}
		}
	}

	err = rows.Err()
	return

}

func (pgb *ChainDB) GetLTCBestBlock() error {
	ltcHash, ltcHeight, err := pgb.LtcClient.GetBestBlock()
	ltcTime := int64(0)
	if err != nil {
		return fmt.Errorf("Unable to get block from ltc node: %v", err)
	}
	blockhash, err := pgb.LtcClient.GetBlockHash(int64(ltcHeight))
	if err == nil {
		blockRst, rstErr := pgb.LtcClient.GetBlockVerbose(blockhash)
		if rstErr == nil {
			ltcTime = blockRst.Time
		}
	}
	//create bestblock object
	bestBlock := &MutilchainBestBlock{
		Height: int64(ltcHeight),
		Hash:   ltcHash.String(),
		Time:   ltcTime,
	}
	pgb.LtcBestBlock = bestBlock
	return nil
}

func (pgb *ChainDB) GetBTCBestBlock() error {
	btcHash, btcHeight, err := pgb.BtcClient.GetBestBlock()
	btcTime := int64(0)
	if err != nil {
		return fmt.Errorf("Unable to get block from btc node: %v", err)
	}
	blockhash, err := pgb.BtcClient.GetBlockHash(int64(btcHeight))
	if err == nil {
		blockRst, rstErr := pgb.BtcClient.GetBlockVerbose(blockhash)
		if rstErr == nil {
			btcTime = blockRst.Time
		}
	}
	//create bestblock object
	bestBlock := &MutilchainBestBlock{
		Height: int64(btcHeight),
		Hash:   btcHash.String(),
		Time:   btcTime,
	}
	pgb.BtcBestBlock = bestBlock
	return nil
}

func (pgb *ChainDB) GetDecredBestBlock() error {
	bestHeight, bestHash, err := RetrieveBestBlock(pgb.ctx, pgb.db)
	if err != nil {
		return fmt.Errorf("RetrieveBestBlock: %w", err)
	}
	bestBlock := &BestBlock{
		height: bestHeight,
		hash:   bestHash,
	}
	pgb.bestBlock = bestBlock
	return nil
}

// Get average block size with formatted string
func (pgb *ChainDB) GetAvgBlockFormattedSize() (string, error) {
	var avgBlockSize int64
	err := pgb.db.QueryRow(internal.SelectAvgBlockSize).Scan(&avgBlockSize)
	if err != nil {
		return "", err
	}
	return humanize.Bytes(uint64(avgBlockSize)), nil
}

// GetAvgTxFee return average tx fees
func (pgb *ChainDB) GetAvgTxFee() (int64, error) {
	var avgTxFee int64
	err := pgb.db.QueryRow(internal.SelectAvgTxFee).Scan(&avgTxFee)
	if err != nil {
		return 0, err
	}
	return avgTxFee, nil
}

// GetBwDashData get total bison wallet vol (By USD). From dcrsnapcsv (in the future, save to DB)
// return (total vol : int64, last 30 days vol : int64)
func (pgb *ChainDB) GetBwDashData() (int64, int64, int64) {
	dailyData := utils.ReadCsvFileFromUrl("https://raw.githubusercontent.com/bochinchero/dcrsnapcsv/main/data/stream/dex_decred_org_VolUSD.csv")
	if len(dailyData) < 2 {
		return 0, 0, 0
	}
	dailyData = dailyData[1:]
	var volSum, last30days, vol24h float64
	count := 0
	for i := len(dailyData) - 1; i >= 0; i-- {
		dailyItem := dailyData[i]
		dailySum := utils.SumVolOfBwRow(dailyItem)
		volSum += dailySum
		if i == len(dailyData)-1 {
			vol24h = dailySum
		}
		if count < 30 {
			last30days += dailySum
			count++
		}
	}
	return int64(math.Round(volSum)), int64(math.Round(last30days)), int64(math.Round(vol24h))
}

// GetTicketsSummaryInfo return summary information of tickets vote
func (pgb *ChainDB) GetTicketsSummaryInfo() (*dbtypes.TicketsSummaryInfo, error) {
	bestBlockHeight := pgb.bestBlock.Height()
	// get summary info
	result := dbtypes.TicketsSummaryInfo{}
	err := pgb.db.QueryRow(internal.SelectTicketSummaryInfo, bestBlockHeight).Scan(&result.MissedTickets, &result.Last1000BlocksMissed, &result.Last1000BlocksTicketFeeAvg)
	if err != nil {
		return nil, err
	}
	// calculator for ticket maturity
	result.TicketMaturity = uint64(pgb.chainParams.TicketMaturity)
	// avg block time
	timePerBlock := pgb.chainParams.TargetTimePerBlock
	// calculate ticket maturity duration (by seconds)
	result.TicketMaturityDuration = uint64(timePerBlock.Seconds()) * result.TicketMaturity
	// ticket expiration
	result.TicketExpiration = uint64(pgb.chainParams.TicketExpiry)
	// calculate to ticket expiration duration (by seconds)
	result.TicketExpirationDuration = uint64(timePerBlock.Seconds()) * result.TicketExpiration
	// Calculate Win probability on 1 block
	result.WinProbability = float64(pgb.chainParams.TicketsPerBlock) / float64(pgb.chainParams.TicketPoolSize*pgb.chainParams.TicketsPerBlock)
	// Calculate the expected block number to ensure 100% selection
	threshold := 0.0001
	blocksNeed := uint64(math.Round(math.Log(threshold) / math.Log(1-result.WinProbability)))
	result.BlocksNeedToWin = blocksNeed
	result.TimeToBlocksNeedToWin = uint64(timePerBlock.Seconds()) * result.BlocksNeedToWin
	return &result, nil
}

// Get24hActiveAddressesCount return active addresses count in 24h
func (pgb *ChainDB) Get24hActiveAddressesCount() int64 {
	var activeAddr int64
	pgb.db.QueryRow(internal.SelectCount24hUniqueAddress).Scan(&activeAddr)
	return activeAddr
}

// Get24hStakingInfo return 24h staking info
func (pgb *ChainDB) Get24hStakingInfo() (poolvalue, missed int64, err error) {
	err = pgb.db.QueryRow(internal.SelectStakingSummaryIn24h).Scan(&poolvalue, &missed)
	return
}

// Get24hTreasuryBalanceChange return 24h treasury balance change
func (pgb *ChainDB) Get24hTreasuryBalanceChange() (treasuryBalanceChange int64, err error) {
	err = pgb.db.QueryRow(internal.SelectTreasuryBalanceChangeIn24h).Scan(&treasuryBalanceChange)
	return
}

// InsertToBlackList insert to black list
func (pgb *ChainDB) InsertToBlackList(agent, ip, note string) error {
	_, err := pgb.db.Exec(internal.UpsertBlackList, agent, ip, note)
	return err
}

// CheckOnBlackList return true if exist on black list
func (pgb *ChainDB) CheckOnBlackList(agent, ip string) (bool, error) {
	var onBlacklist bool
	err := pgb.db.QueryRow(internal.CheckExistOnBlackList, agent, ip).Scan(&onBlacklist)
	return onBlacklist, err
}

func (pgb *ChainDB) GetMultichainStats(chainType string) (*externalapi.ChainStatsData, error) {
	return externalapi.GetBlockchainStats(chainType)
}
