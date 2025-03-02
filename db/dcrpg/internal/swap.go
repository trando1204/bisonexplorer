package internal

const (
	CreateAtomicSwapTableV0 = `CREATE TABLE IF NOT EXISTS swaps (
		contract_tx TEXT,
		contract_vout INT4,
		spend_tx TEXT,
		spend_vin INT4,
		spend_height INT8,
		p2sh_addr TEXT,
		value INT8,
		secret_hash BYTEA,
		secret BYTEA,        -- NULL for refund
		lock_time INT8,
		target_token TEXT,
		is_refund BOOLEAN DEFAULT false,
		CONSTRAINT spend_tx_in PRIMARY KEY (spend_tx, spend_vin)
	);`

	CreateAtomicSwapTable = CreateAtomicSwapTableV0

	InsertContractSpend = `INSERT INTO swaps (contract_tx, contract_vout, spend_tx, spend_vin, spend_height,
		p2sh_addr, value, secret_hash, secret, lock_time, is_refund)
	VALUES ($1, $2, $3, $4, $5,
		$6, $7, $8, $9, $10, $11) 
	ON CONFLICT (spend_tx, spend_vin)
		DO UPDATE SET spend_height = $5;`

	UpdateTargetToken = `UPDATE swaps SET target_token = $1 WHERE spend_tx = $2 AND spend_vin = $3 RETURNING contract_tx;`

	IndexSwapsOnHeightV0 = `CREATE INDEX idx_swaps_height ON swaps (spend_height);`
	IndexSwapsOnHeight   = IndexSwapsOnHeightV0
	DeindexSwapsOnHeight = `DROP INDEX idx_swaps_height;`

	SelectAtomicSwaps = `SELECT * FROM swaps 
		ORDER BY lock_time DESC
		LIMIT $1 OFFSET $2;`

	SelectDecredMinTime = `SELECT COALESCE(MIN(lock_time), 0) AS min_time FROM swaps`
	CountAtomicSwapsRow = `SELECT COUNT(*)
		FROM swaps`
	SelectTotalTradingAmount           = `SELECT SUM(value) FROM swaps`
	SelectAtomicSwapsTimeWithMinHeight = `SELECT lock_time FROM swaps WHERE spend_height > $1
		ORDER BY lock_time`
	SelectDecredMinContractTx   = `SELECT contract_tx FROM swaps WHERE spend_height > $1 ORDER BY lock_time LIMIT 1`
	SelectDecredMaxLockTime     = `SELECT lock_time FROM swaps WHERE spend_height > $1 ORDER BY lock_time DESC LIMIT 1`
	SelectExistSwapBySecretHash = `SELECT spend_tx, spend_height, spend_vin FROM swaps WHERE secret_hash = $1 LIMIT 1`
)
