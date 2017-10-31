package version

import (
	"database/sql"
	"sync"
	"fmt"
	"errors"
)

type Strategy interface {
	Version(db *sql.DB) (int, error)
	SetVersion(db *sql.DB, version int) (error)
}

type Scheme interface {
	Version() int
	VersionStrategy() string
	OnCreate(db *sql.DB) error
	OnUpdate(db *sql.DB, oldVersion int) error
}

var (
	versionDriversMu sync.RWMutex
	versionDrivers   = make(map[string]Strategy)
)

// Register makes a scheme available for a versioned
// scheme to use by the provided name
// It panics if the passed scheme is nil or if a scheme already is
// registered with the same name
func Register(name string, strategy Strategy) {
	versionDriversMu.Lock()
	defer versionDriversMu.Unlock()

	if strategy == nil {
		panic("versioned db: Register strategy is nil")
	}
	if _, dup := versionDrivers[name]; dup {
		panic("versioned db: Register called twice for strategy " + name)
	}
	versionDrivers[name] = strategy
}

func PersistScheme(db *sql.DB, scheme Scheme) error  {
	var (
		version  int
		strategy Strategy
	)

	if db == nil {
		return errors.New("versioned db: db is nil")
	}

	if scheme == nil {
		return errors.New("versioned db: scheme is nil")
	}

	if version = scheme.Version(); version < 1 {
		return errors.New("versioned db: version is less then one")
	}

	if strategy = strategyFromString(scheme.VersionStrategy()); strategy == nil {
		return fmt.Errorf("versioned db: unknown v scheme %q (forgotten import?)", scheme.VersionStrategy())
	}

	return persistSchemeInternal(strategy, db, version, scheme)
}

func strategyFromString(name string) Strategy {
	versionDriversMu.RLock()
	versionDriver, ok := versionDrivers[name]
	versionDriversMu.RUnlock()
	if ok {
		return versionDriver
	}
	return nil
}

func persistSchemeInternal(strategy Strategy, db *sql.DB, version int, scheme Scheme) error  {
	var (
		err error
		dbVersion int
		tx *sql.Tx
	)

	dbVersion, err = strategy.Version(db)

	if err != nil {
		return err
	}

	tx, err = db.Begin()

	if err != nil {
		return err
	}

	if dbVersion == 0 {
		err = scheme.OnCreate(db)
		goto finalize
	} else if dbVersion < version {
		err = scheme.OnUpdate(db, dbVersion)
		goto finalize
	}

	tx.Rollback()
	return nil

finalize:
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return strategy.SetVersion(db, version)
}




