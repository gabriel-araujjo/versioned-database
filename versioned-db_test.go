package versioned_database

import "testing"
import (
	"github.com/stretchr/testify/mock"
	"database/sql"
	"github.com/stretchr/testify/assert"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"errors"
)

var (
	versionDriver *versioningMock
	strategy *strategyMock
	db *sql.DB
	dbMock sqlmock.Sqlmock
)

func setup(t *testing.T) {
	versionDriver = new(versioningMock)
	strategy = new(strategyMock)
	db, dbMock, _ = sqlmock.New()
	assert.NotNil(t, db)
	Register("fake", versionDriver)
}

func tearsDown(*testing.T) {
	versionDrivers = make(map[string]VersioningDriver)
	db.Close()
}

func TestRegister(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	registeredDriver, _ := versionDrivers["fake"]
	assert.Equal(t, versionDriver , registeredDriver, "Driver was not registered")
}

func TestRegisterDuplicated(t *testing.T) {
	setup(t)
	defer tearsDown(t)
	defer func() {
		if r:= recover(); r == nil {
			t.Error("Duplicate registering does not panic")
		}
	}()
	mockDriver := new(versioningMock)
	Register("fake", mockDriver)
}

func TestRegisterNilDriver(t *testing.T)  {
	setup(t)
	defer tearsDown(t)
	defer func() {
		if r:= recover(); r == nil {
			t.Error("Registering nil does not panic")
		}
	}()
	Register("fake", nil)
}

func TestVersionedDatabaseCreation(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 1

	versionDriver.
		On("Version", db).Return(0, nil).
		On("SetVersion", db, dbVersion).Return(nil)

	dbMock.ExpectBegin()
	strategy.
		On("OnCreate", db).Return(nil)
	dbMock.ExpectCommit()

	err := VersionedDatabase("fake", db, dbVersion, strategy)
	assert.Nil(t, err, "VersionedDatabase must not return error on create")

	versionDriver.AssertExpectations(t)
	strategy.AssertExpectations(t)
}

func TestVersionedDatabaseCreationError(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 1

	versionDriver.
		On("Version", db).Return(0, nil)

	dbMock.ExpectBegin()
	strategy.
		On("OnCreate", db).Return(errors.New(""))
	dbMock.ExpectRollback()

	err := VersionedDatabase("fake", db, dbVersion, strategy)
	assert.NotNil(t, err, "Creation error not passed out")

	versionDriver.AssertExpectations(t)
	strategy.AssertExpectations(t)
}

func TestVersionedDatabaseUpdate(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 2

	versionDriver.
		On("Version", db).Return(dbVersion - 1, nil).
		On("SetVersion", db, dbVersion).Return(nil)

	dbMock.ExpectBegin()
	strategy.
		On("OnUpdate", db, dbVersion - 1).Return(nil)
	dbMock.ExpectRollback()

	err := VersionedDatabase("fake", db, dbVersion, strategy)
	assert.Nil(t, err, "VersionedDatabase must not return error on create")

	versionDriver.AssertExpectations(t)
	strategy.AssertExpectations(t)
}

func TestVersionedDatabaseUpdateError(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 2

	versionDriver.
		On("Version", db).Return(dbVersion - 1, nil)

	dbMock.ExpectBegin()
	strategy.
		On("OnUpdate", db, dbVersion - 1).Return(errors.New(""))
	dbMock.ExpectRollback()

	err := VersionedDatabase("fake", db, dbVersion, strategy)
	assert.NotNil(t, err, "Update error must be passed out")

	versionDriver.AssertExpectations(t)
	strategy.AssertExpectations(t)
}

func TestVersionedDatabaseUpToDate(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 1

	versionDriver.
	On("Version", db).Return(dbVersion, nil)

	dbMock.ExpectBegin()
	err := VersionedDatabase("fake", db, dbVersion, strategy)
	assert.Nil(t, err, "Up to date database does not return error")

	dbMock.ExpectRollback()
	versionDriver.AssertExpectations(t)
	strategy.AssertExpectations(t)
}

func TestVersionedDatabaseNilDb(t *testing.T) {
	setup(t)
	defer tearsDown(t)
	err := VersionedDatabase("fake", nil, 1, strategy)
	assert.NotNil(t, err, "An error must be returned when db is nil")
	strategy.AssertExpectations(t)
}

func TestVersionedDatabaseNilStrategy(t *testing.T) {
	setup(t)
	defer tearsDown(t)
	err := VersionedDatabase("fake", db, 1, nil)
	assert.NotNil(t, err, "An error must be returned when db is nil")
	strategy.AssertExpectations(t)
}

func TestVersionedDatabaseNotRegisteredDriver(t *testing.T) {
	setup(t)
	defer tearsDown(t)
	err := VersionedDatabase("not_registered", db, 1, strategy)
	assert.NotNil(t, err, "An error must be returned when db is nil")
	strategy.AssertExpectations(t)
}

/////////////////////////////////////////////////////
// Stubs
/////////////////////////////////////////////////////

type versioningMock struct {
	mock.Mock
}

func (m *versioningMock) Version(db *sql.DB) (int, error) {
	args := m.Called(db)
	return args.Int(0), args.Error(1)
}

func (m *versioningMock) SetVersion(db *sql.DB, version int) (error) {
	args := m.Called(db, version)
	return args.Error(0)
}

type strategyMock struct {
	mock.Mock
}

func (s *strategyMock) OnCreate(db *sql.DB) error  {
	return s.Called(db).Error(0)
}

func (s *strategyMock) OnUpdate(db *sql.DB, oldVersion int) error {
	return s.Called(db, oldVersion).Error(0)
}