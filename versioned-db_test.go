package version

import "testing"
import (
	"database/sql"
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
)

var (
	strategy *versionStrategyMock
	scheme   *schemeMock
	db       *sql.DB
	dbMock   sqlmock.Sqlmock
)

var someError error = errors.New("SomeError")

func setup(t *testing.T) {
	strategy = new(versionStrategyMock)
	scheme = new(schemeMock)
	db, dbMock, _ = sqlmock.New()
	assert.NotNil(t, db)
	Register("fake", strategy)
}

func tearsDown(*testing.T) {
	versionDrivers = make(map[string]Strategy)
	db.Close()
}

func TestRegister(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	registeredDriver, _ := versionDrivers["fake"]
	assert.Equal(t, strategy, registeredDriver, "Driver was not registered")
}

func TestRegisterDuplicated(t *testing.T) {
	setup(t)
	defer tearsDown(t)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Duplicate registering does not panic")
		}
	}()
	mockDriver := new(versionStrategyMock)
	Register("fake", mockDriver)
}

func TestRegisterNilStrategy(t *testing.T) {
	setup(t)
	defer tearsDown(t)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Registering nil does not panic")
		}
	}()
	Register("fake", nil)
}

func TestSchemeCreation(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 1

	strategy.
		On("Version", db).Return(0, nil).
		On("SetVersion", db, dbVersion).Return(nil)

	dbMock.ExpectBegin()
	scheme.
		On("Version").Return(dbVersion).
		On("VersionStrategy").Return("fake").
		On("OnCreate", db).Return(nil)
	dbMock.ExpectCommit()

	err := PersistScheme(db, scheme)
	assert.Nil(t, err, "PersistScheme must not return error on create")

	strategy.AssertExpectations(t)
	scheme.AssertExpectations(t)
	err = dbMock.ExpectationsWereMet()
	if err != nil {
		t.Errorf("Expectations not met. Err %q", err)
	}
}

func TestSchemeCreationError(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 1

	strategy.
		On("Version", db).Return(0, nil)

	dbMock.ExpectBegin()
	scheme.
		On("Version").Return(dbVersion).
		On("VersionStrategy").Return("fake").
		On("OnCreate", db).Return(someError)
	dbMock.ExpectRollback()

	err := PersistScheme(db, scheme)
	assert.NotNil(t, err, "Creation error not passed out")

	strategy.AssertExpectations(t)
	scheme.AssertExpectations(t)
	err = dbMock.ExpectationsWereMet()
	if err != nil {
		t.Errorf("Expectations not met. Err %q", err)
	}
}

func TestVersionError(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 1

	strategy.
		On("Version", db).Return(0, nil).
		On("SetVersion", db, dbVersion).Return(someError)

	dbMock.ExpectBegin()
	scheme.
		On("Version").Return(dbVersion).
		On("VersionStrategy").Return("fake").
		On("OnCreate", db).Return(nil)
	dbMock.ExpectRollback()

	err := PersistScheme(db, scheme)
	assert.NotNil(t, err, "Version error not passed out")

	strategy.AssertExpectations(t)
	scheme.AssertExpectations(t)
	err = dbMock.ExpectationsWereMet()
	if err != nil {
		t.Errorf("Expectations not met. Err %q", err)
	}
}

func TestSchemeUpdate(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 2

	strategy.
		On("Version", db).Return(dbVersion-1, nil).
		On("SetVersion", db, dbVersion).Return(nil)

	dbMock.ExpectBegin()
	scheme.
		On("Version").Return(dbVersion).
		On("VersionStrategy").Return("fake").
		On("OnUpdate", db, dbVersion-1).Return(nil)
	dbMock.ExpectCommit()

	err := PersistScheme(db, scheme)
	assert.Nil(t, err, "PersistScheme must not return error on create")

	strategy.AssertExpectations(t)
	scheme.AssertExpectations(t)
}

func TestSchemeUpdateError(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 2

	strategy.
		On("Version", db).Return(dbVersion-1, nil)

	dbMock.ExpectBegin()
	scheme.
		On("Version").Return(dbVersion).
		On("VersionStrategy").Return("fake").
		On("OnUpdate", db, dbVersion-1).Return(someError)
	dbMock.ExpectRollback()

	err := PersistScheme(db, scheme)
	assert.NotNil(t, err, "Update error must be passed out")

	strategy.AssertExpectations(t)
	scheme.AssertExpectations(t)
}

func TestSchemeUpToDate(t *testing.T) {
	setup(t)
	defer tearsDown(t)

	dbVersion := 1

	strategy.
		On("Version", db).Return(dbVersion, nil)

	scheme.
		On("Version").Return(dbVersion).
		On("VersionStrategy").Return("fake")

	dbMock.ExpectBegin()
	err := PersistScheme(db, scheme)
	assert.Nil(t, err, "Up to date database does not return error")

	dbMock.ExpectRollback()
	strategy.AssertExpectations(t)
	scheme.AssertExpectations(t)
}

func TestPersistSchemeOnNilDb(t *testing.T) {
	setup(t)
	defer tearsDown(t)
	err := PersistScheme(nil, scheme)
	assert.NotNil(t, err, "An error must be returned when db is nil")
}

func TestPersistNilScheme(t *testing.T) {
	setup(t)
	defer tearsDown(t)
	err := PersistScheme(db, nil)
	assert.NotNil(t, err, "An error must be returned when scheme is nil")
	scheme.AssertExpectations(t)
}

func TestNegativeVersion(t *testing.T) {
	setup(t)
	defer tearsDown(t)
	scheme.On("Version").Return(-1)
	err := PersistScheme(db, scheme)
	assert.NotNil(t, err, "It must return err if a negative version is provided")
}

func TestPersistSchemeUsingUnregisteredStrategy(t *testing.T) {
	setup(t)
	defer tearsDown(t)
	scheme.
		On("VersionStrategy").Return("not_registered").
		On("Version").Return(1)
	err := PersistScheme(db, scheme)
	assert.NotNil(t, err, "An error must be returned when a scheme is not registered")
	scheme.AssertExpectations(t)
}

/////////////////////////////////////////////////////
// Stubs
/////////////////////////////////////////////////////

type versionStrategyMock struct {
	mock.Mock
}

func (m *versionStrategyMock) Version(db *sql.DB) (int, error) {
	args := m.Called(db)
	return args.Int(0), args.Error(1)
}

func (m *versionStrategyMock) SetVersion(db *sql.DB, version int) error {
	args := m.Called(db, version)
	return args.Error(0)
}

type schemeMock struct {
	mock.Mock
}

func (s *schemeMock) Version() int {
	return s.Called().Int(0)
}

func (s *schemeMock) VersionStrategy() string {
	return s.Called().String(0)
}

func (s *schemeMock) OnCreate(db *sql.DB) error {
	return s.Called(db).Error(0)
}

func (s *schemeMock) OnUpdate(db *sql.DB, oldVersion int) error {
	return s.Called(db, oldVersion).Error(0)
}
