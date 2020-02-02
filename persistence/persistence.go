package persistence

import (
	"fmt"

	"github.com/jasonrogena/lightraft/configuration"
	"github.com/jasonrogena/lightraft/persistence/sqlite"
)

type Database struct {
	driver databaseDriver
}

type databaseDriver interface {
	IsQueryWrite(query string) bool
	RunWriteQuery(query string, args ...interface{}) (string, error)
	RunReadQuery(query string, args ...interface{}) (string, error)
}

func NewDatabase(config *configuration.Config, nodeIndex int) (*Database, error) {
	database := &Database{}
	switch config.Driver.Name {
	case sqlite.NAME:
		driver, dbError := sqlite.NewDriver(sqlite.GetDBPath(config, nodeIndex))
		database.driver = driver
		return database, dbError
	}

	return database, fmt.Errorf("Could not find database with driver name set to %s", config.Driver.Name)
}

func (db *Database) RequiresConsensus(command string) bool {
	return db.driver.IsQueryWrite(command)
}

func (db *Database) Commit(command string) (string, error) {
	if db.driver.IsQueryWrite(command) {
		return db.driver.RunWriteQuery(command)
	}

	return db.driver.RunReadQuery(command)
}
