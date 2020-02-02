package sqlite

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jasonrogena/lightraft/configuration"
	_ "github.com/mattn/go-sqlite3"
	"github.com/olekukonko/tablewriter"
)

const NAME string = "sqlite"
const EXTENSION string = ".sqlite"

type Driver struct {
	db *sql.DB
}

func GetDBPath(config *configuration.Config, nodeIndex int) string {
	return config.Driver.PathPrefix + "-" + strconv.Itoa(nodeIndex) + EXTENSION
}

func NewDriver(dbPath string) (*Driver, error) {

	database, dbErr := sql.Open("sqlite3", dbPath)

	driver := &Driver{db: database}

	return driver, dbErr
}

func (driver *Driver) IsQueryWrite(query string) bool {
	return !strings.HasPrefix(strings.ToLower(query), "select ")
}

func (driver *Driver) RunWriteQuery(query string, args ...interface{}) (string, error) {
	result, resultErr := driver.RunRawWriteQuery(query, args...)
	if result == nil {
		return "", resultErr
	}

	return fmt.Sprintf("%d rows affected\n", result.RowsAffected), resultErr
}

func (driver *Driver) RunRawWriteQuery(query string, args ...interface{}) (sql.Result, error) {
	stmt, stmtErr := driver.db.Prepare(query)

	if stmtErr != nil {
		return nil, stmtErr
	}
	defer stmt.Close()

	result, resultErr := stmt.Exec(args...)

	return result, resultErr
}

func (driver *Driver) RunReadQuery(query string, args ...interface{}) (string, error) {
	result, resultErr := driver.RunSelectQuery(query, true, args...)

	if resultErr != nil {
		return "", resultErr
	}

	return result.(string), nil
}

func (driver *Driver) RunSelectQuery(query string, resultAsSingleString bool, args ...interface{}) (interface{}, error) {
	rows, rowsErr := driver.db.Query(query, args...)
	if rowsErr != nil {
		return nil, rowsErr
	}
	defer rows.Close()

	result := [][]interface{}{}
	resultAsStrings := [][]string{}
	columnNames := []string{}
	rowIndex := 0
	for rows.Next() {
		result = append(result, []interface{}{})

		columnTypes, columnTypesErr := rows.ColumnTypes()
		if columnTypesErr != nil {
			return nil, columnTypesErr
		}

		if resultAsSingleString {
			resultAsStrings = append(resultAsStrings, []string{})

			if rowIndex == 0 {
				cn, columnNamesErr := rows.Columns()
				if columnNamesErr != nil {
					return nil, columnNamesErr
				}
				columnNames = cn
			}

			if len(columnTypes) != len(columnNames) {
				return nil, fmt.Errorf("Number of column names (%d) doesn't match number of row values (%d)", len(columnNames), len(columnTypes))
			}
		}

		for _, curColumnType := range columnTypes {
			result[rowIndex] = append(result[rowIndex], driver.getColumnCarrier(curColumnType, resultAsSingleString))
		}

		scanErr := rows.Scan(result[rowIndex]...)
		if scanErr != nil {
			return nil, scanErr
		}

		if resultAsSingleString {
			for curIndex := range columnTypes {
				resultAsStrings[rowIndex] = append(resultAsStrings[rowIndex], *result[rowIndex][curIndex].(*string))
			}
		}

		rowIndex++
	}

	// Render the table of results as a string
	if resultAsSingleString {
		buf := new(bytes.Buffer)
		table := tablewriter.NewWriter(buf)
		table.SetHeader(columnNames)
		table.AppendBulk(resultAsStrings)
		table.SetAlignment(tablewriter.ALIGN_LEFT)
		table.SetAutoFormatHeaders(false)
		table.Render()

		return buf.String(), nil
	}

	return result, nil
}

func (driver *Driver) getColumnCarrier(columnType *sql.ColumnType, forceString bool) interface{} {
	if forceString {
		var carrier string
		return &carrier
	}

	switch strings.ToLower(columnType.DatabaseTypeName()) {
	case "integer":
		var carrier int64
		return &carrier
	case "real":
		var carrier float64
		return &carrier
	default:
		var carrier string
		return &carrier
	}
}
