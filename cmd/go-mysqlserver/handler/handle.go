package handler

import (
	//"bytes"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	// "github.com/go-mysql-org/go-mysql/replication"
	// "github.com/go-mysql-org/go-mysql/stmt"
	// "github.com/go-mysql-org/go-mysql/server"
	// "github.com/go-mysql-org/go-mysql/utils"
)

// MyHandler is a mostly empty implementation for demonstration purposes
type MyHandler struct {
	currentDB string
}

// UseDB is called for COM_INIT_DB
func (h *MyHandler) UseDB(dbName string) error {
	log.Printf("Received: UseDB %s", dbName)
	h.currentDB = dbName
	return nil
}

// HandleQuery is called for COM_QUERY
func (h *MyHandler) HandleQuery(query string) (*mysql.Result, error) {
	log.Printf("Received: Query: %s", query)

	normalized := normalizeQuery(query)

	// These two queries are implemented for minimal support for MySQL Shell
	if strings.EqualFold(normalized, `SET NAMES 'utf8mb4'`) {
		return nil, nil
	}
	if strings.EqualFold(normalized, `select concat(@@version, ' ', @@version_comment)`) {
		r, err := mysql.BuildSimpleResultset([]string{"concat(@@version, ' ', @@version_comment)"}, [][]interface{}{
			{"8.0.11"},
		}, false)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(r), nil
	}

	if strings.EqualFold(normalized, "show databases") {
		r, err := mysql.BuildSimpleResultset([]string{"Database"}, [][]interface{}{
			{"userdb"},
		}, false)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(r), nil
	}

	if strings.EqualFold(normalized, "select database()") || strings.EqualFold(normalized, "select schema()") {
		var current interface{}
		if h.currentDB != "" {
			current = h.currentDB
		}
		r, err := mysql.BuildSimpleResultset([]string{"DATABASE()"}, [][]interface{}{
			{current},
		}, false)
		if err != nil {
			return nil, err
		}
		return mysql.NewResult(r), nil
	}

	if strings.HasPrefix(strings.ToLower(normalized), "use ") {
		dbName := strings.TrimSpace(normalized[len("use "):])
		dbName = strings.Trim(dbName, "`\"'")
		if dbName != "" {
			h.currentDB = dbName
		}
		return mysql.NewResultReserveResultset(0), nil
	}

	if strings.EqualFold(normalized, "select * from users") {
		rs := mysql.NewResultset(5)
		rs.Fields[0] = &mysql.Field{
			Name:    []byte("userid"),
			Type:    mysql.MYSQL_TYPE_LONGLONG,
			Charset: 63,
			Flag:    mysql.BINARY_FLAG | mysql.NOT_NULL_FLAG,
		}
		rs.Fields[1] = &mysql.Field{
			Name:    []byte("username"),
			Type:    mysql.MYSQL_TYPE_VAR_STRING,
			Charset: 33,
		}
		rs.Fields[2] = &mysql.Field{
			Name:    []byte("age"),
			Type:    mysql.MYSQL_TYPE_LONGLONG,
			Charset: 63,
			Flag:    mysql.BINARY_FLAG | mysql.NOT_NULL_FLAG,
		}
		rs.Fields[3] = &mysql.Field{
			Name:    []byte("gender"),
			Type:    mysql.MYSQL_TYPE_VAR_STRING,
			Charset: 33,
		}
		rs.Fields[4] = &mysql.Field{
			Name:    []byte("create_date"),
			Type:    mysql.MYSQL_TYPE_DATETIME,
			Charset: 33,
		}

		rs.FieldNames["userid"] = 0
		rs.FieldNames["username"] = 1
		rs.FieldNames["age"] = 2
		rs.FieldNames["gender"] = 3
		rs.FieldNames["create_date"] = 4

		rows := make([][]mysql.FieldValue, 0, 10)
		rs.RowDatas = make([]mysql.RowData, 0, 10)
		base := time.Date(2025, 12, 1, 9, 0, 0, 0, time.UTC)
		for i := 1; i <= 10; i++ {
			gender := "M"
			if i%2 == 0 {
				gender = "F"
			}
			rowValues := []mysql.FieldValue{
				mysql.NewFieldValue(mysql.FieldValueTypeSigned, uint64(i), nil),
				mysql.NewFieldValue(mysql.FieldValueTypeString, 0, []byte(fmt.Sprintf("user%02d", i))),
				mysql.NewFieldValue(mysql.FieldValueTypeSigned, uint64(20+i), nil),
				mysql.NewFieldValue(mysql.FieldValueTypeString, 0, []byte(gender)),
				mysql.NewFieldValue(
					mysql.FieldValueTypeString,
					0,
					[]byte(base.AddDate(0, 0, i).Format(time.DateTime)),
				),
			}
			rows = append(rows, rowValues)
			rowData, err := buildTextRow(rowValues)
			if err != nil {
				return nil, err
			}
			rs.RowDatas = append(rs.RowDatas, rowData)
		}

		rs.Values = rows
		return mysql.NewResult(rs), nil
	}

	return nil, fmt.Errorf("not supported now")
}

// HandleFieldList is called for COM_FIELD_LIST packets
// Note that COM_FIELD_LIST has been deprecated since MySQL 5.7.11
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_field_list.html
func (h *MyHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	log.Printf("Received: FieldList: table=%s, fieldWildcard:%s", table, fieldWildcard)
	return nil, fmt.Errorf("not supported now")
}

// HandleStmtPrepare is called for COM_STMT_PREPARE
func (h *MyHandler) HandleStmtPrepare(query string) (int, int, interface{}, error) {
	log.Printf("Received: StmtPrepare: %s", query)
	return 0, 0, nil, fmt.Errorf("not supported now")
}

// 'context' isn't used but replacing it with `_` would remove important information for who
// wants to extend this later.
//revive:disable:unused-parameter

// HandleStmtExecute is called for COM_STMT_EXECUTE
func (h *MyHandler) HandleStmtExecute(context interface{}, query string, args []interface{}) (*mysql.Result, error) {
	log.Printf("Received: StmtExecute: %s (args: %v)", query, args)
	return nil, fmt.Errorf("not supported now")
}

// HandleStmtClose is called for COM_STMT_CLOSE
func (h *MyHandler) HandleStmtClose(context interface{}) error {
	log.Println("Received: StmtClose")
	return nil
}

// HandleOtherCommand is called for commands not handled elsewhere
func (h *MyHandler) HandleOtherCommand(cmd byte, data []byte) error {
	log.Printf("Received: OtherCommand: cmd=%x, data=%x", cmd, data)
	return mysql.NewError(
		mysql.ER_UNKNOWN_ERROR,
		fmt.Sprintf("command %d is not supported now", cmd),
	)
}

func normalizeQuery(query string) string {
	trimmed := strings.TrimSpace(query)
	for strings.HasSuffix(trimmed, ";") {
		trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, ";"))
	}
	return trimmed
}

func buildTextRow(values []mysql.FieldValue) (mysql.RowData, error) {
	row := make(mysql.RowData, 0, len(values)*8)
	for _, v := range values {
		switch v.Type {
		case mysql.FieldValueTypeNull:
			row = append(row, 0xfb)
		case mysql.FieldValueTypeUnsigned:
			row = append(row, mysql.PutLengthEncodedString(strconv.AppendUint(nil, v.AsUint64(), 10))...)
		case mysql.FieldValueTypeSigned:
			row = append(row, mysql.PutLengthEncodedString(strconv.AppendInt(nil, v.AsInt64(), 10))...)
		case mysql.FieldValueTypeFloat:
			row = append(row, mysql.PutLengthEncodedString(
				strconv.AppendFloat(nil, v.AsFloat64(), 'f', -1, 64),
			)...)
		case mysql.FieldValueTypeString:
			row = append(row, mysql.PutLengthEncodedString(v.AsString())...)
		default:
			return nil, fmt.Errorf("unsupported field value type %d", v.Type)
		}
	}

	return row, nil
}
