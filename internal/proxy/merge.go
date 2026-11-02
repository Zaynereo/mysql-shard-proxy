package proxy

import (
	"fmt"

	"github.com/go-mysql-org/go-mysql/mysql"
)

func mergeRows(parts []*mysql.Result) (*mysql.Result, error) {
	if len(parts) == 0 {
		return &mysql.Result{}, nil
	}
	var names []string
	var rows [][]interface{}
	for i, part := range parts {
		if part == nil || !part.HasResultset() {
			return nil, reject(fmt.Sprintf("shard result %d is not a result set", i))
		}
		if i == 0 {
			names = make([]string, len(part.Fields))
			for c, f := range part.Fields {
				names[c] = string(f.Name)
			}
		} else if part.ColumnNumber() != len(names) {
			return nil, reject("shard result sets have different column counts")
		}
		for r := 0; r < part.RowNumber(); r++ {
			row := make([]interface{}, part.ColumnNumber())
			for c := 0; c < part.ColumnNumber(); c++ {
				val, err := part.GetValue(r, c)
				if err != nil {
					return nil, reject(err.Error())
				}
				row[c] = val
			}
			rows = append(rows, row)
		}
	}
	rs, err := mysql.BuildSimpleTextResultset(names, rows)
	if err != nil {
		return nil, reject(err.Error())
	}
	return mysql.NewResult(rs), nil
}

func mergeOK(parts []*mysql.Result, status uint16) *mysql.Result {
	out := &mysql.Result{Status: status}
	for _, part := range parts {
		if part == nil {
			continue
		}
		out.AffectedRows += part.AffectedRows
		if part.InsertId != 0 {
			out.InsertId = part.InsertId
		}
		out.Warnings += part.Warnings
	}
	return out
}
