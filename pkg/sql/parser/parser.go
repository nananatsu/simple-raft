package parser

import (
	"fmt"
)

type Lexer struct {
	sql    string
	offset int
}

func (l *Lexer) Lex(lval *yySymType) int {

	start := l.offset
	end := len(l.sql)
	if l.offset >= end {
		return 0
	}

	for i := l.offset; i < len(l.sql); i++ {
		switch l.sql[i] {
		case byte(' '):
			end = i
		case byte('<'), byte('>'):
			end = i
			if start == end && i+1 < len(l.sql) && l.sql[i+1] == byte('=') {
				end += 2
			}
		case byte('='), byte(','), byte(';'), byte('('), byte(')'):
			end = i
			if start == end {
				end++
			}
		}

		if i == end {
			break
		}
	}

	l.offset = end
	for l.offset < len(l.sql) && l.sql[l.offset] == byte(' ') {
		l.offset++
	}

	token := l.sql[start:end]
	lval.strVal = token
	switch token {
	case "INSERT":
		return INSERT
	case "INTO":
		return INTO
	case "VALUE":
		return VALUE
	case "VALUES":
		return VALUES
	case "SELECT":
		return SELECT
	case "FROM":
		return FROM
	case "WHERE":
		return WHERE
	case "ORDER":
		return ORDER
	case "BY":
		return BY
	case "LIMIT":
		return LIMIT
	case "OFFSET":
		return OFFSET
	case "ASC":
		return ASC
	case "DESC":
		return DESC
	case "AND":
		return AND
	case "OR":
		return OR
	case "!=":
		return ne
	case "<=":
		return le
	case ">=":
		return ge
	case ">", "<", "=", ",", ";", "(", ")":
		return int(int8(token[0]))
	default:
		return VAL
	}

}

func (*Lexer) Error(s string) {
	fmt.Printf("parse sql get error: %+v ", s)
}

func Parse(sql string) {
	n := yyParse(&Lexer{sql: sql})

	if n == 0 {
		stmt, stmtType := GetStmt()

		if stmtType == SELECT_STMT {
			selectStmt, _ := stmt.(*SelectStmt)
			fmt.Printf(" select: %+v  from: %+v where: %+v order: %+v limit: %+v", selectStmt.Filed, selectStmt.From, selectStmt.Where, selectStmt.Order, selectStmt.Limit)
		} else if stmtType == INSERT_STMT {
			insertStmt, _ := stmt.(*InsertStmt)
			fmt.Printf(" insert into: %s column: %+v values: %+v ", insertStmt.Into, insertStmt.ColumnList, insertStmt.ValueList)
		}
	}
}
