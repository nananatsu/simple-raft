package sql

import (
	"fmt"
	"strconv"
)

type StmtType int

const (
	_ StmtType = iota
	CREATE_STMT
	SELECT_STMT
	INSERT_STMT
)

type Statment interface {
	GetStmtType() StmtType
}

type CompareOp int

const (
	EQ CompareOp = iota
	NE
	LT
	GT
	LE
	GE
)

func (o *CompareOp) Negate() {
	switch *o {
	case EQ:
		*o = NE
	case NE:
		*o = EQ
	case LT:
		*o = GE
	case GT:
		*o = LE
	case LE:
		*o = GT
	case GE:
		*o = LT
	}
}

type SelectStmt struct {
	Filed []*SelectField
	From  *SelectStmtFrom
	Where []BoolExpr
	Order []*OrderField
	Limit *SelectStmtLimit
}

func (*SelectStmt) GetStmtType() StmtType {
	return SELECT_STMT
}

type SelectField struct {
	Field string
	Alias string
}

type SelectStmtFrom struct {
	Table string
}

// type SelectStmtWhere struct {
// 	Field *WhereField
// 	And   *WhereField
// 	Or    *WhereField
// }

type BoolExpr interface {
	Prepare(fieldMapping map[string]int) error
	Filter(row *[]string) bool
	Negate()
}

type WhereExpr struct {
	Negation bool
	Cnf      []BoolExpr
}

func (w *WhereExpr) Negate() {
	w.Negation = !w.Negation
}

func (w *WhereExpr) Prepare(fieldMapping map[string]int) error {

	for _, be := range w.Cnf {
		err := be.Prepare(fieldMapping)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *WhereExpr) Filter(row *[]string) bool {
	filter := true
	for _, be := range w.Cnf {
		filter = filter && be.Filter(row)
	}

	if w.Negation {
		return !filter
	}
	return filter
}

type WhereField struct {
	Field   string
	Pos     int
	Value   string
	Compare CompareOp
}

func (w *WhereField) Negate() {
	w.Compare.Negate()
}

func (w *WhereField) Prepare(fieldMapping map[string]int) error {
	pos, exist := fieldMapping[w.Field]
	if !exist {
		_, err := strconv.Atoi(w.Field)
		if err != nil {
			return fmt.Errorf("查询列 %s 不存在", w.Field)
		}
		w.Pos = -1
		return nil
	}
	w.Pos = pos
	return nil
}

func (w *WhereField) Filter(row *[]string) bool {

	var value string
	if w.Pos == -1 {
		value = w.Field
	} else {
		value = (*row)[w.Pos]
	}

	switch w.Compare {
	case EQ:
		return w.Value == value
	case NE:
		return w.Value != value
	case LT:
		return w.Value < value
	case GT:
		return w.Value > value
	case LE:
		return w.Value <= value
	case GE:
		return w.Value >= value
	}
	return false
}

type OrderField struct {
	Field string
	Asc   bool
}

type SelectStmtLimit struct {
	Offset int
	Size   int
}

type InsertStmt struct {
	Into       string
	ColumnList []string
	ValueList  [][]string
}

func (*InsertStmt) GetStmtType() StmtType {
	return INSERT_STMT
}

type CreateStmt struct {
	Table  string
	Def    *TableDef
	Option *TableOption
}

func (*CreateStmt) GetStmtType() StmtType {
	return CREATE_STMT
}

type ColumnDataType int

const (
	_ ColumnDataType = iota
	VARCHAR
	INT
)

var typeMapping = map[string]ColumnDataType{
	"VARCHAR": VARCHAR,
	"INT":     INT,
}

type TableDef struct {
	TableId   int
	PKAsRowId bool
	RowId     int
	Column    []*ColumnDef
	Index     []*IndexDef
	Primary   *IndexDef
}

type ColumnDef struct {
	FieldName string
	FieldType ColumnDataType
	AllowNull bool
	Default   string
}

type IndexDef struct {
	IndexName  string
	Unique     bool
	Primary    bool
	IndexField []string
}

type TableOption struct {
}
