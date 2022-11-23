package parser

type StmtType int

const (
	_ StmtType = iota
	SELECT_STMT
	INSERT_STMT
)

type CompareOp int

const (
	EQ CompareOp = iota
	NE
	LT
	GT
	LE
	GE
)

type SelectStmt struct {
	Filed SelectFieldList
	From  *SelectStmtFrom
	Where *SelectStmtWhere
	Order OrderFieldList
	Limit *SelectStmtLimit
}

type SelectFieldList []*SelectField

type SelectField struct {
	Field string
	Alias string
}

type SelectStmtFrom struct {
	Table string
}

type SelectStmtWhere struct {
	Field *WhereField
	And   *WhereField
	Or    *WhereField
}

type WhereField struct {
	Field   string
	Value   string
	Compare CompareOp
}

type OrderFieldList []*OrderField

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
