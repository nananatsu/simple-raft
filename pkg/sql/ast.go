package sql

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

type SelectStmt struct {
	Filed SelectFieldList
	From  *SelectStmtFrom
	Where *SelectStmtWhere
	Order OrderFieldList
	Limit *SelectStmtLimit
}

func (*SelectStmt) GetStmtType() StmtType {
	return SELECT_STMT
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

func (*InsertStmt) GetStmtType() StmtType {
	return INSERT_STMT
}

type CreateStmt struct {
	Table   string
	TableId int
	Def     *TableDef
	Option  *TableOption
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
	Column  []*ColumnDef
	Index   []*IndexDef
	Primary *IndexDef
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
