// go run yacc/goyacc.go -o yyParser.go -v yacc.output  sql.y
%{
package sql

import (
    "fmt"
    "strconv"
)

%}

%union {
    str string
    
    stmt Statment
    stmtList []Statment
    
    createStmt *CreateStmt
    tableDef *TableDef
    columnDef *ColumnDef
    indexDef *IndexDef
    tableOption *TableOption
    columnDataType ColumnDataType

    selectStmt *SelectStmt
    selectFieldList SelectFieldList
    selectStmtFrom *SelectStmtFrom
    selectStmtWhere *SelectStmtWhere
    orderFieldList OrderFieldList
    selectStmtLimit *SelectStmtLimit
    compareOp CompareOp

    insertStmt *InsertStmt
    valueList [][]string

    boolean bool
    strList []string
}

%token <str> 
    CREATE "CREATE"
    TABLE "TABLE"
    PRIMARY "PRIMARY"
    UNIQUE "UNIQUE"
    INDEX "INDEX"
    KEY "KEY"
    CONSTRAINT "CONSTRAINT"
    NOT "NOT"
    NULL "NULL"
    DEFAULT "DEFAULT"
    SELECT "SELECT" 
    FROM "FROM" 
    WHERE "WHERE" 
    ORDER "ORDER"
    BY "BY" 
    LIMIT "LIMIT" 
    OFFSET "OFFSET" 
    ASC "ASC" 
    DESC "DESC" 
    AND "AND" 
    OR "OR"
    INSERT "INSERT"
    INTO "INTO"
    VALUE "VALUE"
    VALUES "VALUES"

%token <str> 
    COMP_NE "!="
    COMP_LE "<=" 
    COMP_GE ">=" 

%token <str> VARIABLE
%type <strList> VaribleList

%type <stmt> Stmt
%type <stmtList> StmtList

%type <createStmt> CreateStmt
%type <tableDef> TableDef
%type <columnDef> ColumnDef
%type <indexDef> IndexDef
%type <indexDef> PrimaryDef
%type <tableOption> TableOption
%type <columnDataType> ColumnDataType
%type <boolean> AllowNull
%type <str> DefaultValue

%type <selectStmt> SelectStmt
%type <selectFieldList> SelectFieldList   
%type <selectStmtFrom> SelectStmtFrom
%type <selectStmtWhere> SelectStmtWhere WhereTree
%type <orderFieldList> SelectStmtOrder OrderFieldList
%type <selectStmtLimit> SelectStmtLimit 
%type <compareOp> CompareOp
%type <boolean> Ascend

%type <insertStmt> InsertStmt
%type <str> InsertStmtInto
%type <strList> ColumnGroup ColumnList 
%type <valueList> ValueGroup InsertStmtValue

%left OR
%left AND
%left '+' '-'
%left '*' '/'

%start start

%%

start:
    StmtList

StmtList:
    Stmt
    {
        *stmt = append(*stmt, $1)
    }
    | StmtList Stmt
    {
        *stmt = append(*stmt, $2)
    }

Stmt:
    CreateStmt
    {
        $$ = Statment($1)
    }
    | SelectStmt
    {
        $$ = Statment($1)
    }
    | InsertStmt
    {
        $$ = Statment($1)
    }


CreateStmt:
    "CREATE" "TABLE" VARIABLE '(' TableDef ')' TableOption ';'
    {
        $$ = &CreateStmt{Table: $3, Def: $5, Option: $7}
    }

TableDef:
    ColumnDef
    {
        $$ = &TableDef{
            Column: []*ColumnDef{$1},
            Index: []*IndexDef{},
        }
    }
    | IndexDef
    {
        $$ = &TableDef{
            Column: []*ColumnDef{},
            Index: []*IndexDef{$1},
        }
    }
    | PrimaryDef
    {
        $$ = &TableDef{
            Column:[]*ColumnDef{},
            Index: []*IndexDef{},
            Primary: $1,
        }
    }
    | TableDef ',' ColumnDef
    {
        $$.Column = append( $$.Column, $3)
    }
    | TableDef ',' IndexDef
    {
        $$.Index = append( $$.Index, $3)
    }
    | TableDef ',' PrimaryDef
    {
        if $$.Primary == nil{
            $$.Primary = $3
        } else {
             __yyfmt__.Printf("重复定义主键 %v %v ", $$.Primary, $3)
            goto ret1
        }
    }

ColumnDef:
    VARIABLE ColumnDataType AllowNull DefaultValue
    {
        $$ = &ColumnDef{FieldName: $1, FieldType: $2, AllowNull: $3, Default: $4 }
    }

IndexDef:
     IndexKey VARIABLE '(' VaribleList  ')'
    {
        $$ = &IndexDef{IndexName: $2, IndexField: $4}
    }
    | "UNIQUE" IndexKey VARIABLE '(' VaribleList  ')'
    {
        $$ = &IndexDef{IndexName: $3, IndexField: $5, Unique: true}
    }

PrimaryDef:
    "PRIMARY" "KEY" '(' VaribleList  ')'
    {
        $$ = &IndexDef{Primary: true, IndexField: $4}
    }

IndexKey:
    "KEY"
    | "INDEX"

ColumnDataType:
    VARIABLE
    {
        t, ok := typeMapping[$1]

        if ok {
            $$ = t
        }else{
            __yyfmt__.Printf("不支持的数据类型 %s",$1)
            goto ret1
        }
    }

AllowNull:
    {
        $$ = true
    }
    | "NULL"
    {
        $$ = true
    }
    | "NOT" "NULL"
    {
        $$ = false
    }

DefaultValue:
    {
        $$ = ""
    }
    | "DEFAULT"
    {
        $$ = ""
    }
    | "DEFAULT" "NULL"
    {
         $$ = ""
    }
    | "DEFAULT" VARIABLE
    {
        $$ = $2
    }

TableOption:
    {
        $$ = nil
    }

InsertStmt:
    "INSERT" InsertStmtInto ColumnGroup InsertStmtValue ';'
    {
        $$ = &InsertStmt{Into: $2, ColumnList: $3, ValueList: $4}
    }

InsertStmtInto:
    VARIABLE
    {
        $$ = $1
    }
    | "INTO" VARIABLE
    {
        $$ = $2
    }

ColumnGroup:
    '(' ColumnList ')'
    {
        $$ = $2
    }

ColumnList:
    {
        $$ = nil
    }
    | VaribleList

InsertStmtValue:
    "VALUES" ValueGroup
    {
        $$ = $2
    }
    | "VALUE" ValueGroup
    {
        $$ = $2
    }

ValueGroup:
     '(' VaribleList ')'
    {
        $$ = [][]string{$2}
    }
    | ValueGroup ',' '(' VaribleList ')'
    {
        $$ = append($1, $4)
    }

SelectStmt:
    "SELECT" SelectFieldList SelectStmtLimit ';'
    {
        $$ = &SelectStmt{ Filed: $2 , Limit: $3}
    }
    |  "SELECT" SelectFieldList SelectStmtFrom SelectStmtWhere SelectStmtOrder SelectStmtLimit ';'
    {
        $$ = &SelectStmt{ Filed: $2, From: $3 ,Where: $4, Order: $5, Limit: $6 }
    }

SelectFieldList:
    VARIABLE
    {
        $$ = SelectFieldList{ &SelectField{ Field: $1 } }
    }
    | SelectFieldList ',' VARIABLE
    {
        $$ = append( $1, &SelectField{ Field: $3 } )
    }

SelectStmtFrom:
    "FROM" VARIABLE
    {
        $$ = &SelectStmtFrom{
            Table: $2,
        }
    }

SelectStmtWhere:
    {
        $$ = nil
    }
    | "WHERE" WhereTree
    {
        $$ = $2
    }

WhereTree:
    VARIABLE CompareOp VARIABLE
    {
        $$ = &SelectStmtWhere{ Field: &WhereField{Field:$1, Value:$3, Compare:$2} }
    }
    | WhereTree OR VARIABLE CompareOp VARIABLE  %prec OR
    {
        $1.Or = &WhereField{Field:$3, Value:$5, Compare:$4}
        $$ =  $1
    }
    | WhereTree AND VARIABLE CompareOp VARIABLE %prec AND
    {
        $1.And = &WhereField{Field:$3, Value:$5, Compare:$4}
        $$ =  $1
    }


CompareOp:
    '='
    {
        $$ = EQ
    }
    | '<'
    {
        $$ = LT
    }
    | '>'
    {
        $$ = GT
    }
    | "<="
    {
        $$ = LE
    }
    | ">="
    {
        $$ = GE
    }
    | "!="
    {
        $$ = NE
    }


SelectStmtOrder:
    {
        $$ = nil
    }
    | "ORDER" "BY" OrderFieldList 
    {
        $$ = $3
    }

OrderFieldList:
    VARIABLE Ascend
    {
        $$ = OrderFieldList{&OrderField{ Field: $1, Asc: $2 }}
    }
    | OrderFieldList ',' VARIABLE Ascend
    {
        $$ = append($1, &OrderField{ Field: $3, Asc: $4 })
    }

Ascend:
    {
        $$ = true
    }
    | "ASC"
    {
        $$ = true
    }
    | "DESC" {
        $$ = false
    }


SelectStmtLimit:
    {
        $$ = nil
    }
    | "LIMIT" VARIABLE
    {
        size,err :=  strconv.Atoi($2)
        if err != nil{
            fmt.Println(err)
            return -1
        }
        $$ = &SelectStmtLimit{Size: size }
    }
    | "LIMIT" VARIABLE ',' VARIABLE
    {   

        offset,err :=  strconv.Atoi($2)
        if err != nil{
            __yyfmt__.Println(err)
            goto ret1
        }
        size,err :=  strconv.Atoi($4)
        if err != nil{
            __yyfmt__.Println(err)
            goto ret1
        }
        $$ = &SelectStmtLimit{Offset: offset, Size: size }
    }
    | "LIMIT" VARIABLE "OFFSET" VARIABLE
    {
        offset,err :=  strconv.Atoi($4)
        if err != nil{
            __yyfmt__.Println(err)
            goto ret1
        }
        size,err :=  strconv.Atoi($2)
        if err != nil{
            __yyfmt__.Println(err)
            goto ret1
        }
        $$ = &SelectStmtLimit{Offset: offset, Size: size }
    }

VaribleList:
    VARIABLE
    {
        $$ = []string{ $1 }
    }
    | VaribleList ',' VARIABLE
    {
        $$ = append($1, $3)
    }


