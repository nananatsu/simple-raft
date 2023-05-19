// go run yacc/goyacc.go -o yyParser.go -v yacc.output  sql.y
%{
package sql

import (
	"strconv"
)

%}

%union {
    str string
    strList []string
    boolean bool
    
    stmt Statment
    stmtList []Statment
    
    createStmt *CreateStmt
    tableDef *TableDef
    columnDef *ColumnDef
    indexDef *IndexDef
    tableOption *TableOption
    columnDataType ColumnDataType

    selectStmt *SelectStmt
    sqlFunc *SqlFunction
    selectFieldList []*SelectField
    selectStmtFrom *SelectStmtFrom
    whereExprList []BoolExpr
    orderFieldList []*OrderField
    selectStmtLimit *SelectStmtLimit
    compareOp CompareOp

    insertStmt *InsertStmt
    valueList [][]string
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
    COUNT "COUNT"
    MAX "MAX"
    MIN "MIN"

%token <str> 
    COMP_NE "!="
    COMP_LE "<=" 
    COMP_GE ">=" 

%token <str> VARIABLE
%type <str> Expr
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
%type <sqlFunc> AggregateFunction
%type <selectFieldList> SelectFieldList   
%type <selectStmtFrom> SelectStmtFrom
%type <whereExprList> SelectStmtWhere WhereExprList
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
        $$ = append($$, $1)
    }
    | StmtList Stmt
    {
        $$ = append($$, $2)
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
    "CREATE" "TABLE" Expr '(' TableDef ')' TableOption ';'
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
    Expr ColumnDataType AllowNull DefaultValue
    {
        $$ = &ColumnDef{FieldName: $1, FieldType: $2, AllowNull: $3, Default: $4 }
    }

IndexDef:
     IndexKey Expr '(' VaribleList  ')'
    {
        $$ = &IndexDef{IndexName: $2, IndexField: $4}
    }
    | "UNIQUE" IndexKey Expr '(' VaribleList  ')'
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
    Expr
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
    | "DEFAULT" Expr
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
    Expr
    {
        $$ = $1
    }
    | "INTO" Expr
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
    Expr
    {
        $$ = []*SelectField{ &SelectField{ Field: $1 } }
    }
    | SelectFieldList ',' Expr
    {
        $$ = append( $1, &SelectField{ Field: $3 } )
    }
    | AggregateFunction
    {
        $$ = []*SelectField{ &SelectField{ Expr: $1 }}
    }
    | SelectFieldList ',' AggregateFunction
    {
        $$ = append( $1, &SelectField{ Expr: $3 } )
    }

AggregateFunction:
    "COUNT" '(' VARIABLE ')'
    {
        $$ = &SqlFunction{ Type: AGGREGATE_FUNCTION, Func: $1 , Args: []interface{}{$3} }
    }
    | "MAX" '(' VARIABLE ')'
    {
        $$ = &SqlFunction{ Type: AGGREGATE_FUNCTION, Func: $1 , Args: []interface{}{$3} }
    }
    | "MIN" '(' VARIABLE ')'
    {
        $$ = &SqlFunction{ Type: AGGREGATE_FUNCTION, Func: $1 , Args: []interface{}{$3} }
    }

SelectStmtFrom:
    "FROM" Expr
    {
        $$ = &SelectStmtFrom{
            Table: $2,
        }
    }

SelectStmtWhere:
    {
        $$ = nil
    }
    | "WHERE" WhereExprList
    {
        $$ = $2
    }

WhereExprList:
    Expr CompareOp Expr
    {
        $$ = []BoolExpr{ &WhereField{Field:$1, Value:$3, Compare:$2} }
    }
    | WhereExprList OR Expr CompareOp Expr  %prec OR
    {
        $4.Negate()
        filed := &WhereField{ Field:$3, Value:$5, Compare:$4 }
        if len($$) == 1{
            $$[0].Negate()
            $$ = append($$, filed)
            $$ = []BoolExpr{ &WhereExpr{ Negation: true, Cnf: $$ }  }
        }else{
            $$ = []BoolExpr{ &WhereExpr{ Negation: true, Cnf: []BoolExpr{ &WhereExpr{ Negation: true, Cnf: $$ } , filed} } }
        }
    }
    | WhereExprList AND Expr CompareOp Expr %prec AND
    {
        $$ = append($$, &WhereField{ Field:$3, Value:$5, Compare:$4} )
    }
    | WhereExprList OR '(' WhereExprList ')' %prec OR
    {
        if len($$) == 1{
            $$[0].Negate()
            $$ = append($$, &WhereExpr{ Negation: true, Cnf: $4 })
            $$ = []BoolExpr{ &WhereExpr{ Negation: true, Cnf: $$ }  }
        }else{
           $$ =[]BoolExpr{ &WhereExpr{ Negation: true, Cnf: []BoolExpr{ &WhereExpr{ Negation: true, Cnf: $$ } , &WhereExpr{ Negation: true, Cnf: $4 } } } }
        }
    }
    | WhereExprList AND '(' WhereExprList ')' %prec AND
    {
        $$ = append($$, $4... )
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
    Expr Ascend
    {
        $$ = []*OrderField{&OrderField{ Field: $1, Asc: $2 }}
    }
    | OrderFieldList ',' Expr Ascend
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
            yylex.Error(err.Error())
            goto ret1
        }
        $$ = &SelectStmtLimit{Size: size }
    }
    | "LIMIT" VARIABLE ',' VARIABLE
    {   

        offset,err :=  strconv.Atoi($2)
        if err != nil{
            yylex.Error(err.Error())
            goto ret1
        }
        size,err :=  strconv.Atoi($4)
        if err != nil{
            yylex.Error(err.Error())
            goto ret1
        }
        $$ = &SelectStmtLimit{Offset: offset, Size: size }
    }
    | "LIMIT" VARIABLE "OFFSET" VARIABLE
    {
        offset,err :=  strconv.Atoi($4)
        if err != nil{
            yylex.Error(err.Error())
            goto ret1
        }
        size,err :=  strconv.Atoi($2)
        if err != nil{
            yylex.Error(err.Error())
            goto ret1
        }
        $$ = &SelectStmtLimit{Offset: offset, Size: size }
    }

VaribleList:
    Expr
    {
        $$ = []string{ $1 }
    }
    | VaribleList ',' Expr
    {
        $$ = append($1, $3)
    }

Expr:
    VARIABLE
    {
        str,err := TrimQuote($1)
        if err != nil{
            yylex.Error(err.Error())
            goto ret1
        }
        $$ = str
    }
