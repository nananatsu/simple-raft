%{
package parser

import (
    "fmt"
    "strconv"
)

var stmt interface{}
var stmtType StmtType

func GetStmt() (interface{},StmtType){
    return stmt,stmtType
}

%}

%union {
    strVal string
    selectStmt *SelectStmt
    selectFieldList SelectFieldList
    selectStmtFrom *SelectStmtFrom
    selectStmtWhere *SelectStmtWhere
    orderFieldList OrderFieldList
    selectStmtLimit *SelectStmtLimit

    insertStmt *InsertStmt
    valueList [][]string

    compareOp CompareOp
    boolVal bool
    strList []string
}

%token <strVal> 
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

%token <strVal> 
    ne "!="
    le "<=" 
    ge ">=" 

%token <strVal> VAL

%type <selectStmt> SelectStmt
%type <selectFieldList> SelectFieldList   
%type <selectStmtFrom> SelectStmtFrom
%type <selectStmtWhere> SelectStmtWhere WhereTree
%type <orderFieldList> SelectStmtOrder OrderFieldList
%type <selectStmtLimit> SelectStmtLimit 
%type <compareOp> CompareOp
%type <boolVal> ASCEND

%type <insertStmt> InsertStmt
%type <strVal> InsertStmtInto
%type <strList> ColumnGroup ColumnList ValueList
%type <valueList> ValueGroup InsertStmtValue


%left OR
%left AND
%left '+' '-'
%left '*' '/'

%start start

%%

start:
    SelectStmt
    {
        stmt = $1
        stmtType = SELECT_STMT
    }
    | InsertStmt
    {
        stmt = $1
        stmtType = INSERT_STMT
    }

InsertStmt:
    "INSERT" InsertStmtInto ColumnGroup InsertStmtValue ';'
    {
        $$ = &InsertStmt{Into: $2, ColumnList: $3, ValueList: $4}
    }

InsertStmtInto:
    VAL
    {
        $$ = $1
    }
    | "INTO" VAL
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
    | VAL
    {
        $$ = []string{ $1 }
    }
    | ColumnList ',' VAL
    {
        $$ = append($1, $3)
    }

InsertStmtValue:
    "VALUES" ValueGroup
    {
        $$ = $2
    }
    | "VALUE"ValueGroup
    {
        $$ = $2
    }

ValueGroup:
     '(' ValueList ')'
    {
        $$ = [][]string{$2}
    }
    | ValueGroup ',' '(' ValueList ')'
    {
        $$ = append($1, $4)
    }

ValueList:
    VAL
    {
        $$ = []string{ $1 }
    }
    | ValueList ',' VAL
    {
        $$ = append($1, $3)
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
    VAL
    {
        $$ = SelectFieldList{ &SelectField{ Field: $1 } }
    }
    | SelectFieldList ',' VAL
    {
        $$ = append( $1, &SelectField{ Field: $3 } )
    }

SelectStmtFrom:
    "FROM" VAL
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
    VAL CompareOp VAL
    {
        $$ = &SelectStmtWhere{ Field: &WhereField{Field:$1, Value:$3, Compare:$2} }
    }
    | WhereTree OR VAL CompareOp VAL  %prec OR
    {
        $1.Or = &WhereField{Field:$3, Value:$5, Compare:$4}
        $$ =  $1
    }
    | WhereTree AND VAL CompareOp VAL %prec AND
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
    VAL ASCEND
    {
        $$ = OrderFieldList{&OrderField{ Field: $1, Asc: $2 }}
    }
    | OrderFieldList ',' VAL ASCEND
    {
        $$ = append($1, &OrderField{ Field: $3, Asc: $4 })
    }

ASCEND:
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
    | "LIMIT" VAL
    {
        size,err :=  strconv.Atoi($2)
        if err != nil{
            fmt.Println(err)
            return -1
        }
        $$ = &SelectStmtLimit{Size: size }
    }
    | "LIMIT" VAL ',' VAL
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
    | "LIMIT" VAL "OFFSET" VAL
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
    ;