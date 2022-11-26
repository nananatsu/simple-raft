// Code generated by goyacc - DO NOT EDIT.

package sql

import __yyfmt__ "fmt"

import (
	"strconv"
)

type yySymType struct {
	yys     int
	str     string
	strList []string
	boolean bool

	stmt     Statment
	stmtList []Statment

	createStmt     *CreateStmt
	tableDef       *TableDef
	columnDef      *ColumnDef
	indexDef       *IndexDef
	tableOption    *TableOption
	columnDataType ColumnDataType

	selectStmt      *SelectStmt
	selectFieldList []*SelectField
	selectStmtFrom  *SelectStmtFrom
	whereExprList   []BoolExpr
	orderFieldList  []*OrderField
	selectStmtLimit *SelectStmtLimit
	compareOp       CompareOp

	insertStmt *InsertStmt
	valueList  [][]string
}

type yyXError struct {
	state, xsym int
}

const (
	yyDefault  = 57375
	yyEofCode  = 57344
	AND        = 57365
	ASC        = 57363
	BY         = 57360
	COMP_GE    = 57373
	COMP_LE    = 57372
	COMP_NE    = 57371
	CONSTRAINT = 57352
	CREATE     = 57346
	DEFAULT    = 57355
	DESC       = 57364
	FROM       = 57357
	INDEX      = 57350
	INSERT     = 57367
	INTO       = 57368
	KEY        = 57351
	LIMIT      = 57361
	NOT        = 57353
	NULL       = 57354
	OFFSET     = 57362
	OR         = 57366
	ORDER      = 57359
	PRIMARY    = 57348
	SELECT     = 57356
	TABLE      = 57347
	UNIQUE     = 57349
	VALUE      = 57369
	VALUES     = 57370
	VARIABLE   = 57374
	WHERE      = 57358
	yyErrCode  = 57345

	yyMaxDepth = 200
	yyTabOfs   = -71
)

var (
	yyPrec = map[int]int{
		OR:  0,
		AND: 1,
		'+': 2,
		'-': 2,
		'*': 3,
		'/': 3,
	}

	yyXLAT = map[int]int{
		44:    0,  // ',' (43x)
		57374: 1,  // VARIABLE (41x)
		41:    2,  // ')' (37x)
		59:    3,  // ';' (33x)
		57385: 4,  // Expr (29x)
		57361: 5,  // LIMIT (21x)
		40:    6,  // '(' (13x)
		57344: 7,  // $end (11x)
		57346: 8,  // CREATE (11x)
		57367: 9,  // INSERT (11x)
		57356: 10, // SELECT (11x)
		57359: 11, // ORDER (10x)
		57365: 12, // AND (9x)
		57366: 13, // OR (9x)
		57355: 14, // DEFAULT (6x)
		57404: 15, // VaribleList (6x)
		57354: 16, // NULL (5x)
		60:    17, // '<' (4x)
		61:    18, // '=' (4x)
		62:    19, // '>' (4x)
		57373: 20, // COMP_GE (4x)
		57372: 21, // COMP_LE (4x)
		57371: 22, // COMP_NE (4x)
		57357: 23, // FROM (4x)
		57351: 24, // KEY (4x)
		57363: 25, // ASC (3x)
		57382: 26, // CompareOp (3x)
		57364: 27, // DESC (3x)
		57350: 28, // INDEX (3x)
		57387: 29, // IndexKey (3x)
		57353: 30, // NOT (3x)
		57358: 31, // WHERE (3x)
		57405: 32, // WhereExprList (3x)
		57377: 33, // Ascend (2x)
		57379: 34, // ColumnDef (2x)
		57383: 35, // CreateStmt (2x)
		57386: 36, // IndexDef (2x)
		57388: 37, // InsertStmt (2x)
		57348: 38, // PRIMARY (2x)
		57392: 39, // PrimaryDef (2x)
		57394: 40, // SelectStmt (2x)
		57396: 41, // SelectStmtLimit (2x)
		57399: 42, // Stmt (2x)
		57349: 43, // UNIQUE (2x)
		57369: 44, // VALUE (2x)
		57403: 45, // ValueGroup (2x)
		57370: 46, // VALUES (2x)
		57376: 47, // AllowNull (1x)
		57360: 48, // BY (1x)
		57378: 49, // ColumnDataType (1x)
		57380: 50, // ColumnGroup (1x)
		57381: 51, // ColumnList (1x)
		57384: 52, // DefaultValue (1x)
		57389: 53, // InsertStmtInto (1x)
		57390: 54, // InsertStmtValue (1x)
		57368: 55, // INTO (1x)
		57362: 56, // OFFSET (1x)
		57391: 57, // OrderFieldList (1x)
		57393: 58, // SelectFieldList (1x)
		57395: 59, // SelectStmtFrom (1x)
		57397: 60, // SelectStmtOrder (1x)
		57398: 61, // SelectStmtWhere (1x)
		57406: 62, // start (1x)
		57400: 63, // StmtList (1x)
		57347: 64, // TABLE (1x)
		57401: 65, // TableDef (1x)
		57402: 66, // TableOption (1x)
		57375: 67, // $default (0x)
		42:    68, // '*' (0x)
		43:    69, // '+' (0x)
		45:    70, // '-' (0x)
		47:    71, // '/' (0x)
		57352: 72, // CONSTRAINT (0x)
		57345: 73, // error (0x)
	}

	yySymNames = []string{
		"','",
		"VARIABLE",
		"')'",
		"';'",
		"Expr",
		"LIMIT",
		"'('",
		"$end",
		"CREATE",
		"INSERT",
		"SELECT",
		"ORDER",
		"AND",
		"OR",
		"DEFAULT",
		"VaribleList",
		"NULL",
		"'<'",
		"'='",
		"'>'",
		"COMP_GE",
		"COMP_LE",
		"COMP_NE",
		"FROM",
		"KEY",
		"ASC",
		"CompareOp",
		"DESC",
		"INDEX",
		"IndexKey",
		"NOT",
		"WHERE",
		"WhereExprList",
		"Ascend",
		"ColumnDef",
		"CreateStmt",
		"IndexDef",
		"InsertStmt",
		"PRIMARY",
		"PrimaryDef",
		"SelectStmt",
		"SelectStmtLimit",
		"Stmt",
		"UNIQUE",
		"VALUE",
		"ValueGroup",
		"VALUES",
		"AllowNull",
		"BY",
		"ColumnDataType",
		"ColumnGroup",
		"ColumnList",
		"DefaultValue",
		"InsertStmtInto",
		"InsertStmtValue",
		"INTO",
		"OFFSET",
		"OrderFieldList",
		"SelectFieldList",
		"SelectStmtFrom",
		"SelectStmtOrder",
		"SelectStmtWhere",
		"start",
		"StmtList",
		"TABLE",
		"TableDef",
		"TableOption",
		"$default",
		"'*'",
		"'+'",
		"'-'",
		"'/'",
		"CONSTRAINT",
		"error",
	}

	yyTokenLiteralStrings = map[int]string{
		57361: "LIMIT",
		57346: "CREATE",
		57367: "INSERT",
		57356: "SELECT",
		57359: "ORDER",
		57365: "AND",
		57366: "OR",
		57355: "DEFAULT",
		57354: "NULL",
		57373: ">=",
		57372: "<=",
		57371: "!=",
		57357: "FROM",
		57351: "KEY",
		57363: "ASC",
		57364: "DESC",
		57350: "INDEX",
		57353: "NOT",
		57358: "WHERE",
		57348: "PRIMARY",
		57349: "UNIQUE",
		57369: "VALUE",
		57370: "VALUES",
		57360: "BY",
		57368: "INTO",
		57362: "OFFSET",
		57347: "TABLE",
		57352: "CONSTRAINT",
	}

	yyReductions = map[int]struct{ xsym, components int }{
		0:  {0, 1},
		1:  {62, 1},
		2:  {63, 1},
		3:  {63, 2},
		4:  {42, 1},
		5:  {42, 1},
		6:  {42, 1},
		7:  {35, 8},
		8:  {65, 1},
		9:  {65, 1},
		10: {65, 1},
		11: {65, 3},
		12: {65, 3},
		13: {65, 3},
		14: {34, 4},
		15: {36, 5},
		16: {36, 6},
		17: {39, 5},
		18: {29, 1},
		19: {29, 1},
		20: {49, 1},
		21: {47, 0},
		22: {47, 1},
		23: {47, 2},
		24: {52, 0},
		25: {52, 1},
		26: {52, 2},
		27: {52, 2},
		28: {66, 0},
		29: {37, 5},
		30: {53, 1},
		31: {53, 2},
		32: {50, 3},
		33: {51, 0},
		34: {51, 1},
		35: {54, 2},
		36: {54, 2},
		37: {45, 3},
		38: {45, 5},
		39: {40, 4},
		40: {40, 7},
		41: {58, 1},
		42: {58, 3},
		43: {59, 2},
		44: {61, 0},
		45: {61, 2},
		46: {32, 3},
		47: {32, 5},
		48: {32, 5},
		49: {32, 5},
		50: {32, 5},
		51: {26, 1},
		52: {26, 1},
		53: {26, 1},
		54: {26, 1},
		55: {26, 1},
		56: {26, 1},
		57: {60, 0},
		58: {60, 3},
		59: {57, 2},
		60: {57, 4},
		61: {33, 0},
		62: {33, 1},
		63: {33, 1},
		64: {41, 0},
		65: {41, 2},
		66: {41, 4},
		67: {41, 4},
		68: {15, 1},
		69: {15, 3},
		70: {4, 1},
	}

	yyXErrors = map[yyXError]string{}

	yyParseTab = [134][]uint16{
		// 0
		{8: 78, 79, 80, 35: 75, 37: 77, 40: 76, 42: 74, 62: 72, 73},
		{7: 71},
		{7: 70, 78, 79, 80, 35: 75, 37: 77, 40: 76, 42: 204},
		{7: 69, 69, 69, 69},
		{7: 67, 67, 67, 67},
		// 5
		{7: 66, 66, 66, 66},
		{7: 65, 65, 65, 65},
		{64: 161},
		{1: 83, 4: 137, 53: 136, 55: 138},
		{1: 83, 4: 82, 58: 81},
		// 10
		{86, 3: 7, 5: 88, 23: 87, 41: 84, 59: 85},
		{30, 3: 30, 5: 30, 23: 30},
		{1, 1, 1, 1, 5: 1, 1, 11: 1, 1, 1, 1, 16: 1, 1, 1, 1, 1, 1, 1, 1, 25: 1, 27: 1, 30: 1, 1},
		{3: 135},
		{3: 27, 5: 27, 11: 27, 31: 97, 61: 96},
		// 15
		{1: 83, 4: 95},
		{1: 83, 4: 94},
		{1: 89},
		{90, 3: 6, 56: 91},
		{1: 93},
		// 20
		{1: 92},
		{3: 4},
		{3: 5},
		{3: 28, 5: 28, 11: 28, 31: 28},
		{29, 3: 29, 5: 29, 23: 29},
		// 25
		{3: 14, 5: 14, 11: 123, 60: 122},
		{1: 83, 4: 99, 32: 98},
		{3: 26, 5: 26, 11: 26, 109, 108},
		{17: 102, 101, 103, 105, 104, 106, 26: 100},
		{1: 83, 4: 107},
		// 30
		{1: 20},
		{1: 19},
		{1: 18},
		{1: 17},
		{1: 16},
		// 35
		{1: 15},
		{2: 25, 25, 5: 25, 11: 25, 25, 25},
		{1: 83, 4: 116, 6: 117},
		{1: 83, 4: 110, 6: 111},
		{17: 102, 101, 103, 105, 104, 106, 26: 114},
		// 40
		{1: 83, 4: 99, 32: 112},
		{2: 113, 12: 109, 108},
		{2: 21, 21, 5: 21, 11: 21, 21, 21},
		{1: 83, 4: 115},
		{2: 23, 23, 5: 23, 11: 23, 23, 23},
		// 45
		{17: 102, 101, 103, 105, 104, 106, 26: 120},
		{1: 83, 4: 99, 32: 118},
		{2: 119, 12: 109, 108},
		{2: 22, 22, 5: 22, 11: 22, 22, 22},
		{1: 83, 4: 121},
		// 50
		{2: 24, 24, 5: 24, 11: 24, 24, 24},
		{3: 7, 5: 88, 41: 133},
		{48: 124},
		{1: 83, 4: 126, 57: 125},
		{130, 3: 13, 5: 13},
		// 55
		{10, 3: 10, 5: 10, 25: 128, 27: 129, 33: 127},
		{12, 3: 12, 5: 12},
		{9, 3: 9, 5: 9},
		{8, 3: 8, 5: 8},
		{1: 83, 4: 131},
		// 60
		{10, 3: 10, 5: 10, 25: 128, 27: 129, 33: 132},
		{11, 3: 11, 5: 11},
		{3: 134},
		{7: 31, 31, 31, 31},
		{7: 32, 32, 32, 32},
		// 65
		{6: 141, 50: 140},
		{6: 41},
		{1: 83, 4: 139},
		{6: 40},
		{44: 150, 46: 149, 54: 148},
		// 70
		{1: 83, 38, 4: 144, 15: 143, 51: 142},
		{2: 147},
		{145, 2: 37},
		{3, 2: 3},
		{1: 83, 4: 146},
		// 75
		{2, 2: 2},
		{44: 39, 46: 39},
		{3: 160},
		{6: 152, 45: 159},
		{6: 152, 45: 151},
		// 80
		{155, 3: 35},
		{1: 83, 4: 144, 15: 153},
		{145, 2: 154},
		{34, 3: 34},
		{6: 156},
		// 85
		{1: 83, 4: 144, 15: 157},
		{145, 2: 158},
		{33, 3: 33},
		{155, 3: 36},
		{7: 42, 42, 42, 42},
		// 90
		{1: 83, 4: 162},
		{6: 163},
		{1: 83, 4: 168, 24: 172, 28: 173, 169, 34: 165, 36: 166, 38: 171, 167, 43: 170, 65: 164},
		{198, 2: 197},
		{63, 2: 63},
		// 95
		{62, 2: 62},
		{61, 2: 61},
		{1: 83, 4: 188, 49: 187},
		{1: 83, 4: 183},
		{24: 172, 28: 173, 178},
		// 100
		{24: 174},
		{1: 53},
		{1: 52},
		{6: 175},
		{1: 83, 4: 144, 15: 176},
		// 105
		{145, 2: 177},
		{54, 2: 54},
		{1: 83, 4: 179},
		{6: 180},
		{1: 83, 4: 144, 15: 181},
		// 110
		{145, 2: 182},
		{55, 2: 55},
		{6: 184},
		{1: 83, 4: 144, 15: 185},
		{145, 2: 186},
		// 115
		{56, 2: 56},
		{50, 2: 50, 14: 50, 16: 190, 30: 191, 47: 189},
		{51, 2: 51, 14: 51, 16: 51, 30: 51},
		{47, 2: 47, 14: 194, 52: 193},
		{49, 2: 49, 14: 49},
		// 120
		{16: 192},
		{48, 2: 48, 14: 48},
		{57, 2: 57},
		{46, 83, 46, 4: 196, 16: 195},
		{45, 2: 45},
		// 125
		{44, 2: 44},
		{3: 43, 66: 202},
		{1: 83, 4: 168, 24: 172, 28: 173, 169, 34: 199, 36: 200, 38: 171, 201, 43: 170},
		{60, 2: 60},
		{59, 2: 59},
		// 130
		{58, 2: 58},
		{3: 203},
		{7: 64, 64, 64, 64},
		{7: 68, 68, 68, 68},
	}
)

var yyDebug = 0

type yyLexer interface {
	Lex(lval *yySymType) int
	Error(s string)
}

type yyLexerEx interface {
	yyLexer
	Reduced(rule, state int, lval *yySymType) bool
}

func yySymName(c int) (s string) {
	x, ok := yyXLAT[c]
	if ok {
		return yySymNames[x]
	}

	if c < 0x7f {
		return __yyfmt__.Sprintf("%q", c)
	}

	return __yyfmt__.Sprintf("%d", c)
}

func yylex1(yylex yyLexer, lval *yySymType) (n int) {
	n = yylex.Lex(lval)
	if n <= 0 {
		n = yyEofCode
	}
	if yyDebug >= 3 {
		__yyfmt__.Printf("\nlex %s(%#x %d), lval: %+v\n", yySymName(n), n, n, lval)
	}
	return n
}

func yyParse(yylex yyLexer, stmt *[]Statment) int {
	const yyError = 73

	yyEx, _ := yylex.(yyLexerEx)
	var yyn int
	var yylval yySymType
	var yyVAL yySymType
	yyS := make([]yySymType, 200)

	Nerrs := 0   /* number of errors */
	Errflag := 0 /* error recovery flag */
	yyerrok := func() {
		if yyDebug >= 2 {
			__yyfmt__.Printf("yyerrok()\n")
		}
		Errflag = 0
	}
	_ = yyerrok
	yystate := 0
	yychar := -1
	var yyxchar int
	var yyshift int
	yyp := -1
	goto yystack

ret0:
	return 0

ret1:
	return 1

yystack:
	/* put a state and value onto the stack */
	yyp++
	if yyp >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyS[yyp] = yyVAL
	yyS[yyp].yys = yystate

yynewstate:
	if yychar < 0 {
		yylval.yys = yystate
		yychar = yylex1(yylex, &yylval)
		var ok bool
		if yyxchar, ok = yyXLAT[yychar]; !ok {
			yyxchar = len(yySymNames) // > tab width
		}
	}
	if yyDebug >= 4 {
		var a []int
		for _, v := range yyS[:yyp+1] {
			a = append(a, v.yys)
		}
		__yyfmt__.Printf("state stack %v\n", a)
	}
	row := yyParseTab[yystate]
	yyn = 0
	if yyxchar < len(row) {
		if yyn = int(row[yyxchar]); yyn != 0 {
			yyn += yyTabOfs
		}
	}
	switch {
	case yyn > 0: // shift
		yychar = -1
		yyVAL = yylval
		yystate = yyn
		yyshift = yyn
		if yyDebug >= 2 {
			__yyfmt__.Printf("shift, and goto state %d\n", yystate)
		}
		if Errflag > 0 {
			Errflag--
		}
		goto yystack
	case yyn < 0: // reduce
	case yystate == 1: // accept
		if yyDebug >= 2 {
			__yyfmt__.Println("accept")
		}
		goto ret0
	}

	if yyn == 0 {
		/* error ... attempt to resume parsing */
		switch Errflag {
		case 0: /* brand new error */
			if yyDebug >= 1 {
				__yyfmt__.Printf("no action for %s in state %d\n", yySymName(yychar), yystate)
			}
			msg, ok := yyXErrors[yyXError{yystate, yyxchar}]
			if !ok {
				msg, ok = yyXErrors[yyXError{yystate, -1}]
			}
			if !ok && yyshift != 0 {
				msg, ok = yyXErrors[yyXError{yyshift, yyxchar}]
			}
			if !ok {
				msg, ok = yyXErrors[yyXError{yyshift, -1}]
			}
			if yychar > 0 {
				ls := yyTokenLiteralStrings[yychar]
				if ls == "" {
					ls = yySymName(yychar)
				}
				if ls != "" {
					switch {
					case msg == "":
						msg = __yyfmt__.Sprintf("unexpected %s", ls)
					default:
						msg = __yyfmt__.Sprintf("unexpected %s, %s", ls, msg)
					}
				}
			}
			if msg == "" {
				msg = "syntax error"
			}
			yylex.Error(msg)
			Nerrs++
			fallthrough

		case 1, 2: /* incompletely recovered error ... try again */
			Errflag = 3

			/* find a state where "error" is a legal shift action */
			for yyp >= 0 {
				row := yyParseTab[yyS[yyp].yys]
				if yyError < len(row) {
					yyn = int(row[yyError]) + yyTabOfs
					if yyn > 0 { // hit
						if yyDebug >= 2 {
							__yyfmt__.Printf("error recovery found error shift in state %d\n", yyS[yyp].yys)
						}
						yystate = yyn /* simulate a shift of "error" */
						goto yystack
					}
				}

				/* the current p has no shift on "error", pop stack */
				if yyDebug >= 2 {
					__yyfmt__.Printf("error recovery pops state %d\n", yyS[yyp].yys)
				}
				yyp--
			}
			/* there is no state on the stack with an error shift ... abort */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery failed\n")
			}
			goto ret1

		case 3: /* no shift yet; clobber input char */
			if yyDebug >= 2 {
				__yyfmt__.Printf("error recovery discards %s\n", yySymName(yychar))
			}
			if yychar == yyEofCode {
				goto ret1
			}

			yychar = -1
			goto yynewstate /* try again in the same state */
		}
	}

	r := -yyn
	x0 := yyReductions[r]
	x, n := x0.xsym, x0.components
	yypt := yyp
	_ = yypt // guard against "declared and not used"

	yyp -= n
	if yyp+1 >= len(yyS) {
		nyys := make([]yySymType, len(yyS)*2)
		copy(nyys, yyS)
		yyS = nyys
	}
	yyVAL = yyS[yyp+1]

	/* consult goto table to find next state */
	exState := yystate
	yystate = int(yyParseTab[yyS[yyp].yys][x]) + yyTabOfs
	/* reduction by production r */
	if yyDebug >= 2 {
		__yyfmt__.Printf("reduce using rule %v (%s), and goto state %d\n", r, yySymNames[x], yystate)
	}

	switch r {
	case 2:
		{
			*stmt = append(*stmt, yyS[yypt-0].stmt)
		}
	case 3:
		{
			*stmt = append(*stmt, yyS[yypt-0].stmt)
		}
	case 4:
		{
			yyVAL.stmt = Statment(yyS[yypt-0].createStmt)
		}
	case 5:
		{
			yyVAL.stmt = Statment(yyS[yypt-0].selectStmt)
		}
	case 6:
		{
			yyVAL.stmt = Statment(yyS[yypt-0].insertStmt)
		}
	case 7:
		{
			yyVAL.createStmt = &CreateStmt{Table: yyS[yypt-5].str, Def: yyS[yypt-3].tableDef, Option: yyS[yypt-1].tableOption}
		}
	case 8:
		{
			yyVAL.tableDef = &TableDef{
				Column: []*ColumnDef{yyS[yypt-0].columnDef},
				Index:  []*IndexDef{},
			}
		}
	case 9:
		{
			yyVAL.tableDef = &TableDef{
				Column: []*ColumnDef{},
				Index:  []*IndexDef{yyS[yypt-0].indexDef},
			}
		}
	case 10:
		{
			yyVAL.tableDef = &TableDef{
				Column:  []*ColumnDef{},
				Index:   []*IndexDef{},
				Primary: yyS[yypt-0].indexDef,
			}
		}
	case 11:
		{
			yyVAL.tableDef.Column = append(yyVAL.tableDef.Column, yyS[yypt-0].columnDef)
		}
	case 12:
		{
			yyVAL.tableDef.Index = append(yyVAL.tableDef.Index, yyS[yypt-0].indexDef)
		}
	case 13:
		{
			if yyVAL.tableDef.Primary == nil {
				yyVAL.tableDef.Primary = yyS[yypt-0].indexDef
			} else {
				__yyfmt__.Printf("重复定义主键 %v %v ", yyVAL.tableDef.Primary, yyS[yypt-0].indexDef)
				goto ret1
			}
		}
	case 14:
		{
			yyVAL.columnDef = &ColumnDef{FieldName: yyS[yypt-3].str, FieldType: yyS[yypt-2].columnDataType, AllowNull: yyS[yypt-1].boolean, Default: yyS[yypt-0].str}
		}
	case 15:
		{
			yyVAL.indexDef = &IndexDef{IndexName: yyS[yypt-3].str, IndexField: yyS[yypt-1].strList}
		}
	case 16:
		{
			yyVAL.indexDef = &IndexDef{IndexName: yyS[yypt-3].str, IndexField: yyS[yypt-1].strList, Unique: true}
		}
	case 17:
		{
			yyVAL.indexDef = &IndexDef{Primary: true, IndexField: yyS[yypt-1].strList}
		}
	case 20:
		{
			t, ok := typeMapping[yyS[yypt-0].str]

			if ok {
				yyVAL.columnDataType = t
			} else {
				__yyfmt__.Printf("不支持的数据类型 %s", yyS[yypt-0].str)
				goto ret1
			}
		}
	case 21:
		{
			yyVAL.boolean = true
		}
	case 22:
		{
			yyVAL.boolean = true
		}
	case 23:
		{
			yyVAL.boolean = false
		}
	case 24:
		{
			yyVAL.str = ""
		}
	case 25:
		{
			yyVAL.str = ""
		}
	case 26:
		{
			yyVAL.str = ""
		}
	case 27:
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 28:
		{
			yyVAL.tableOption = nil
		}
	case 29:
		{
			yyVAL.insertStmt = &InsertStmt{Into: yyS[yypt-3].str, ColumnList: yyS[yypt-2].strList, ValueList: yyS[yypt-1].valueList}
		}
	case 30:
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 31:
		{
			yyVAL.str = yyS[yypt-0].str
		}
	case 32:
		{
			yyVAL.strList = yyS[yypt-1].strList
		}
	case 33:
		{
			yyVAL.strList = nil
		}
	case 35:
		{
			yyVAL.valueList = yyS[yypt-0].valueList
		}
	case 36:
		{
			yyVAL.valueList = yyS[yypt-0].valueList
		}
	case 37:
		{
			yyVAL.valueList = [][]string{yyS[yypt-1].strList}
		}
	case 38:
		{
			yyVAL.valueList = append(yyS[yypt-4].valueList, yyS[yypt-1].strList)
		}
	case 39:
		{
			yyVAL.selectStmt = &SelectStmt{Filed: yyS[yypt-2].selectFieldList, Limit: yyS[yypt-1].selectStmtLimit}
		}
	case 40:
		{
			yyVAL.selectStmt = &SelectStmt{Filed: yyS[yypt-5].selectFieldList, From: yyS[yypt-4].selectStmtFrom, Where: yyS[yypt-3].whereExprList, Order: yyS[yypt-2].orderFieldList, Limit: yyS[yypt-1].selectStmtLimit}
		}
	case 41:
		{
			yyVAL.selectFieldList = []*SelectField{&SelectField{Field: yyS[yypt-0].str}}
		}
	case 42:
		{
			yyVAL.selectFieldList = append(yyS[yypt-2].selectFieldList, &SelectField{Field: yyS[yypt-0].str})
		}
	case 43:
		{
			yyVAL.selectStmtFrom = &SelectStmtFrom{
				Table: yyS[yypt-0].str,
			}
		}
	case 44:
		{
			yyVAL.whereExprList = nil
		}
	case 45:
		{
			yyVAL.whereExprList = yyS[yypt-0].whereExprList
		}
	case 46:
		{
			yyVAL.whereExprList = []BoolExpr{&WhereField{Field: yyS[yypt-2].str, Value: yyS[yypt-0].str, Compare: yyS[yypt-1].compareOp}}
		}
	case 47:
		{
			yyS[yypt-1].compareOp.Negate()
			filed := &WhereField{Field: yyS[yypt-2].str, Value: yyS[yypt-0].str, Compare: yyS[yypt-1].compareOp}
			if len(yyVAL.whereExprList) == 1 {
				yyVAL.whereExprList[0].Negate()
				yyVAL.whereExprList = append(yyVAL.whereExprList, filed)
				yyVAL.whereExprList = []BoolExpr{&WhereExpr{Negation: true, Cnf: yyVAL.whereExprList}}
			} else {
				yyVAL.whereExprList = []BoolExpr{&WhereExpr{Negation: true, Cnf: []BoolExpr{&WhereExpr{Negation: true, Cnf: yyVAL.whereExprList}, filed}}}
			}
		}
	case 48:
		{
			yyVAL.whereExprList = append(yyVAL.whereExprList, &WhereField{Field: yyS[yypt-2].str, Value: yyS[yypt-0].str, Compare: yyS[yypt-1].compareOp})
		}
	case 49:
		{
			if len(yyVAL.whereExprList) == 1 {
				yyVAL.whereExprList[0].Negate()
				yyVAL.whereExprList = append(yyVAL.whereExprList, &WhereExpr{Negation: true, Cnf: yyS[yypt-1].whereExprList})
				yyVAL.whereExprList = []BoolExpr{&WhereExpr{Negation: true, Cnf: yyVAL.whereExprList}}
			} else {
				yyVAL.whereExprList = []BoolExpr{&WhereExpr{Negation: true, Cnf: []BoolExpr{&WhereExpr{Negation: true, Cnf: yyVAL.whereExprList}, &WhereExpr{Negation: true, Cnf: yyS[yypt-1].whereExprList}}}}
			}
		}
	case 50:
		{
			yyVAL.whereExprList = append(yyVAL.whereExprList, yyS[yypt-1].whereExprList...)
		}
	case 51:
		{
			yyVAL.compareOp = EQ
		}
	case 52:
		{
			yyVAL.compareOp = LT
		}
	case 53:
		{
			yyVAL.compareOp = GT
		}
	case 54:
		{
			yyVAL.compareOp = LE
		}
	case 55:
		{
			yyVAL.compareOp = GE
		}
	case 56:
		{
			yyVAL.compareOp = NE
		}
	case 57:
		{
			yyVAL.orderFieldList = nil
		}
	case 58:
		{
			yyVAL.orderFieldList = yyS[yypt-0].orderFieldList
		}
	case 59:
		{
			yyVAL.orderFieldList = []*OrderField{&OrderField{Field: yyS[yypt-1].str, Asc: yyS[yypt-0].boolean}}
		}
	case 60:
		{
			yyVAL.orderFieldList = append(yyS[yypt-3].orderFieldList, &OrderField{Field: yyS[yypt-1].str, Asc: yyS[yypt-0].boolean})
		}
	case 61:
		{
			yyVAL.boolean = true
		}
	case 62:
		{
			yyVAL.boolean = true
		}
	case 63:
		{
			yyVAL.boolean = false
		}
	case 64:
		{
			yyVAL.selectStmtLimit = nil
		}
	case 65:
		{
			size, err := strconv.Atoi(yyS[yypt-0].str)
			if err != nil {
				yylex.Error(err.Error())
				goto ret1
			}
			yyVAL.selectStmtLimit = &SelectStmtLimit{Size: size}
		}
	case 66:
		{

			offset, err := strconv.Atoi(yyS[yypt-2].str)
			if err != nil {
				yylex.Error(err.Error())
				goto ret1
			}
			size, err := strconv.Atoi(yyS[yypt-0].str)
			if err != nil {
				yylex.Error(err.Error())
				goto ret1
			}
			yyVAL.selectStmtLimit = &SelectStmtLimit{Offset: offset, Size: size}
		}
	case 67:
		{
			offset, err := strconv.Atoi(yyS[yypt-0].str)
			if err != nil {
				yylex.Error(err.Error())
				goto ret1
			}
			size, err := strconv.Atoi(yyS[yypt-2].str)
			if err != nil {
				yylex.Error(err.Error())
				goto ret1
			}
			yyVAL.selectStmtLimit = &SelectStmtLimit{Offset: offset, Size: size}
		}
	case 68:
		{
			yyVAL.strList = []string{yyS[yypt-0].str}
		}
	case 69:
		{
			yyVAL.strList = append(yyS[yypt-2].strList, yyS[yypt-0].str)
		}
	case 70:
		{
			str, err := TrimQuote(yyS[yypt-0].str)
			if err != nil {
				yylex.Error(err.Error())
				goto ret1
			}
			yyVAL.str = str
		}

	}

	if yyEx != nil && yyEx.Reduced(r, exState, &yyVAL) {
		return -1
	}
	goto yystack /* stack new state and value */
}