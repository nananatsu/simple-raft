package sql

import (
	"fmt"
	"strings"
)

type Lexer struct {
	sql    string
	offset int
	errs   []string
}

var SqlTokenMapping map[string]int

func init() {

	singleCharToken := []string{">", "<", "=", ",", ";", "(", ")"}
	SqlTokenMapping = make(map[string]int, len(yyTokenLiteralStrings)+len(singleCharToken))

	for k, v := range yyTokenLiteralStrings {
		SqlTokenMapping[v] = k
		SqlTokenMapping[strings.ToLower(v)] = k
	}

	for _, v := range singleCharToken {
		SqlTokenMapping[v] = int(int8(v[0]))
	}

}

func (l *Lexer) Lex(lval *yySymType) int {

	start := l.offset
	end := len(l.sql)
	if l.offset >= end {
		return 0
	}

	prevQuote := false
	prevBacktick := false
	prevSingleQuotes := false
	prevDoubleQuotes := false
	for i := l.offset; i < len(l.sql); i++ {
		switch l.sql[i] {
		case '\\':
			continue
		case '\'':
			prevSingleQuotes = !prevSingleQuotes
			prevQuote = prevBacktick || prevSingleQuotes || prevDoubleQuotes
		case '"':
			prevDoubleQuotes = !prevDoubleQuotes
			prevQuote = prevBacktick || prevSingleQuotes || prevDoubleQuotes
		case '`':
			prevBacktick = !prevBacktick
			prevQuote = prevBacktick || prevSingleQuotes || prevDoubleQuotes
		}

		if !prevQuote {
			switch l.sql[i] {
			case ' ', '\n', '\t':
				end = i
			case '<', '>', '!':
				end = i
				if start == end {
					if i+1 < len(l.sql) && l.sql[i+1] == '=' {
						end += 2
					} else {
						end++
					}
				}
			case '=', ',', ';', '(', ')':
				end = i
				if start == end {
					end++
				}
			}
		}

		if i == end {
			break
		}
	}

	l.offset = end
	for l.offset < len(l.sql) {
		char := l.sql[l.offset]
		if char == ' ' || char == '\n' || char == '\t' {
			l.offset++
		} else {
			break
		}
	}

	token := l.sql[start:end]
	lval.str = token

	// fmt.Printf("token: %s \n", token)

	num, ok := SqlTokenMapping[token]
	if ok {
		return num
	} else {
		return VARIABLE
	}
}

func (l *Lexer) Error(s string) {
	l.errs = append(l.errs, s)
}

func ParseSQL(sql string) ([]Statment, error) {

	var stmts []Statment
	lex := &Lexer{sql: sql}

	n := yyParse(lex, &stmts)

	if n != 0 {
		return nil, fmt.Errorf("解析sql异常 %+v", lex.errs)
	}
	return stmts, nil
}

func TrimQuote(str string) (string, error) {

	end := len(str) - 1
	switch str[0] {
	case '\'':
		if str[end] != '\'' {
			return "", fmt.Errorf("%s 缺少单引号", str)
		}
		return str[1:end], nil
	case '"':
		if str[end] != '"' {
			return "", fmt.Errorf("%s 缺少双引号", str)
		}
		return str[1:end], nil
	case '`':
		if str[end] != '`' {
			return "", fmt.Errorf("%s 缺少反引号", str)
		}
		return str[1:end], nil
	}
	return str, nil
}
