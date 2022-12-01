package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"kvdb/pkg/sql"
	"math"
	"strconv"
)

type SQLExector struct {
	keySet map[string]struct{}

	selectStmt *sql.SelectStmt
	aggregate  func([]string) []string

	startOffset int
	endOffset   int

	Done   bool
	Count  int
	Result [][]string
}

type SQLResult struct {
	Type        sql.StmtType
	SelecResult [][]string
}

func (e *SQLExector) Filter(key, raw []byte) (bool, error) {
	if e.Count < e.startOffset {
		e.Count++
		return false, nil
	}

	keyStr := string(key)
	_, exist := e.keySet[keyStr]
	if exist {
		return false, nil
	}

	e.keySet[keyStr] = struct{}{}

	var row []string
	err := json.Unmarshal(raw, &row)
	if err != nil {
		return false, err
	}

	accept := true
	for _, be := range e.selectStmt.Where {
		accept = accept && be.Filter(&row)
		if !accept {
			break
		}
	}

	if accept {
		if e.aggregate != nil {
			if len(e.Result) > 0 {
				e.Result[0] = e.aggregate(row)
			} else {
				e.Result = [][]string{e.aggregate(row)}
			}
		} else {
			resultRow := make([]string, 0, len(e.selectStmt.Filed))
			for _, v := range e.selectStmt.Filed {
				if v.Field != "" {
					resultRow = append(resultRow, row[v.Pos])
				}
				if v.Expr != nil {
					resultRow = append(resultRow, "## TODO")
				}
			}
			e.Count++
			e.Result = append(e.Result, resultRow)
		}
	}

	if e.Count >= e.endOffset {
		e.Done = true
		return true, nil
	}
	return false, nil

}

func (s *RaftServer) ExcuteCreate(stmt *sql.CreateStmt) error {

	tableMetaKey := []byte(strconv.Itoa(1) + "_meta_" + stmt.Table)
	meta, err := s.get(tableMetaKey)
	if err != nil {
		return fmt.Errorf("检查表 %s 是否存在失败 %v", stmt.Table, err)
	}

	if meta != nil {
		return fmt.Errorf("表 %s 已存在失败 %v", stmt.Table, err)
	}

	key := []byte(strconv.Itoa(1) + "_max_table_id")
	value, err := s.get(key)
	if err != nil {
		return fmt.Errorf("获取表 %s 编号失败 %v", stmt.Table, err)
	}

	var tableId int
	if value == nil {
		tableId = 2
	} else {
		tableId, err = strconv.Atoi(string(value))
		if err != nil {
			return fmt.Errorf("解析表 %s 编号失败 %v", stmt.Table, err)
		}
	}

	err = s.put(key, []byte(strconv.Itoa(tableId+1)))
	if err != nil {
		return fmt.Errorf("更新表编号失败 %v", err)
	}

	stmt.Def.TableId = tableId

	if len(stmt.Def.Primary.IndexField) == 1 {
		pk := stmt.Def.Primary.IndexField[0]
		for _, cd := range stmt.Def.Column {
			if cd.FieldName == pk && cd.FieldType == sql.INT {
				stmt.Def.PKAsRowId = true
				break
			}
		}
	}

	table, err := json.Marshal(stmt.Def)
	if err != nil {
		return fmt.Errorf("序列化表 %s 元数据 %+v 失败 %v", stmt.Table, stmt.Def, err)
	}

	err = s.put(tableMetaKey, []byte(table))
	if err != nil {
		return fmt.Errorf("写入表 %s 元数据失败 %v", stmt.Table, err)
	}

	return nil
}

func (s *RaftServer) ExcuteInsert(stmt *sql.InsertStmt) error {

	tableInfo, err := s.LoadMeataData(stmt.Into)
	if err != nil {
		return err
	}

	colKey := strconv.Itoa(tableInfo.TableId) + "_"
	if tableInfo.PKAsRowId {
		pkIdx := -1
		pk := tableInfo.Primary.IndexField[0]

		if len(stmt.ColumnList) != 0 {
			for i, v := range stmt.ColumnList {
				if pk == v {
					pkIdx = i
				}
			}
		} else {
			if len(stmt.ValueList[0]) != len(tableInfo.Column) {
				return fmt.Errorf("表 %s 列数量 %d 与数据列数量 %d 不一致", stmt.Into, len(stmt.ValueList[0]), len(tableInfo.Column))
			}

			for i, cd := range tableInfo.Column {
				if pk == cd.FieldName {
					pkIdx = i
				}
			}
		}

		if pkIdx == -1 {
			return fmt.Errorf("列数据需包含主键 %s", pk)
		}

		for _, v := range stmt.ValueList {
			key := colKey + v[pkIdx]
			value, err := json.Marshal(v)

			if err != nil {
				return fmt.Errorf("序列化行数据 %v 失败 %v", v, err)
			}

			err = s.put([]byte(key), value)
			if err != nil {
				return fmt.Errorf("序列化行数据 %v 失败 %v", v, err)
			}
		}
	} else {
		b := make([]byte, 8)
		for _, v := range stmt.ValueList {
			tableInfo.RowId++
			binary.BigEndian.PutUint64(b, uint64(tableInfo.RowId))

			key := append([]byte(colKey), b...)
			value, err := json.Marshal(v)

			if err != nil {
				return fmt.Errorf("序列化行数据 %v 失败 %v", v, err)
			}

			err = s.put(key, value)
			if err != nil {
				return fmt.Errorf("序列化行数据 %v 失败 %v", v, err)
			}
		}
	}

	return nil
}

func (s *RaftServer) ExecuteSelect(stmt *sql.SelectStmt) ([][]string, error) {
	tableInfo, err := s.LoadMeataData(stmt.From.Table)
	if err != nil {
		return nil, err
	}

	colOrder := make(map[string]int, len(tableInfo.Column))
	for i, cd := range tableInfo.Column {
		colOrder[cd.FieldName] = i
	}

	for _, be := range stmt.Where {
		err = be.Prepare(colOrder)
		if err != nil {
			return nil, err
		}
	}

	start := []byte(strconv.Itoa(tableInfo.TableId) + "_")
	end := binary.BigEndian.AppendUint64(start, uint64(math.MaxInt64))
	var aggFuncs []*sql.SqlFunction

	for _, sf := range stmt.Filed {
		if sf.Field != "" {
			idx, exist := colOrder[sf.Field]
			if exist {
				sf.Pos = idx
			} else {
				return nil, fmt.Errorf("查询列 %s 不存在", sf.Field)
			}
		}

		if sf.Expr != nil {
			if sf.Expr.Type == sql.AGGREGATE_FUNCTION {
				str, ok := sf.Expr.Args[0].(string)
				if !ok {
					return nil, fmt.Errorf("函数 %s 参数 %+v 异常", sf.Expr.Func, sf.Expr.Args)
				}

				if sf.Expr.Func == "COUNT" && (str == "*" || str == "1") {
					sf.Expr.FieldPos = append(sf.Expr.FieldPos, -1)
					aggFuncs = append(aggFuncs, sf.Expr)
					continue
				}

				idx, exist := colOrder[str]
				if exist {
					sf.Expr.FieldPos = append(sf.Expr.FieldPos, idx)
					aggFuncs = append(aggFuncs, sf.Expr)
				} else {
					return nil, fmt.Errorf("函数 %s 参数列 %s 不存在", sf.Expr.Func, sf.Expr.Args[0])
				}
			}
		}
	}

	var aggregate func(row []string) []string
	if len(aggFuncs) > 0 {
		if len(aggFuncs) != len(stmt.Filed) {
			return nil, fmt.Errorf("聚合函数 %+v 不能同非聚合函数一同使用", aggFuncs)
		}
		totalCount := 0
		max := math.MinInt
		min := math.MaxInt
		result := make([]string, len(aggFuncs))
		aggregate = func(row []string) []string {
			for i, sf := range aggFuncs {
				switch sf.Func {
				case "COUNT":
					idx := sf.FieldPos[0]
					if idx == -1 {
						totalCount++
					} else {
						if row[idx] != "" {
							totalCount++
						}
					}
					result[i] = strconv.Itoa(totalCount)
				case "MAX":
					idx := sf.FieldPos[0]
					num, err := strconv.Atoi(row[idx])
					if err == nil {
						if num > max {
							max = num
						}
						result[i] = strconv.Itoa(max)
					}
				case "MIN":
					idx := sf.FieldPos[0]
					num, err := strconv.Atoi(row[idx])
					if err == nil {
						if num < min {
							min = num
						}
						result[i] = strconv.Itoa(min)
					}
				}
			}
			return result
		}
	}

	var result [][]string
	var endOffset int
	var startOffset int
	if stmt.Limit != nil {
		startOffset = stmt.Limit.Offset
		endOffset = stmt.Limit.Offset + stmt.Limit.Size
		result = make([][]string, 0, stmt.Limit.Size)
	} else {
		endOffset = math.MaxInt
		result = make([][]string, 0)
	}

	excutor := &SQLExector{
		keySet:      make(map[string]struct{}),
		selectStmt:  stmt,
		aggregate:   aggregate,
		startOffset: startOffset,
		endOffset:   endOffset,
		Result:      result,
	}

	err = s.storage.GetRange(s.encoding.DefaultPrefix(start), s.encoding.DefaultPrefix(end), excutor.Filter)
	if err != nil {
		return nil, err
	}

	return excutor.Result, nil
}

func (s *RaftServer) LoadMeataData(table string) (*sql.TableDef, error) {

	tableMetaKey := []byte(strconv.Itoa(1) + "_meta_" + table)
	metaCache, exist := s.cache[string(tableMetaKey)]
	if exist {
		tableInfo, ok := metaCache.(*sql.TableDef)
		if ok {
			return tableInfo, nil
		}
	}

	meta, err := s.get(tableMetaKey)
	if err != nil {
		return nil, fmt.Errorf("获取表 %s 元数据失败 %v", table, err)
	}
	if meta == nil {
		return nil, fmt.Errorf("表 %s 不存在", table)
	}

	var tableInfo sql.TableDef
	err = json.Unmarshal(meta, &tableInfo)
	if err != nil {
		return nil, fmt.Errorf("解析表 %s 元数据失败 %v", table, err)
	}

	s.cache[string(tableMetaKey)] = &tableInfo

	return &tableInfo, nil
}
