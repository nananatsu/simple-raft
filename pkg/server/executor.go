package server

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"kvdb/pkg/sql"
	"math"
	"strconv"
)

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

	ret, err := s.storage.GetRange(s.encoding.DefaultPrefix(start), s.encoding.DefaultPrefix(end), stmt.Where, stmt.Limit.Size)

	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (s *RaftServer) LoadMeataData(table string) (*sql.TableDef, error) {

	tableMetaKey := []byte(strconv.Itoa(1) + "_meta_" + table)
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

	return &tableInfo, nil
}
