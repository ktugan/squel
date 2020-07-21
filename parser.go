package main

import (
	"C"
	"encoding/json"
	"github.com/campoy/unique"
	"github.com/xwb1989/sqlparser"
	"github.com/xwb1989/sqlparser/dependency/querypb"
	"reflect"
	"regexp"
	"strings"
)

type JsonResponse struct {
	ColumnAccessPatterns []ColumnAccess `json:"column_access_patterns"`
	RedactedQuery        string         `json:"redacted_query"`
	Error                string         `json:"error"`
}

//export RedactQuery
func RedactQuery(query string) *C.char {
	// The c interface to redact a query.
	r, _ := RedactSQLQuery(query)
	return C.CString(r)
}

//export ParseReturnJson
func ParseReturnJson(query string) *C.char {
	return C.CString(parseReturnJson(query))
}

func parseReturnJson(query string) string {
	stmt, e := sqlparser.Parse(query)
	if e != nil {
		response := JsonResponse{Error: e.Error()}
		js, _ := json.Marshal(response)
		return string(js)
	}

	tables := parseReflect(reflect.Indirect(reflect.ValueOf(stmt)))
	redactedQuery := redactSQLQuery(stmt)
	response := JsonResponse{ColumnAccessPatterns: tables, RedactedQuery: redactedQuery}

	js, _ := json.Marshal(response)
	return string(js)
}

type ColumnAccess struct {
	TableName  string `json:"table_name"`
	ColumnName string `json:"column_name"`
	QueryType  string `json:"query_type"`
}

func main() {
	print("Use c-interface or import")
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func RedactSQLQuery(sql string) (string, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return "", err
	}
	return redactSQLQuery(stmt), nil
}

func redactSQLQuery(stmt sqlparser.Statement) string {
	bv := map[string]*querypb.BindVariable{}
	prefix := "?"
	sqlparser.Normalize(stmt, bv, prefix)

	//The output of the normalizer looks adds numbers after each question mark. We will remove the numbers here.
	re := regexp.MustCompile(`:\?\d*`)
	sql := re.ReplaceAllString(sqlparser.String(stmt), "?")

	return sql
}

func sortAndUnique(cols []ColumnAccess) []ColumnAccess {
	// sort and filter out duplicates
	less := func(i, j int) bool {
		return cols[i].TableName+":"+cols[i].ColumnName+":"+cols[i].QueryType <
			cols[j].TableName+":"+cols[j].ColumnName+":"+cols[j].QueryType
	}
	unique.Slice(&cols, less)
	return cols
}

func parseQuery(query string) ([]ColumnAccess, error) {
	//Helper method to bring it into the right format
	stmt, e := sqlparser.Parse(query)
	if e != nil {
		return nil, e
	}
	return parseReflect(reflect.Indirect(reflect.ValueOf(stmt))), nil
}

func parseReflect(v reflect.Value) []ColumnAccess {
	//This method maps and cleans the output of the real parser.
	qi := getTableNames(v, *NewQueryInfo(), 0, "NULL", false)

	if len(qi.tables) == 1 && qi.tables[0] == "dual" {
		//The vitess sql parser behaves weird sometimes when variable queries are done. In these cases it will set the
		//the table name to "dual". If that should happen we will just return an empty set of ColumnAccess.
		//See `TestVariableQuery` for example query.
		return []ColumnAccess{}
	}

	for i := 0; i < len(qi.columns); i++ {
		col := &qi.columns[i]

		// Check if table name is empty. There are two outcomes if it is:
		// a. Condition: The query references only one table. Action: Set this table as the table name
		// b. Otherwise: We set the table name as a join of all tables seperated by comma
		if col.TableName == "" {
			if len(qi.tables) == 1 {
				col.TableName = qi.tables[0]
			} else {
				col.TableName = strings.Join(qi.tables, ",")
			}
		}

		//This section is to map columns from an aliased subquery to variables in the levels above.
		//See test `TestAliasedSubquery`
		key := Column{table: col.TableName, column: col.ColumnName}
		if value, ok := qi.aliasColumnToColumnMapping[key]; ok {
			col.TableName = value.table
		}

		//Check if the table name is in the list of aliasToTableMappings. If yes we will replace the alias with the
		// table name itself.
		// I cannot fathom a single case where it shouldn't find the mapping. Maybe throw error?
		if tableName, ok := qi.aliasToTableMapping[col.TableName]; ok {
			col.TableName = tableName
		}
	}
	//todo find unused tables. set(qi.tables) - set(qi.columns.values())
	//     if the difference in sets is bigger, then it would be weird and the query bad?
	//     or it could be a sign that we did a mistake!
	cols := sortAndUnique(qi.columns)

	// filter and sort out duplicates.
	return cols
}

type Column struct {
	table  string
	column string
}

type QueryInfo struct {
	tables                     []string
	aliasToTableMapping        map[string]string
	aliasColumnToColumnMapping map[Column]Column
	columns                    []ColumnAccess
}

func NewQueryInfo() *QueryInfo {
	tableMapping := make(map[string]string)
	columnMapping := make(map[Column]Column)
	return &QueryInfo{aliasToTableMapping: tableMapping, aliasColumnToColumnMapping: columnMapping}
}

func traverseSubquery(v reflect.Value, queryInfo *QueryInfo, alias string) {
	colAcc := parseReflect(v)
	queryInfo.columns = append(queryInfo.columns, colAcc...)
	for i := 0; i < len(colAcc); i++ {

		// Add the table to our slice if it is not in there yet
		if !stringInSlice(colAcc[i].TableName, queryInfo.tables) {
			queryInfo.tables = append(queryInfo.tables, colAcc[i].TableName)
		}

		//Here we need to map each column to the subquery alias. See test `TestAliasedSubquery` for case.
		if alias != "" {
			key := Column{table: alias, column: colAcc[i].ColumnName}
			value := Column{table: colAcc[i].TableName, column: colAcc[i].ColumnName}
			queryInfo.aliasColumnToColumnMapping[key] = value
		}
	}
}

func getTableNames(v reflect.Value, queryInfo QueryInfo, level int, queryType string, isTable bool) QueryInfo {
	switch v.Kind() {
	case reflect.Struct:
		//fmt.Printf("%*d s %s %s\n", level, level, v.Type().Name(), v)

		typeName := v.Type().Name()

		if typeName == "Subquery" && level != 0 {
			// Recursively call `parseReflect` since it is its own query with its own scope.
			// This allows us to map variable names in inner scopes which are not aliased.
			// If we would call `getTableNames` as usual we wouldn't be able to identify this scope without making
			// this method more complex than it already is.
			//
			// Documentation: Why level != 0??
			// In this case we are already in the recursion we are calling a few lines down. So we need to skip it
			// since we already have the desired clean slate and ultimately to prevent the infinite loop.
			traverseSubquery(v, &queryInfo, "")
			break
		}
		if typeName == "StarExpr" {
			expr := v.Interface().(sqlparser.StarExpr)
			tableName := expr.TableName.Name.CompliantName()
			tq := ColumnAccess{
				TableName:  tableName,
				ColumnName: "*",
				QueryType:  queryType,
			}
			queryInfo.columns = append(queryInfo.columns, tq)
			break
		}

		if typeName == "TableName" {
			value := v.Interface().(sqlparser.TableName)
			tableName := value.Name.CompliantName()
			qualifierName := value.Qualifier.CompliantName()
			if qualifierName == "" {
				qualifierName = tableName
			}

			queryInfo.tables = append(queryInfo.tables, tableName)
			queryInfo.aliasToTableMapping[qualifierName] = tableName

		}

		if typeName == "AliasedTableExpr" {
			expr := v.Interface().(sqlparser.AliasedTableExpr)
			switch value := expr.Expr.(type) {
			case sqlparser.TableName:
				tableName := value.Name.CompliantName()
				alias := expr.As.CompliantName()

				queryInfo.tables = append(queryInfo.tables, tableName)
				// add the alias to table mapping, if the alias is empty we will map the table name to the table name
				// this is needed since sometimes the alias/qualifier used is the table name itself!
				// Example: select car.model from car;
				queryInfo.aliasToTableMapping[alias] = tableName
				if alias == "" {
					queryInfo.aliasToTableMapping[tableName] = tableName
				}
			case *sqlparser.Subquery:
				traverseSubquery(reflect.Indirect(reflect.ValueOf(value)), &queryInfo, expr.As.String())
			}
			break
		}

		if typeName == "ColName" {
			colName := v.Interface().(sqlparser.ColName)
			tq := ColumnAccess{
				TableName:  colName.Qualifier.Name.CompliantName(),
				ColumnName: colName.Name.CompliantName(),
				QueryType:  queryType,
			}
			queryInfo.columns = append(queryInfo.columns, tq)
			break
		}

		if stringInSlice(typeName, []string{"Select", "Update", "Insert", "Delete", "Create", "Drop"}) {
			queryType = v.Type().Name()
		}

		// otherwise enumerate all fields of the struct and process further
		for i := 0; i < v.NumField(); i++ {
			queryInfo = getTableNames(reflect.Indirect(v.Field(i)), queryInfo, level+1, queryType, isTable)
		}

	case reflect.Array, reflect.Slice:
		switch v.Type().Name() {

		case "Columns":
			//We need to handle the `Columns` type here since it contains raw ColIdents which can be also functions.
			//  This would lead to function names being interpreted as columns.
			columns := v.Interface().(sqlparser.Columns)
			for i := 0; i < v.Len(); i++ {
				colIdent := columns[i]
				if colIdent.CompliantName() == "" {
					continue
				}
				tq := ColumnAccess{
					TableName:  "",
					ColumnName: colIdent.CompliantName(),
					QueryType:  queryType,
				}
				queryInfo.columns = append(queryInfo.columns, tq)
			}
			return queryInfo

		default:
			for i := 0; i < v.Len(); i++ {
				// enumerate all elements of an array/slice and process further
				queryInfo = getTableNames(reflect.Indirect(v.Index(i)), queryInfo, level+1, queryType, isTable)
			}
		}

	case reflect.Interface:
		switch v.Type().Name() {
		default:
			queryInfo = getTableNames(reflect.Indirect(reflect.ValueOf(v.Interface())), queryInfo, level+1, queryType, isTable)
		}
	}

	return queryInfo
}
