package gocassa

import (
	"fmt"
	"strings"
	"time"
)

// SelectStatement represents a read (SELECT) query for some data in C*
// It satisfies the Statement interface
type SelectStatement struct {
	keyspace       string                  // name of the keyspace
	table          string                  // name of the table
	fields         []string                // list of fields we want to select
	where          []Relation              // where filter clauses
	order          []ClusteringOrderColumn // order by clauses
	limit          int                     // limit count, 0 means no limit
	allowFiltering bool                    // whether we should allow filtering
	keys           Keys                    // partition / clustering keys for table
}

// Query provides the CQL query string for an SELECT query
func (s SelectStatement) Query() string {
	query, _ := s.QueryAndValues()
	return query
}

// Values provide the binding values for an SELECT query
func (s SelectStatement) Values() []interface{} {
	_, values := s.QueryAndValues()
	return values
}

// QueryAndValues returns the CQL query and any bind values
func (s SelectStatement) QueryAndValues() (string, []interface{}) {
	values := make([]interface{}, 0)
	query := []string{
		"SELECT",
		strings.Join(s.fields, ", "),
		fmt.Sprintf("FROM %s.%s", s.Keyspace(), s.Table()),
	}

	whereCQL, whereValues := generateWhereCQL(s.Relations())
	if whereCQL != "" {
		query = append(query, "WHERE", whereCQL)
		values = append(values, whereValues...)
	}

	orderByCQL := generateOrderByCQL(s.OrderBy())
	if orderByCQL != "" {
		query = append(query, "ORDER BY", orderByCQL)
	}

	if s.Limit() > 0 {
		query = append(query, "LIMIT ?")
		values = append(values, s.limit)
	}

	if s.AllowFiltering() {
		query = append(query, "ALLOW FILTERING")
	}

	return strings.Join(query, " "), values
}

// Keyspace returns the name of the Keyspace for the statement
func (s SelectStatement) Keyspace() string {
	return s.keyspace
}

// Table returns the name of the table for this statement
func (s SelectStatement) Table() string {
	return s.table
}

// Fields returns the list of fields to be selected
func (s SelectStatement) Fields() []string {
	return s.fields
}

// Relations provides the WHERE clause Relation items used to evaluate
// this query
func (s SelectStatement) Relations() []Relation {
	return s.where
}

// OrderBy returns the ClusteringOrderColumn clauses used
func (s SelectStatement) OrderBy() []ClusteringOrderColumn {
	return s.order
}

// Limit returns the number of rows to be returned, a value of zero
// means no limit
func (s SelectStatement) Limit() int {
	if s.limit < 1 {
		return 0
	}
	return s.limit
}

func (s SelectStatement) AllowFiltering() bool {
	return s.allowFiltering
}

// Keys provides the Partition / Clustering keys defined by the table recipe
func (s SelectStatement) Keys() Keys {
	return s.keys
}

// InsertStatement represents an INSERT query to write some data in C*
// It satisfies the Statement interface
type InsertStatement struct {
	keyspace string                 // name of the keyspace
	table    string                 // name of the table
	fieldMap map[string]interface{} // fields to be inserted
	ttl      time.Duration          // ttl of the row
	keys     Keys                   // partition / clustering keys for table
}

// Query provides the CQL query string for an INSERT INTO query
func (s InsertStatement) Query() string {
	query, _ := s.QueryAndValues()
	return query
}

// Values provide the binding values for an INSERT INTO query
func (s InsertStatement) Values() []interface{} {
	_, values := s.QueryAndValues()
	return values
}

// QueryAndValues returns the CQL query and any bind values
func (s InsertStatement) QueryAndValues() (string, []interface{}) {
	query := []string{"INSERT INTO", fmt.Sprintf("%s.%s", s.Keyspace(), s.Table())}

	fieldMap := s.FieldMap()
	fieldNames := make([]string, 0, len(fieldMap))
	placeholders := make([]string, 0, len(fieldMap))
	values := make([]interface{}, 0, len(fieldMap))
	for _, field := range sortedKeys(fieldMap) {
		fieldNames = append(fieldNames, strings.ToLower(field))
		placeholders = append(placeholders, "?")
		values = append(values, fieldMap[field])
	}

	query = append(query, "("+strings.Join(fieldNames, ", ")+")")
	query = append(query, "VALUES ("+strings.Join(placeholders, ", ")+")")

	// Determine if we need to set a TTL
	if s.TTL() > time.Duration(0) {
		query = append(query, "USING TTL ?")
		values = append(values, int(s.TTL().Seconds()))
	}

	return strings.Join(query, " "), values
}

// Keyspace returns the name of the Keyspace for the statement
func (s InsertStatement) Keyspace() string {
	return s.keyspace
}

// Table returns the name of the table for this statement
func (s InsertStatement) Table() string {
	return s.table
}

// FieldMap gives a map of all the fields to be inserted. In an INSERT
// statement, none of these will be Modifier types
func (s InsertStatement) FieldMap() map[string]interface{} {
	return s.fieldMap
}

// TTL returns the Time-To-Live for this row statement. A duration of 0
// means there is no TTL
func (s InsertStatement) TTL() time.Duration {
	if s.ttl < time.Duration(1) {
		return time.Duration(0)
	}
	return s.ttl
}

// Keys provides the Partition / Clustering keys defined by the table recipe
func (s InsertStatement) Keys() Keys {
	return s.keys
}

// UpdateStatement represents an UPDATE query to update some data in C*
// It satisfies the Statement interface
type UpdateStatement struct {
	keyspace string                 // name of the keyspace
	table    string                 // name of the table
	fieldMap map[string]interface{} // fields to be updated
	where    []Relation             // where filter clauses
	ttl      time.Duration          // ttl of the row
	keys     Keys                   // partition / clustering keys for table
}

// Query provides the CQL query string for an UPDATE query
func (s UpdateStatement) Query() string {
	query, _ := s.QueryAndValues()
	return query
}

// Values provide the binding values for an UPDATE query
func (s UpdateStatement) Values() []interface{} {
	_, values := s.QueryAndValues()
	return values
}

// QueryAndValues returns the CQL query and any bind values
func (s UpdateStatement) QueryAndValues() (string, []interface{}) {
	values := make([]interface{}, 0)
	query := []string{"UPDATE", fmt.Sprintf("%s.%s", s.Keyspace(), s.Table())}

	// Determine if we need to set a TTL
	if s.TTL() > 0 {
		query = append(query, "USING TTL ?")
		values = append(values, int(s.TTL().Seconds()))
	}

	setCQL, setValues := generateUpdateSetCQL(s.FieldMap())
	query = append(query, "SET", setCQL)
	values = append(values, setValues...)

	whereCQL, whereValues := generateWhereCQL(s.Relations())
	if whereCQL != "" {
		query = append(query, "WHERE", whereCQL)
		values = append(values, whereValues...)
	}
	return strings.Join(query, " "), values
}

// Keyspace returns the name of the Keyspace for the statement
func (s UpdateStatement) Keyspace() string {
	return s.keyspace
}

// Table returns the name of the table for this statement
func (s UpdateStatement) Table() string {
	return s.table
}

// FieldMap gives a map of all the fields to be inserted. In an UPDATE
// statement, the values may be Modifier types
func (s UpdateStatement) FieldMap() map[string]interface{} {
	return s.fieldMap
}

// Relations provides the WHERE clause Relation items used to evaluate
// this query
func (s UpdateStatement) Relations() []Relation {
	return s.where
}

// TTL returns the Time-To-Live for this row statement. A duration of 0
// means there is no TTL
func (s UpdateStatement) TTL() time.Duration {
	if s.ttl < time.Duration(1) {
		return time.Duration(0)
	}
	return s.ttl
}

// Keys provides the Partition / Clustering keys defined by the table recipe
func (s UpdateStatement) Keys() Keys {
	return s.keys
}

// DeleteStatement represents a DELETE query to delete some data in C*
// It satisfies the Statement interface
type DeleteStatement struct {
	keyspace string     // name of the keyspace
	table    string     // name of the table
	where    []Relation // where filter clauses
	keys     Keys       // partition / clustering keys for table
}

// Query provides the CQL query string for a DELETE query
func (s DeleteStatement) Query() string {
	query, _ := s.QueryAndValues()
	return query
}

// Values provide the binding values for a DELETE query
func (s DeleteStatement) Values() []interface{} {
	_, values := s.QueryAndValues()
	return values
}

// QueryAndValues returns the CQL query and any bind values
func (s DeleteStatement) QueryAndValues() (string, []interface{}) {
	query := fmt.Sprintf("DELETE FROM %s.%s", s.Keyspace(), s.Table())
	whereCQL, whereValues := generateWhereCQL(s.Relations())
	if whereCQL != "" {
		query += " WHERE " + whereCQL
	}
	return query, whereValues
}

// Keyspace returns the name of the Keyspace for the statement
func (s DeleteStatement) Keyspace() string {
	return s.keyspace
}

// Table returns the name of the table for this statement
func (s DeleteStatement) Table() string {
	return s.table
}

// Relations provides the WHERE clause Relation items used to evaluate
// this query
func (s DeleteStatement) Relations() []Relation {
	return s.where
}

// Keys provides the Partition / Clustering keys defined by the table recipe
func (s DeleteStatement) Keys() Keys {
	return s.keys
}

// cqlStatement represents a statement that executes raw CQL
type cqlStatement struct {
	query  string
	values []interface{}
}

func (s cqlStatement) Query() string { return s.query }

func (s cqlStatement) Values() []interface{} { return s.values }

// noOpStatement represents a statement that doesn't perform any specific
// query. It's used internally for testing, satisfies the Statement interface
type noOpStatement struct{}

func (_ noOpStatement) Query() string { return "" }

func (_ noOpStatement) Values() []interface{} { return []interface{}{} }

// generateUpdateSetCQL takes in a field map and generates the comma separated
// SET syntax. An expected output may be something like:
// 	- "foo = ?", {1}
// 	- "foo = ?, bar = ?", {1, 2}
func generateUpdateSetCQL(fm map[string]interface{}) (string, []interface{}) {
	clauses, values := make([]string, 0, len(fm)), make([]interface{}, 0, len(fm))
	for _, fieldName := range sortedKeys(fm) {
		value := fm[fieldName]
		if modifier, ok := value.(Modifier); ok {
			stmt, vals := modifier.cql(fieldName)
			clauses = append(clauses, stmt)
			values = append(values, vals...)
			continue
		}
		clauses = append(clauses, fieldName+" = ?")
		values = append(values, value)
	}
	return strings.Join(clauses, ", "), values
}

// generateWhereCQL takes a list of relations and generates the CQL for
// a WHERE clause. An expected output may be something like:
//	- "foo = ?", {1}
//	- "foo = ? AND bar IN ?", {1, {"a", "b", "c"}}
func generateWhereCQL(rs []Relation) (string, []interface{}) {
	clauses, values := make([]string, 0, len(rs)), make([]interface{}, 0, len(rs))
	for _, relation := range rs {
		clause, bindValue := generateRelationCQL(relation)
		clauses = append(clauses, clause)
		values = append(values, bindValue)
	}
	return strings.Join(clauses, " AND "), values
}

func generateRelationCQL(rel Relation) (string, interface{}) {
	field := strings.ToLower(rel.Field())
	switch rel.Comparator() {
	case CmpEquality:
		return field + " = ?", rel.Terms()[0]
	case CmpIn:
		return field + " IN ?", rel.Terms()
	case CmpGreaterThan:
		return field + " > ?", rel.Terms()[0]
	case CmpGreaterThanOrEquals:
		return field + " >= ?", rel.Terms()[0]
	case CmpLesserThan:
		return field + " < ?", rel.Terms()[0]
	case CmpLesserThanOrEquals:
		return field + " <= ?", rel.Terms()[0]
	default:
		// This represents an invalid Comparator and would only manifest
		// if we've initialised a Relation incorrectly within this package
		panic(fmt.Sprintf("unknown comparator %v", rel.Comparator()))
	}
}

// generateOrderByCQL generates the CQL for the ORDER BY clause. An expected
// output might look like:
//	- foo ASC
//  - foo ASC, bar DESC
func generateOrderByCQL(order []ClusteringOrderColumn) string {
	out := make([]string, 0, len(order))
	for _, oc := range order {
		out = append(out, oc.Column+" "+oc.Direction.String())
	}
	return strings.Join(out, ", ")
}
