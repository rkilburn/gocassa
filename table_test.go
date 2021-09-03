package gocassa

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

func createIf(cs TableChanger, tes *testing.T) {
	err := cs.(TableChanger).Recreate()
	if err != nil {
		tes.Fatal(err)
	}
}

// cqlsh> CREATE TABLE test.customer1 (id text, name text, PRIMARY KEY((id, name)));
func TestTables(t *testing.T) {
	res, err := ns.(*k).Tables()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) == 0 {
		t.Fatal("Not found ", len(res))
	}
}

func TestCreateTable(t *testing.T) {
	rand.Seed(time.Now().Unix())
	name := fmt.Sprintf("customer_%v", rand.Int()%100)
	cs := ns.Table(name, Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
	})
	createIf(cs, t)
	err := cs.Set(Customer{
		Id:   "1001",
		Name: "Joe",
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	res := &[]Customer{}
	err = cs.Where(Eq("Id", "1001"), Eq("Name", "Joe")).Read(res).Run()
	if err != nil {
		t.Fatal(err)
	}
	if len(*res) != 1 {
		t.Fatal("Not found ", len(*res))
	}
	err = ns.(*k).DropTable(name)
	if err != nil {
		t.Fatal(err)
	}
}

func TestClusteringOrder(t *testing.T) {
	options := Options{}.AppendClusteringOrder("Id", DESC)
	name := "customer_by_name"
	cs := ns.Table(name, Customer{}, Keys{
		PartitionKeys:     []string{"Name"},
		ClusteringColumns: []string{"Id"},
	}).WithOptions(options)
	createIf(cs, t)

	customers := []Customer{
		Customer{
			Id:   "1001",
			Name: "Brian",
		},
		Customer{
			Id:   "1002",
			Name: "Adam",
		},
		Customer{
			Id:   "1003",
			Name: "Brian",
		},
		Customer{
			Id:   "1004",
			Name: "Brian",
		},
	}

	for _, c := range customers {
		err := cs.Set(c).Run()
		if err != nil {
			t.Fatal(err)
		}
	}
	res := &[]Customer{}
	err := cs.Where(Eq("Name", "Brian")).Read(res).Run()
	if err != nil {
		t.Fatal(err)
	}

	expected := []string{"1004", "1003", "1001"}
	if len(*res) != len(expected) {
		t.Fatal("Expected", len(*res), " results, got", len(*res))
	}
	for i, id := range expected {
		if (*res)[i].Id != id {
			t.Fatal("Got result out of order. i:", i, "expected ID:", id, "actual ID:", (*res)[i].Id)
		}
	}
}

func TestClusteringOrderMultipl(t *testing.T) {
	options := Options{}.AppendClusteringOrder("Tag", DESC).AppendClusteringOrder("Id", DESC)
	name := "customer_by_name2"
	cs := ns.Table(name, Customer2{}, Keys{
		PartitionKeys:     []string{"Name"},
		ClusteringColumns: []string{"Tag", "Id"},
	}).WithOptions(options)
	createIf(cs, t)

	customers := []Customer2{
		Customer2{
			Id:   "1001",
			Name: "Brian",
			Tag:  "B",
		},
		Customer2{
			Id:   "1002",
			Name: "Adam",
			Tag:  "A",
		},
		Customer2{
			Id:   "1003",
			Name: "Brian",
			Tag:  "A",
		},
		Customer2{
			Id:   "1004",
			Name: "Brian",
			Tag:  "B",
		},
	}

	for _, c := range customers {
		err := cs.Set(c).Run()
		if err != nil {
			t.Fatal(err)
		}
	}
	res := &[]Customer2{}
	err := cs.Where(Eq("Name", "Brian")).Read(res).Run()
	if err != nil {
		t.Fatal(err)
	}

	expected := []struct {
		Tag string
		Id  string
	}{
		{"B", "1004"},
		{"B", "1001"},
		{"A", "1003"},
	}
	if len(*res) != len(expected) {
		t.Fatal("Expected", len(*res), " results, got", len(*res))
	}
	for i, e := range expected {
		result := (*res)[i]
		if result.Id != e.Id || result.Tag != e.Tag {
			t.Fatal("Got result out of order. i:", i, "expected ID:", e.Id, "actual ID:", result.Id, "expected tag:", e.Tag, "actual tag:", result.Tag)
		}
	}
}

func TestCreateStatement(t *testing.T) {
	cs := ns.Table("something", Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
	})
	stmt, err := cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stmt.Query(), "something") {
		t.Fatal(stmt.Query())
	}
	stmt, err = cs.WithOptions(Options{TableName: "funky"}).CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stmt.Query(), "funky") {
		t.Fatal(stmt.Query())
	}
	// if clustering order is not specified, it should omit the clustering order by and use the default
	if strings.Contains(stmt.Query(), "WITH CLUSTERING ORDER BY") {
		t.Fatal(stmt.Query())
	}
}

func TestAllowFiltering(t *testing.T) {
	name := "allow_filtering"
	cs := ns.Table(name, Customer2{}, Keys{
		PartitionKeys:     []string{"Name"},
		ClusteringColumns: []string{"Tag", "Id"},
	})
	createIf(cs, t)
	c2 := Customer2{}
	//This shouldn't contain allow filtering
	st := cs.Where(Eq("Name", "Brian")).Read(&c2).GenerateStatement()
	if strings.Contains(st.Query(), "ALLOW FILTERING") {
		t.Error("Allow filtering should be disabled by default")
	}

	op := Options{AllowFiltering: true}
	stAllow := cs.Where(Eq("", "")).Read(&c2).WithOptions(op).GenerateStatement()
	if !strings.Contains(stAllow.Query(), "ALLOW FILTERING") {
		t.Error("Allow filtering show be included in the statement")
	}
}

func TestKeysCreation(t *testing.T) {
	cs := ns.Table("composite_keys", Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
	})
	stmt, err := cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//composite
	if !strings.Contains(stmt.Query(), "PRIMARY KEY ((id, name ))") {
		t.Fatal(stmt.Query())
	}

	cs = ns.Table("compound_keys", Customer{}, Keys{
		PartitionKeys: []string{"Id", "Name"},
		Compound:      true,
	})
	stmt, err = cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//compound
	if !strings.Contains(stmt.Query(), "PRIMARY KEY (id, name )") {
		t.Fatal(stmt.Query())
	}

	cs = ns.Table("clustering_keys", Customer{}, Keys{
		PartitionKeys:     []string{"Id"},
		ClusteringColumns: []string{"Name"},
	})
	stmt, err = cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//with columns
	if !strings.Contains(stmt.Query(), "PRIMARY KEY ((id), name)") {
		t.Fatal(stmt.Query())
	}
	//compound gets ignored when using clustering columns
	cs = ns.Table("clustering_keys", Customer{}, Keys{
		PartitionKeys:     []string{"Id"},
		ClusteringColumns: []string{"Name"},
		Compound:          true,
	})
	stmt, err = cs.CreateStatement()
	if err != nil {
		t.Fatal(err)
	}
	//with columns
	if !strings.Contains(stmt.Query(), "PRIMARY KEY ((id), name)") {
		t.Fatal(stmt.Query())
	}
}

// Mock QueryExecutor that keeps track of options passed to it
type OptionCheckingQE struct {
	stmt Statement
	opts *Options
}

func (qe *OptionCheckingQE) QueryWithOptions(opts Options, stmt Statement, scanner Scanner) error {
	qe.stmt = stmt
	qe.opts.Consistency = opts.Consistency
	return nil
}

func (qe *OptionCheckingQE) Query(stmt Statement, scanner Scanner) error {
	return qe.QueryWithOptions(Options{}, stmt, scanner)
}

func (qe *OptionCheckingQE) ExecuteWithOptions(opts Options, stmt Statement) error {
	qe.stmt = stmt
	qe.opts.Consistency = opts.Consistency
	return nil
}

func (qe *OptionCheckingQE) Execute(stmt Statement) error {
	return qe.ExecuteWithOptions(Options{}, stmt)
}

func (qe *OptionCheckingQE) ExecuteAtomically(stmt []Statement) error {
	return nil
}

func (qe *OptionCheckingQE) ExecuteAtomicallyWithOptions(opts Options, stmt []Statement) error {
	qe.opts.Consistency = opts.Consistency
	return nil
}

func (qe *OptionCheckingQE) IncrementPrometheusCounterSuccess(method string) {}

func (qe *OptionCheckingQE) IncrementPrometheusCounterError(method string) {}

func TestQueryWithConsistency(t *testing.T) {
	// It's tricky to verify this against a live DB, so mock out the
	// query executor and make sure the right options get passed
	// through
	resultOpts := Options{}
	qe := &OptionCheckingQE{opts: &resultOpts}
	conn := &connection{q: qe}
	ks := conn.KeySpace("some ks")
	cs := ks.Table("customerWithConsistency", Customer{}, Keys{PartitionKeys: []string{"Id"}})
	res := &[]Customer{}
	cons := gocql.Quorum
	opts := Options{Consistency: &cons}

	q := cs.Where(Eq("Id", 1)).Read(res).WithOptions(opts)

	if err := q.Run(); err != nil {
		t.Fatal(err)
	}
	if resultOpts.Consistency == nil {
		t.Fatal(fmt.Sprint("Expected consistency:", cons, "got: nil"))
	}
	if resultOpts.Consistency != nil && *resultOpts.Consistency != cons {
		t.Fatal(fmt.Sprint("Expected consistency:", cons, "got:", resultOpts.Consistency))
	}
}

func TestExecuteWithConsistency(t *testing.T) {
	resultOpts := Options{}
	qe := &OptionCheckingQE{opts: &resultOpts}
	conn := &connection{q: qe}
	ks := conn.KeySpace("some ks")
	cs := ks.Table("customerWithConsistency2", Customer{}, Keys{PartitionKeys: []string{"Id"}})
	cons := gocql.All
	opts := Options{Consistency: &cons}

	// This calls Execute() under the covers
	err := cs.Set(Customer{
		Id:   "100",
		Name: "Joe",
	}).WithOptions(opts).Run()
	if err != nil {
		t.Fatal(err)
	}
	if resultOpts.Consistency == nil {
		t.Fatal(fmt.Sprint("Expected consistency:", cons, "got: nil"))
	}
	if resultOpts.Consistency != nil && *resultOpts.Consistency != cons {
		t.Fatal(fmt.Sprint("Expected consistency:", cons, "got:", resultOpts.Consistency))
	}
}

func TestExecuteWithNullableFields(t *testing.T) {
	type UserBasic struct {
		Id       string
		Metadata []byte
	}

	qe := &OptionCheckingQE{opts: &Options{}}
	conn := &connection{q: qe}
	ks := conn.KeySpace("user")
	cs := ks.Table("user", UserBasic{}, Keys{PartitionKeys: []string{"Id"}}).
		WithOptions(Options{TableName: "user_by_id"})

	// inserting primary key (ID) only (with a nullable field)
	err := cs.Set(UserBasic{Id: "100"}).Run()
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO user.user_by_id (id, metadata) VALUES (?, ?)", qe.stmt.Query())

	// upserting with metadata set
	err = cs.Set(UserBasic{Id: "100", Metadata: []byte{0x02}}).Run()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE user.user_by_id SET metadata = ? WHERE id = ?", qe.stmt.Query())

	// upserting with a non-nullable field
	type UserWithPhone struct {
		Id          string
		PhoneNumber *string
		Metadata    []byte
	}
	cs = ks.Table("user", UserWithPhone{}, Keys{PartitionKeys: []string{"Id"}}).
		WithOptions(Options{TableName: "user_by_id"})
	err = cs.Set(UserWithPhone{Id: "100", PhoneNumber: nil}).Run()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE user.user_by_id SET metadata = ?, phonenumber = ? WHERE id = ?", qe.stmt.Query())

	number := "01189998819991197253"
	err = cs.Set(UserWithPhone{Id: "100", PhoneNumber: &number}).Run()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE user.user_by_id SET metadata = ?, phonenumber = ? WHERE id = ?", qe.stmt.Query())

	type UserWithName struct {
		Id       string
		Name     string
		Metadata []byte
		Status   map[string]string
	}
	cs = ks.Table("user", UserWithName{}, Keys{
		PartitionKeys: []string{"Id"}, ClusteringColumns: []string{"Name"}}).
		WithOptions(Options{TableName: "user_by_id"})

	// inserting with all nullable fields not set
	err = cs.Set(UserWithName{Id: "100", Name: "Moss"}).Run()
	assert.NoError(t, err)
	assert.Equal(t, "INSERT INTO user.user_by_id (id, metadata, name, status) VALUES (?, ?, ?, ?)", qe.stmt.Query())

	// upserting with a nullable field actually set
	err = cs.Set(UserWithName{Id: "100", Name: "Moss", Status: map[string]string{"foo": "bar"}}).Run()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE user.user_by_id SET metadata = ?, status = ? WHERE id = ? AND name = ?", qe.stmt.Query())
}

func TestAllFieldValuesAreNullable(t *testing.T) {
	// all collection types defined are nullable
	assert.True(t, allFieldValuesAreNullable(map[string]interface{}{
		"field1": []byte{},
		"field2": map[string]string{},
		"field3": [0]int{},
	}))

	// not nullable due to populated byte array
	assert.False(t, allFieldValuesAreNullable(map[string]interface{}{
		"field1": []byte{0x00},
		"field2": map[string]string{},
		"field3": [0]int{},
	}))

	// not nullable due to string
	assert.False(t, allFieldValuesAreNullable(map[string]interface{}{
		"field1": []byte{},
		"field4": "",
	}))

	// not nullable due to int
	assert.False(t, allFieldValuesAreNullable(map[string]interface{}{
		"field2": map[string]string{},
		"field5": 0,
	}))

	// the empty field list is nullable
	assert.True(t, allFieldValuesAreNullable(map[string]interface{}{}))
}
