package gocassa

import (
	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type goCQLBackend struct {
	session              *gocql.Session
	readSuccessCounter   prometheus.Counter
	insertSuccessCounter prometheus.Counter
	updateSuccessCounter prometheus.Counter
	deleteSuccessCounter prometheus.Counter
	totalSuccessCounter  prometheus.Counter
	readErrorCounter     prometheus.Counter
	insertErrorCounter   prometheus.Counter
	updateErrorCounter   prometheus.Counter
	deleteErrorCounter   prometheus.Counter
	totalErrorCounter    prometheus.Counter
}

func (cb goCQLBackend) Query(stmt Statement, scanner Scanner) error {
	return cb.QueryWithOptions(Options{}, stmt, scanner)
}

func (cb goCQLBackend) QueryWithOptions(opts Options, stmt Statement, scanner Scanner) error {
	qu := cb.session.Query(stmt.Query(), stmt.Values()...)
	if opts.Consistency != nil {
		qu = qu.Consistency(*opts.Consistency)
	}
	if opts.Context != nil {
		qu = qu.WithContext(opts.Context)
	}

	iter := qu.Iter()
	if _, err := scanner.ScanIter(iter.Scanner()); err != nil {
		return err
	}

	return iter.Close()
}

func (cb goCQLBackend) Execute(stmt Statement) error {
	return cb.ExecuteWithOptions(Options{}, stmt)
}

func (cb goCQLBackend) ExecuteWithOptions(opts Options, stmt Statement) error {
	qu := cb.session.Query(stmt.Query(), stmt.Values()...)
	if opts.Consistency != nil {
		qu = qu.Consistency(*opts.Consistency)
	}
	if opts.Context != nil {
		qu = qu.WithContext(opts.Context)
	}
	return qu.Exec()
}

func (cb goCQLBackend) ExecuteAtomically(stmts []Statement) error {
	return cb.ExecuteAtomicallyWithOptions(Options{}, stmts)
}

func (cb goCQLBackend) ExecuteAtomicallyWithOptions(opts Options, stmts []Statement) error {
	if len(stmts) == 0 {
		return nil
	}
	batch := cb.session.NewBatch(gocql.LoggedBatch)
	for i := range stmts {
		stmt := stmts[i]
		batch.Query(stmt.Query(), stmt.Values()...)
	}

	if opts.Consistency != nil {
		batch.Cons = *opts.Consistency
	}
	if opts.Context != nil {
		batch = batch.WithContext(opts.Context)
	}

	return cb.session.ExecuteBatch(batch)
}

// GoCQLSessionToQueryExecutor enables you to supply your own gocql session with your custom options
// Then you can use NewConnection to mint your own thing
// See #90 for more details
func GoCQLSessionToQueryExecutor(sess *gocql.Session) QueryExecutor {
	return goCQLBackend{
		session:              sess,
		readSuccessCounter:   promauto.NewCounter(prometheus.CounterOpts{Name: "cassandra_read_success"}),
		insertSuccessCounter: promauto.NewCounter(prometheus.CounterOpts{Name: "cassandra_insert_success"}),
		updateSuccessCounter: promauto.NewCounter(prometheus.CounterOpts{Name: "cassandra_update_success"}),
		deleteSuccessCounter: promauto.NewCounter(prometheus.CounterOpts{Name: "cassandra_delete_success"}),
		totalSuccessCounter:  promauto.NewCounter(prometheus.CounterOpts{Name: "cassandra_total_success"}),
		readErrorCounter:     promauto.NewCounter(prometheus.CounterOpts{Name: "cassandra_read_error"}),
		insertErrorCounter:   promauto.NewCounter(prometheus.CounterOpts{Name: "cassandra_insert_error"}),
		updateErrorCounter:   promauto.NewCounter(prometheus.CounterOpts{Name: "cassandra_update_error"}),
		deleteErrorCounter:   promauto.NewCounter(prometheus.CounterOpts{Name: "cassandra_delete_error"}),
		totalErrorCounter:    promauto.NewCounter(prometheus.CounterOpts{Name: "cassandra_total_error"}),
	}
}

func newGoCQLBackend(nodeIps []string, username, password string) (QueryExecutor, error) {
	cluster := gocql.NewCluster(nodeIps...)
	cluster.Consistency = gocql.One
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: username,
		Password: password,
	}
	sess, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	return GoCQLSessionToQueryExecutor(sess), nil
}

func (cb goCQLBackend) IncrementPrometheusCounterSuccess(counter string) {
	switch counter {
	case "read":
		cb.readSuccessCounter.Inc()
	case "insert":
		cb.insertSuccessCounter.Inc()
	case "update":
		cb.updateSuccessCounter.Inc()
	case "delete":
		cb.deleteSuccessCounter.Inc()
	}
	cb.totalSuccessCounter.Inc()
}

func (cb goCQLBackend) IncrementPrometheusCounterError(counter string) {
	switch counter {
	case "read":
		cb.readErrorCounter.Inc()
	case "insert":
		cb.insertErrorCounter.Inc()
	case "update":
		cb.updateErrorCounter.Inc()
	case "delete":
		cb.deleteErrorCounter.Inc()
	}
	cb.totalErrorCounter.Inc()
}
