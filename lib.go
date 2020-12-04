package mongo

import (
	"fmt"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

const (
	defaultConTimeout = 15 * time.Second
	defaultMaxTimeMS  = 30 * time.Second
	errorNotConnected = "DB is not connected"
	errorNotValid     = "Query is not valid"
)

// DB for database
type DB struct {
	sync.RWMutex

	sess      *mgo.Session
	maxTimeMS time.Duration
}

// M for bson.M object
type M = bson.M

// D for bson.D object
type D = bson.D

func NewConnection(dsn string) (*DB, error) {
	var db = DB{
		maxTimeMS: defaultMaxTimeMS,
	}
	return &db, db.Connect(dsn)
}

func NewConnectionWithTimeout(dsn string, timeout time.Duration) (*DB, error) {
	var db = DB{
		maxTimeMS: defaultMaxTimeMS,
	}
	return &db, db.ConnectWithTimeout(dsn, timeout)
}

func GetDb() *DB { return &DB{} }

func (db *DB) IsConnected() bool {
	return db.sess != nil
}

func (db *DB) Connect(dsn string) error {
	var err error

	db.sess, err = mgo.DialWithTimeout(dsn, defaultConTimeout)

	return err
}

func (db *DB) ConnectWithTimeout(dsn string, timeout time.Duration) error {
	var err error

	if timeout < time.Second {
		timeout = defaultConTimeout
	}

	db.sess, err = mgo.DialWithTimeout(dsn, timeout)

	return err
}

func (db *DB) SetMaxTimeMS(d time.Duration) {
	db.RWMutex.Lock()
	db.maxTimeMS = d
	db.RWMutex.Unlock()
}

func (db *DB) Disconnect() {
	if db.IsConnected() {
		db.sess.Close()
	}
}

func (db *DB) CreateIndexKey(coll string, key ...string) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()
	defer sess.Close()

	return sess.DB("").C(coll).EnsureIndexKey(key...)
}

func (db *DB) CreateIndexKeys(coll string, keys ...string) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var (
		err  error
		sess = db.sess.Copy()
	)

	defer sess.Close()

	for _, key := range keys {
		err = sess.DB("").C(coll).EnsureIndexKey(key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Insert(coll string, v ...interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Insert(v...)
}

func (db *DB) InsertBulk(coll string, v ...interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	var (
		err  error
		bulk = sess.DB("").C(coll).Bulk()
	)

	bulk.Unordered()
	bulk.Insert(v...)
	_, err = bulk.Run()

	return err
}

func (db *DB) InsertSess(coll string, sess *mgo.Session,
	v ...interface{}) error {
	if !db.IsConnected() || sess == nil {
		return fmt.Errorf("%s", errorNotConnected)
	}

	return sess.DB("").C(coll).Insert(v...)
}

func (db *DB) Find(coll string, query map[string]interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	var bsonQuery = bson.M{}

	for k, qv := range query {
		bsonQuery[k] = qv
	}

	return sess.DB("").C(coll).Find(bsonQuery).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *DB) Pipe(coll string, query []bson.M, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Pipe(query).AllowDiskUse().SetMaxTime(db.maxTimeMS).All(v)
}

func (db *DB) PipeOne(coll string, query []bson.M, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Pipe(query).AllowDiskUse().SetMaxTime(db.maxTimeMS).One(v)
}

func (db *DB) FindByID(coll string, id string, v interface{}) bool {
	if !db.IsConnected() {
		return false
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return mgo.ErrNotFound != sess.DB("").C(coll).FindId(id).SetMaxTime(db.maxTimeMS).One(v)
}

func (db *DB) FindAll(coll string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(bson.M{}).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *DB) FindWithQuery(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).SetMaxTime(db.maxTimeMS).One(v)
}

func (db *DB) FindWithQuerySortOne(coll string, query interface{},
	order string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(order).SetMaxTime(db.maxTimeMS).One(v)
}

func (db *DB) FindWithQuerySortAll(coll string, query interface{},
	order string, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(order).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *DB) FindWithQuerySortLimitAll(coll string, query interface{},
	order string, limit int, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(order).Limit(limit).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *DB) FindWithQueryOne(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).SetMaxTime(db.maxTimeMS).One(v)
}

func (db *DB) FindWithQueryAll(coll string, query interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *DB) FindWithQuerySortLimitOffsetAll(coll string, query interface{}, sort string,
	limit int, offset int, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).Sort(sort).Limit(limit).Skip(offset).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *DB) FindWithQuerySortLimitOffsetTotalAll(coll string, query interface{},
	sort string, limit int, offset int, v interface{}, total *int) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	if total != nil {
		*total, _ = sess.DB("").C(coll).Find(query).SetMaxTime(db.maxTimeMS).Count()
	}

	return sess.DB("").C(coll).Find(query).Sort(sort).Limit(limit).Skip(offset).SetMaxTime(db.maxTimeMS).All(v)
}

func (db *DB) Count(coll string, query interface{}) (int, error) {
	if !db.IsConnected() {
		return 0, fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Find(query).SetMaxTime(db.maxTimeMS).Count()
}

func (db *DB) Update(coll string, id interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Update(bson.M{"_id": id}, bson.M{"$set": v})
}

func (db *DB) UpdateWithQuery(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	return sess.DB("").C(coll).Update(query, set)
}

func (db *DB) UpdateWithQueryAll(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var (
		err  error
		sess = db.sess.Copy()
	)

	defer sess.Close()

	_, err = sess.DB("").C(coll).UpdateAll(query, set)

	return err
}

func (db *DB) Upsert(coll string, id interface{}, v interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	var _, err = sess.DB("").C(coll).Upsert(bson.M{"_id": id}, v)

	return err
}

func (db *DB) UpsertWithQuery(coll string, query interface{}, set interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	var _, err = sess.DB("").C(coll).Upsert(query, set)

	return err
}

func (db *DB) UpsertMulti(coll string, id []interface{}, v []interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	if len(id) != len(v) {
		return fmt.Errorf("%s", errorNotValid)
	}

	var (
		index = 0
		sess  = db.sess.Copy()
	)

	defer sess.Close()

	for index < len(id) {
		// TODO: fix errcheck linter issue: return value is not checked
		sess.DB("").C(coll).Upsert(bson.M{"_id": id[index]}, v[index])
		index++
	}

	return nil
}

func (db *DB) Remove(coll string, id interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	_, err := sess.DB("").C(coll).RemoveAll(bson.M{"_id": id})

	return err
}

func (db *DB) RemoveAll(coll string) error {
	var sess = db.sess.Copy()

	defer sess.Close()

	_, err := sess.DB("").C(coll).RemoveAll(bson.M{})

	return err
}

func (db *DB) RemoveWithQuery(coll string, query interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var (
		err  error
		sess = db.sess.Copy()
	)

	defer sess.Close()

	_, err = sess.DB("").C(coll).RemoveAll(query)

	return err
}

func (db *DB) RemoveWithIDs(coll string, ids interface{}) error {
	if !db.IsConnected() {
		return fmt.Errorf("%s", errorNotConnected)
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	_, err := sess.DB("").C(coll).RemoveAll(bson.M{"_id": bson.M{"$in": ids}})

	return err
}

func (db *DB) SessExec(cb func(*mgo.Session)) {
	if !db.IsConnected() {
		return
	}

	var sess = db.sess.Copy()

	defer sess.Close()

	cb(sess)
}

func (db *DB) SessCopy() *mgo.Session {
	if !db.IsConnected() {
		return nil
	}

	return db.sess.Copy()
}

func (db *DB) SessClose(sess *mgo.Session) {
	if !db.IsConnected() || sess == nil {
		return
	}

	sess.Close()
}
