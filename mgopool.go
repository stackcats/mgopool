package mgopool

import (
	"log"
	"math/rand"
	"time"

	"github.com/globalsign/mgo"
)

const (
	sessionTimeout = 60 * 60 * 3
	poolSize       = 128
)

// DB ...
type DB struct {
	dbname      string
	sessionPool [poolSize]*mgo.Session
	timestamp   [poolSize]int64
}

var db *DB

// NewDB ...
func NewDB(name string, addr string) (*DB, error) {
	if db == nil {

		db = &DB{}
		db.dbname = name

		mongoDBDialInfo, err := mgo.ParseURL(addr)
		if err != nil {
			return nil, err
		}

		session, err := mgo.DialWithInfo(mongoDBDialInfo)
		if err != nil {
			return nil, err
		}

		session.SetMode(mgo.Monotonic, true)
		session.SetPoolLimit(512)

		for i := 0; i < poolSize; i++ {
			db.sessionPool[i] = session.Copy()
			db.timestamp[i] = time.Now().Unix()
		}
	}

	return db, nil
}

// Collection ...
func (b *DB) Collection(c string) *mgo.Collection {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	ndx := r.Intn(poolSize)
	if ndx == 0 {
		ndx = 1
	}
	defer func() {
		if err := recover(); err != nil {
			log.Println(err)
			b.sessionPool[0].Refresh()
			b.sessionPool[ndx] = b.sessionPool[0].Copy()
		}
	}()
	now := time.Now().Unix()
	if now-b.timestamp[ndx] > sessionTimeout {
		log.Printf("fresh index %d, %d, %d", ndx, b.timestamp[ndx], now)
		b.sessionPool[ndx].Refresh()
	}
	b.timestamp[ndx] = now
	return b.sessionPool[ndx].DB(b.dbname).C(c)
}

// Close ...
func (b *DB) Close() {
	for i := 0; i < poolSize; i++ {
		b.sessionPool[i].Close()
	}
}
