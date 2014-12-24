package mongo

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/docker/swarm/discovery"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"strings"
	"time"
)

var (
	ErrUnknownDBName     = errors.New("the mongodb database name must be provided")
	ErrFiledNotExists    = errors.New("the url field not exists in the database")
	ErrInvalidFieldValue = errors.New("the value of the url field must be string")
)

const (
	DEFAULT_COLLECTION_NAME = "nodes"
	DEFAULT_FIELD_NAME      = "url"
)

type MongoDiscoveryService struct {
	session    *mgo.Session
	dbname     string
	collection string
	field      string
	heartbeat  int
}

func init() {
	discovery.Register("mongo", &MongoDiscoveryService{})
}

// The following forms of uris are supported:
//
//     mongo://192.168.122.122:27017/dbname
//     mongo://192.168.122.122:27017/dbname/collection
//     mongo://192.168.122.122:27017/dbname/collection/field
func (s *MongoDiscoveryService) Initialize(uris string, heartbeat int) error {
	var (
		err   error
		segs  = strings.Split(uris, "/")
		nsegs = len(segs)
	)

	if nsegs < 2 {
		return ErrUnknownDBName
	}

	s.dbname = segs[1]
	s.heartbeat = heartbeat
	if nsegs >= 3 {
		s.collection = segs[2]
	} else {
		s.collection = DEFAULT_COLLECTION_NAME
	}

	if nsegs == 4 {
		s.field = segs[3]
	} else {
		s.field = DEFAULT_FIELD_NAME
	}

	s.session, err = mgo.Dial("mongodb://" + segs[0] + "/" + segs[1])
	if err != nil {
		log.Errorf("MongoDB connection failed. %s", err.Error())
		return err
	}
	db := s.session.DB(s.dbname)

	return db.C(s.collection).EnsureIndex(mgo.Index{
		Key:    []string{s.field},
		Unique: true,
	})
}

func (s *MongoDiscoveryService) Fetch() ([]*discovery.Node, error) {
	var (
		nodes   []*discovery.Node
		results []map[string]interface{}
		ss      = s.session.Clone()
		db      = ss.DB(s.dbname)
	)
	defer ss.Close()

	// perhaps Find(bson.M{"status":"ok"}) should be better?
	err := db.C(s.collection).Find(nil).All(&results)
	if err != nil {
		return nil, err
	}
	for _, n := range results {
		if _, ok := n[s.field]; !ok {
			return nil, ErrFiledNotExists
		}
		if url, ok := n[s.field].(string); ok {
			nodes = append(nodes, discovery.NewNode(url))
		} else {
			return nil, ErrInvalidFieldValue
		}
	}
	return nodes, nil
}

func (s *MongoDiscoveryService) Watch(callback discovery.WatchCallback) {
	for _ = range time.Tick(time.Duration(s.heartbeat) * time.Second) {
		nodes, err := s.Fetch()
		if err == nil {
			callback(nodes)
		}
	}
}

func (s *MongoDiscoveryService) Register(addr string) error {
	var (
		ss     = s.session.Clone()
		db     = ss.DB(s.dbname)
		record = map[string]string{s.field: addr, "comment": "registered by swarm"}
	)
	defer ss.Close()
	if !strings.HasPrefix(addr, "http://") {
		record[s.field] = "http://" + addr
	}
	err := db.C(s.collection).Insert(record)
	if mgo.IsDup(err) {
		return nil
	}
	return err
}

func (s *MongoDiscoveryService) Deregister(addr string) error {
	var (
		ss = s.session.Clone()
		db = ss.DB(s.dbname)
	)
	defer ss.Close()
	// or Update(bson.M{s.field: addr}, bson.M{"$set": bson.M{"status":"removed"}})
	return db.C(s.collection).Remove(bson.M{s.field: addr})
}
