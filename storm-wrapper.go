package stormwrapper

import (
	"log"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/asdine/storm"
	"github.com/asdine/storm/q"
	"github.com/grafov/bcast"
)

// Db is a database object that wrap the real db and his subscriptions
type Db struct {
	db                  *storm.DB
	path                string
	dbmap               map[string]*storm.DB
	bucketsubscriptions map[string]*bcast.Group
	sync.Mutex
	mapMutex sync.Mutex

	memory map[string]map[string]interface{}
}

// New return a new db object
func New(path string) *Db {

	var db = &Db{}
	db.path = path
	db.dbmap = make(map[string]*storm.DB)
	db.bucketsubscriptions = make(map[string]*bcast.Group)

	return db
}

// Init initialize a bucket and indexes to be ready to save an object
func (db *Db) Init(data interface{}) {

	dbc := db.GetDbForValue(data)
	dbc.Init(data)
}

// Save an object to the database
func (db *Db) Save(data interface{}) error {

	// Save to db
	dbc := db.GetDbForValue(data)
	err := dbc.Save(data)
	if err != nil {
		return err
	}

	// Getting type of data
	s := reflect.ValueOf(data)
	if s.Kind() == reflect.Ptr {
		s = s.Elem()
	}
	if s.Kind() != reflect.Struct {
		return nil
	}
	name := s.Type().Name()

	// Broadcast value to subscribed handlers

	var group *bcast.Group
	var ok bool
	db.Lock()
	group, ok = db.bucketsubscriptions[name]
	db.Unlock()

	if ok {

		group.Send(data)
	}

	return nil
}

// Get return an object from the database looking by fieldname = value and saving to "to"
func (db *Db) Get(fieldName string, value interface{}, to interface{}) error {

	dbc := db.GetDbForValue(to)
	err := dbc.One(fieldName, value, to)
	return err
}

// GetFilter return various objects from the database looking by fieldname = value and saving to "to"
func (db *Db) GetFilter(fieldName string, value interface{}, to interface{}) error {

	dbc := db.GetDbForValue(to)
	err := dbc.Find(fieldName, value, to)
	return err
}

// GetAll return a table from the database
func (db *Db) GetAll(to interface{}) error {

	dbc := db.GetDbForValue(to)
	err := dbc.All(to)
	return err
}

// GetRegex return various objects from the database looking by fieldname = regex and saving to "to"
func (db *Db) GetRegex(fieldName, regex string, to interface{}) error {

	dbc := db.GetDbForValue(to)
	err := dbc.Select(q.Re(fieldName, regex)).Find(to)

	return err
}

// Remove remove an object from the database
func (db *Db) Remove(item interface{}) error {

	dbc := db.GetDbForValue(item)
	err := dbc.DeleteStruct(item)
	return err
}

// Drop remove a table from the database
func (db *Db) Drop(bucketName string) {

	os.Remove(db.path + "/" + bucketName + ".db")
}

// SubscribeChanges request the application to notify when a table changes
func (db *Db) SubscribeChanges(bucketName string) *bcast.Member {

	var group *bcast.Group
	var ok bool
	db.Lock()
	group, ok = db.bucketsubscriptions[bucketName]

	if !ok {

		db.bucketsubscriptions[bucketName] = bcast.NewGroup()
		group = db.bucketsubscriptions[bucketName]
		go group.Broadcast(0)
	}

	db.Unlock()

	member := group.Join()

	return member
}

type callback func(interface{})

// SubscribeChangesCallback request the application to notify when a table changes and call the callback passed as parameter
func (db *Db) SubscribeChangesCallback(bucketName string, cb callback) {

	sub := db.SubscribeChanges(bucketName)
	go func() {

		for {

			select {
			case val := <-sub.Read:

				go cb(val)
			}
		}
	}()
}

// GetDbForValue return the database for the value
func (db *Db) GetDbForValue(value interface{}) *storm.DB {

	i := reflect.TypeOf(value).Elem()
	ia := strings.Split(i.String(), ".")
	name := ia[len(ia)-1]

	if name == "" {

		s := reflect.ValueOf(value)
		if s.Kind() == reflect.Ptr {
			s = s.Elem()
		}
		if s.Kind() != reflect.Struct {
			return nil
		}
		name = s.Type().Name()
	}

	db.mapMutex.Lock()
	if val, ok := db.dbmap[name]; ok {

		db.mapMutex.Unlock()
		return val
	}

	ndb, err := storm.Open(db.path + "/" + name + ".db")
	if err != nil {
		log.Println(err)
	}
	db.dbmap[name] = ndb

	db.mapMutex.Unlock()
	return ndb
}

// SetToMemoryDb write  a value to the in-memory database
func (db *Db) SetToMemoryDb(dbname, key string, value interface{}) {

}

// GetFromMemoryDb return a value from memory database
func (db *Db) GetFromMemoryDb(dbname, key string) interface{} {

	return nil
}
