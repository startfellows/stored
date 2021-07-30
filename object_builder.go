package stored

import (
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// ObjectBuilder is main interface to declare objects
type ObjectBuilder struct {
	object   *Object
	waitInit sync.WaitGroup // waiter for main directory
	waitAll  sync.WaitGroup // waiter for all planned async operations
	mux      sync.Mutex
	scheme   schemeFull
}

func (ob *ObjectBuilder) panic(text string) {
	panic("Stored error, object «" + ob.object.name + "» declaration: " + text)
}

func (ob *ObjectBuilder) buildScheme(schemeObj interface{}) {
	t := reflect.TypeOf(schemeObj)
	v := reflect.ValueOf(schemeObj)
	if v.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	o := ob.object
	ob.mux.Lock()
	o.reflectType = t
	numfields := v.NumField()
	o.immutableFields = map[string]*Field{}
	o.mutableFields = map[string]*Field{}
	primaryFields := []string{}
	for i := 0; i < numfields; i++ {
		field := Field{
			object: o,
			Num:    i,
			Type:   t.Field(i),
			Value:  v.Field(i),
		}

		field.Kind = field.Value.Kind()
		if field.Kind == reflect.Slice {
			field.SubKind = field.Value.Type().Elem().Kind()
		}

		tag := field.ParseTag()
		if tag != nil {
			field.Name = tag.Name
			if tag.AutoIncrement {
				field.SetAutoIncrement()
			}
			if tag.Primary {
				primaryFields = append(primaryFields, tag.Name)
				//o.setPrimary(tag.Name)
				//panic("not implemented yet")
			}

			if tag.UnStored {
				field.UnStored = true
			} else {
				o.keysCount++
			}

			if tag.mutable {
				o.mutableFields[tag.Name] = &field
				field.mutable = true
			} else {
				if field.UnStored {
					// Looks like an field is unstored, so it MUST be an mutable field, any way we would not write it anywhere
					o.mutableFields[tag.Name] = &field
					field.mutable = true
				} else {
					// Do we want to store id as immutable field? Looks like yes, due panics about it
					o.immutableFields[tag.Name] = &field
				}
			}
			// init unique field here, tmp disabled, need to test
			//if tag.unique {
			//	ob.Unique(field.Name)
			//}
		}
	}
	ob.mux.Unlock()
	if len(primaryFields) > 0 {
		ob.Primary(primaryFields...)
	}
	return
}

func (ob *ObjectBuilder) addIndex(indexKey string) *Index {
	o := ob.object
	ob.mux.Lock()
	_, ok := o.indexes[indexKey]
	ob.mux.Unlock()
	if ok {
		ob.panic("already has index «" + indexKey + "»")
	}

	index := Index{
		Name:   indexKey,
		object: o,
	}

	ob.waitAll.Add(1)
	go func() {
		ob.waitInit.Wait()
		// at this point in time all index properties are probably set up and configured
		indexSubspace, err := o.dir.CreateOrOpen(o.db, []string{indexKey}, nil)
		if err != nil {
			panic(err)
		}
		index.dir = indexSubspace
		if index.needValueStore() {
			indexSubspace, err = o.dir.CreateOrOpen(o.db, []string{indexKey, "value"}, nil)
			if err != nil {
				panic(err)
			}
			index.valueDir = indexSubspace
		}
		ob.mux.Lock()
		o.indexes[indexKey] = &index
		ob.mux.Unlock()
		ob.waitAll.Done()
	}()

	return &index
}

func (ob *ObjectBuilder) addFieldIndex(fieldKeys []string) *Index {
	ob.mux.Lock()

	fields := make([]*Field, len(fieldKeys))
	for k, keyName := range fieldKeys {
		fields[k] = ob.object.field(keyName)
	}
	ob.mux.Unlock()

	index := ob.addIndex(strings.Join(fieldKeys, ","))
	index.fields = fields
	return index
}

func (ob *ObjectBuilder) addGeoIndex(latKey, longKey, indexKey string) *Index {
	ob.mux.Lock()
	latField := ob.object.field(latKey)
	longField := ob.object.field(longKey)
	ob.mux.Unlock()

	index := ob.addIndex(indexKey)
	//index.field = field
	index.fields = []*Field{latField, longField}
	return index
}

func (ob *ObjectBuilder) need() {
	o := ob.object
	o.init()
	res, err := o.db.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		scheme := schemeFull{}
		scheme.load(ob, o.miscDir, tr)
		return scheme, nil
	})
	if err != nil {
		ob.panic("could not read schema")
	}
	ob.mux.Lock()
	ob.scheme = res.(schemeFull)
	ob.mux.Unlock()
	//fmt.Println("init -1")
	ob.waitInit.Done()
	//fmt.Println("all -1")
	ob.waitAll.Done()
}

// Done will finish the object
func (ob *ObjectBuilder) Done() *Object {
	ob.waitAll.Wait()
	ob.scheme.buildCurrent(ob)
	return ob.object
}

// Primary sets primary field in case it wasnot set with annotations
func (ob *ObjectBuilder) Primary(names ...string) *ObjectBuilder {
	ob.mux.Lock()
	for _, name := range names {
		_ = ob.object.field(name)
	}
	//ob.object.setPrimary(names...)
	o := ob.object
	var name string
	if len(names) == 1 {
		name = names[0]
	}
	if o.primaryKey != "" {
		for k, name := range names {
			if o.primaryFields[k].Name != name {
				o.panic("primary key already set to «" + o.primaryKey + "», could not set to «" + strings.Join(names, ", ") + "»")
			}
		}
		o.panic("primary key already set to «" + o.primaryKey + "», could not set to «" + name + "»")
	}

	if len(names) > 1 {
		o.primaryFields = []*Field{}
		for _, rangeName := range names {
			field := o.field(rangeName)
			field.primary = true
			o.primaryFields = append(o.primaryFields, field)
		}
		o.primaryKey = names[0]
		o.multiplePrimary = true
	} else {
		o.primaryKey = name
		field := o.field(name)
		field.primary = true
		o.primaryFields = []*Field{field}
	}

	ob.mux.Unlock()
	ob.waitAll.Add(1)
	//fmt.Println("all +1")
	go func() {
		ob.waitInit.Wait()
		var err error
		o.primary, err = o.dir.CreateOrOpen(o.db, names, nil)
		if err != nil {
			ob.panic(err.Error())
		}
		ob.waitAll.Done()
		//fmt.Println("all -1")
	}()
	return ob
}

// IDDate is unique id generated using date as first part, this approach is usefull
// if date index necessary too
// field type should be int64
func (ob *ObjectBuilder) IDDate(fieldName string) *ObjectBuilder {
	ob.mux.Lock()
	field := ob.object.field(fieldName)
	field.SetID(GenIDDate)
	ob.mux.Unlock()

	return ob
}

// IDRandom is unique id generated using random number, this approach is usefull
// if you whant randomly distribute objects, and you do not whant to unveil data object
func (ob *ObjectBuilder) IDRandom(fieldName string) *ObjectBuilder {
	ob.mux.Lock()
	field := ob.object.field(fieldName)
	field.SetID(GenIDRandom)
	ob.mux.Unlock()

	return ob
}

// AutoIncrement make defined field autoincremented before adding new objects
//
func (ob *ObjectBuilder) AutoIncrement(name string) *ObjectBuilder {
	ob.mux.Lock()
	field := ob.object.field(name)
	field.SetAutoIncrement()
	ob.mux.Unlock()

	return ob
}

// Unique index: if object with same field value already presented, Set and Add will return an ErrAlreadyExist
func (ob *ObjectBuilder) Unique(names ...string) *ObjectBuilder {
	index := ob.addFieldIndex(names)
	index.Unique = true
	return ob
}

// UniqueOptional index: if object with same field value already presented, Set and Add will return an ErrAlreadyExist
// If the value is empty index do not set
func (ob *ObjectBuilder) UniqueOptional(names ...string) *ObjectBuilder {
	index := ob.addFieldIndex(names)
	index.Unique = true
	index.optional = true
	return ob
}

// Index add an simple index for specific key or set of keys
func (ob *ObjectBuilder) Index(names ...string) *ObjectBuilder {
	ob.addFieldIndex(names)
	return ob
}

// IndexOptional is the simple index which will be written only if field is not empty
func (ob *ObjectBuilder) IndexOptional(names ...string) *ObjectBuilder {
	index := ob.addFieldIndex(names)
	index.optional = true
	return ob
}

// FastIndex will set index storing copy of object, performing denormalisation
func (ob *ObjectBuilder) FastIndex(names ...string) *ObjectBuilder {
	ob.mux.Lock()
	for _, name := range names {
		_ = ob.object.field(name)
	}
	ob.mux.Unlock()
	// init fast index here
	return ob
}

// IndexGeo will add and geohash based index to allow geographicly search objects
// geoPrecision 0 means full precision:
// 10 < 1m, 9 ~ 7.5m, 8 ~ 21m, 7 ~ 228m, 6 ~ 1.8km, 5 ~ 7.2km, 4 ~ 60km, 3 ~ 234km, 2 ~ 1890km, 1 ~ 7500km
func (ob *ObjectBuilder) IndexGeo(latKey string, longKey string, geoPrecision int) *IndexGeo {
	index := ob.addGeoIndex(latKey, longKey, latKey+","+longKey+":"+strconv.Itoa(geoPrecision))
	if geoPrecision < 1 || geoPrecision > 12 {
		geoPrecision = 12
	}
	index.Geo = geoPrecision
	return &IndexGeo{index: index}
}

// IndexCustom add an custom index generated dynamicly using callback function
// custom indexes in an general way to implement any index on top of it
func (ob *ObjectBuilder) IndexCustom(key string, cb func(object interface{}) KeyTuple) *Index {
	index := ob.addIndex(key)
	index.handle = cb
	return index
}

// IndexSearch will add serchable index which will allow
func (ob *ObjectBuilder) IndexSearch(key string, options ...IndexOption) *IndexSearch {
	index := ob.addFieldIndex([]string{key})
	field := ob.object.field(key)
	if field.Kind != reflect.String {
		ob.panic("field " + key + " should be string for IndexSearch")
	}
	for _, opt := range options {
		index.SetOption(opt)
	}
	index.search = true

	return &IndexSearch{Index: index}
}

// Counter will count all objects with same value of passed fields
func (ob *ObjectBuilder) Counter(fieldNames ...string) *Counter {
	fields := []*Field{}
	ob.mux.Lock()
	for _, fieldName := range fieldNames {
		field := ob.object.field(fieldName)
		fields = append(fields, field)
	}
	ob.mux.Unlock()
	return counterNew(ob, fields)
}

// N2N Creates object to object relation between current object and other one.
// Other words it represents relations when unlimited number of host objects connected to unlimited
// amount of client objects
func (ob *ObjectBuilder) N2N(client *ObjectBuilder, name string) *Relation {
	rel := Relation{name: name}
	rel.init(RelationN2N, ob.object, client.object)
	return &rel
}
