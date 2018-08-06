package stored

import (
	"reflect"
	"strconv"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Query is interface for query building
type Query struct {
	object  *Object
	index   *Index
	primary tuple.Tuple
	from    tuple.Tuple
	next    struct {
		from    tuple.Tuple // fills after first slice of data was scanned
		started bool
	}
	limit   int
	reverse bool
	fn      func()
}

// Use is an index selector for query building
func (q *Query) Use(index string) *Query {
	for key, i := range q.object.Indexes {
		if key == index {
			q.index = i
			return q
		}
	}
	q.object.panic("index " + index + " is undefined")
	return q
}

// Limit sets limit for the query
func (q *Query) Limit(limit int) *Query {
	q.limit = limit
	return q
}

// Reverse reverse the query order
func (q *Query) Reverse() *Query {
	q.reverse = true
	return q
}

// List queries list of items using primary key subspace. Pass no params if fetching all objects
func (q *Query) List(values ...interface{}) *Query {
	if len(values) == 0 {
		return q
	}
	q.primary = tuple.Tuple{}
	for _, v := range values {
		q.primary = append(q.primary, v)
	}

	maxFields := len(q.object.primaryFields)
	if len(q.primary) >= maxFields {
		q.object.panic("List should have less than " + strconv.Itoa(maxFields) + " params (number of primary keys)")
	}
	return q
}

// From sets the part of primary key of item starting from which result will be returned
// primary key part passed to List param should be excluded
func (q *Query) From(values ...interface{}) *Query {
	if len(values) == 0 {
		return q
	}
	q.from = tuple.Tuple{}
	for _, v := range values {
		q.from = append(q.from, v)
	}
	return q
}

// ScanAll scans the query result into the passed
func (q *Query) ScanAll(slicePointer interface{}) error {
	// sould be queried here
	sliceI := q.execute()
	slice := sliceI.(*Slice)
	return slice.ScanAll(slicePointer)
}

// execute the query
func (q *Query) execute() interface{} {
	q.next.started = true
	keyLen := len(q.object.primaryFields)
	resp, err := q.object.db.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		if q.index != nil { // select using index
			values, err := q.index.getList(tr, q)
			if err != nil {
				return nil, err
			}
			slice := Slice{}
			for _, needed := range values {
				v, err := needed.fetch()
				if err != nil {
					return nil, err
				}
				slice.Append(v)
			}
			return &slice, nil
		}

		var sub subspace.Subspace
		sub = q.object.primary
		if q.primary != nil {
			sub = sub.Sub(q.primary...)
		}
		start, end := sub.FDBRangeKeys()
		if q.from != nil {
			if q.reverse {
				end = sub.Pack(q.from)
			} else {
				start = sub.Pack(q.from)
			}
		}
		r := fdb.KeyRange{Begin: start, End: end}
		rangeResult := tr.GetRange(r, fdb.RangeOptions{Mode: fdb.StreamingModeWantAll, Limit: q.object.getKeyLimit(q.limit), Reverse: q.reverse})
		iterator := rangeResult.Iterator()
		elem := valueRaw{}
		res := []valueRaw{}
		var lastTuple tuple.Tuple
		rowsNum := 0
		for iterator.Advance() {
			kv, err := iterator.Get()
			if err != nil {
				return nil, err
			}
			fullTuple, err := q.object.primary.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}

			if len(fullTuple) <= keyLen {
				return nil, ErrDataCorrupt
			}
			primaryTuple := fullTuple[:keyLen]

			q.next.from = primaryTuple // set up nextFrom

			if lastTuple != nil && !reflect.DeepEqual(primaryTuple, lastTuple) {
				// push to items here
				res = append(res, elem)
				elem = valueRaw{}
				rowsNum = 0
			}
			fieldsKey := fullTuple[keyLen:]
			if len(fieldsKey) > 1 {
				q.object.panic("nested fields not yet supported")
			}
			keyName, ok := fieldsKey[0].(string)
			if !ok {
				q.object.panic("invalid key, not string")
			}
			elem[keyName] = kv.Value
			lastTuple = primaryTuple
			rowsNum++
		}
		if rowsNum != 0 {
			res = append(res, elem)
		}
		if len(res) == 0 {
			return &Slice{values: []*Value{}}, nil
			//return nil, ErrNotFound
		}

		return q.object.wrapObjectList(res)
	})
	if err != nil {
		return &Slice{err: err}
	}
	return resp
}

// Next sets from identifier from nextFrom; return true if more data could be fetched
func (q *Query) Next() bool {
	if q.next.started {
		if q.next.from != nil {
			q.from = q.next.from
			return true
		}
		return false
	}
	return true
}
