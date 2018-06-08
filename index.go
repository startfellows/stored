package stored

import (
	"errors"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// Index represend all indexes sored has
type Index struct {
	Name   string
	Unique bool
	dir    directory.DirectorySubspace
	object *Object
	field  *Field
}

func (i *Index) Write(tr fdb.Transaction, primary interface{}, primaryBytes []byte, input *Struct) error {
	indexValue := input.Get(i.field)
	if i.Unique {
		key := tuple.Tuple{indexValue}
		previousPromise := tr.Get(i.dir.Pack(key))

		tr.Set(i.dir.Pack(key), primaryBytes) // will be cancelled in case of error

		previousBytes, err := previousPromise.Get()
		if err != nil {
			return err
		}
		if len(previousBytes) != 0 {
			previous := i.object.GetPrimaryField().ToInterface(previousBytes)
			if previous != primary {
				return errors.New("Object with this index already set")
			}
		}
	} else {
		key := tuple.Tuple{indexValue, primary}
		tr.Set(i.dir.Pack(key), []byte{})
	}
	return nil
}

func (i *Index) GetPrimary(tr fdb.ReadTransaction, data interface{}) (subspace.Subspace, error) {
	indexKey := tuple.Tuple{data}
	if i.Unique {
		bytes, err := tr.Get(i.dir.Pack(indexKey)).Get()
		if err != nil {
			return nil, err
		}
		if len(bytes) == 0 {
			return nil, ErrNotFound
		}
		primaryField := i.object.GetPrimaryField()
		primaryData := primaryField.ToInterface(bytes)

		return i.object.primary.Sub(primaryData), nil
	} else {
		sel := fdb.FirstGreaterThan(i.dir.Pack(indexKey))
		primaryKey, err := tr.GetKey(sel).Get()
		if err != nil {
			return nil, err
		}
		primary, err := i.dir.Unpack(primaryKey)
		//primary, err := UnpackKeyIndex(indexKey, primaryKey)
		if err != nil || len(primary) < 2 || primary[0] != data {
			return nil, ErrNotFound
		}

		return i.object.primary.Sub(primary[1]), nil
	}
}
