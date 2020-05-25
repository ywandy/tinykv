package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneStorageReader struct {
	Txn *badger.Txn
	Db  *badger.DB
}

func (s *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	dat, err := engine_util.GetCF(s.Db, cf, key)
	if err != nil {
		if err.Error() == "Key not found" {
			return nil, err
		}
	}
	return dat, err
}

func (s *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.Txn)
}

func (s *StandAloneStorageReader) Close() {
	s.Txn.Discard()
}
