package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	config *config.Config
	db     *badger.DB
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	//初始化一个新的数据存储
	db := engine_util.CreateDB("single", conf)
	eng := engine_util.NewEngines(db, nil, "single", "")
	return &StandAloneStorage{
		config: conf,
		db:     db,
		engine: eng,
	}
}

func (s *StandAloneStorage) Start() error {
	s.engine = engine_util.NewEngines(s.db, nil, "single", "")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	standAloneStorageReader := &StandAloneStorageReader{}
	txn := s.db.NewTransaction(false)
	standAloneStorageReader.Txn = txn
	standAloneStorageReader.Db = s.db
	return standAloneStorageReader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, dat := range batch {
		switch dat.Data.(type) {
		case storage.Put:
			if err := engine_util.PutCF(s.db, dat.Cf(), dat.Key(), dat.Value()); err != nil {
				return err
			}
		case storage.Delete:
			if err := engine_util.DeleteCF(s.db, dat.Cf(), dat.Key()); err != nil {
				return err
			}
		}
	}
	return nil
}
