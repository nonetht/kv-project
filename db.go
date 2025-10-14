package index

// DB bitcask 存储引擎实例
type DB struct {
}

func (db *DB) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return nil
	}

}

func (db *DB) Get(key []byte) ([]byte, error) {

}

func (db *DB) Delete(key []byte) error {

}
