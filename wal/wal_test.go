package wal

// func TestWalWriter(t *testing.T) {
// 	key := []byte("xiasang")
// 	writer, err := NewWal(1)
// 	t.Log(err)
// 	_, err = writer.Write(key, key)
// 	t.Log(err)
// }
// func TestWalWriters(t *testing.T) {
// 	writer, err := NewWal(1)
// 	t.Log(err)
// 	for i := range 120 {
// 		key, value := utils.GenerateKey(i), utils.GenerateRandomBytes(12)
// 		fmt.Printf("k:%s,v:%s\n", key, value)
// 		_, err = writer.Write(key, value)
// 		t.Log(err)
// 		if i < 100 {
// 			_, err = writer.Write(key, nil)
// 			t.Log(err)
// 		}
// 	}

// }
// func TestWalReader(t *testing.T) {
// 	// key := []byte("xiasang")
// 	reader, err := NewWal(1)
// 	t.Log(err)
// 	btree := memtable.NewBTreeMemTable()
// 	err = reader.read(btree)
// 	t.Log(err)
// 	for iter := btree.Iterator(); iter.Valid(); iter.Next() {
// 		key, value := iter.Curr()
// 		fmt.Printf("%s:%s\n", key, value)
// 	}

// }
