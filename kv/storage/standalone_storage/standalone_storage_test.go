package standalone_storage

import (
	"github.com/pingcap-incubator/tinykv/kv/config"
	st "github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func TestNewStandAloneStorage(t *testing.T) {
	tmpDir, err := ioutil.TempDir("/tmp", "tinykv")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	conf := &config.Config{
		DBPath: tmpDir,
	}
	storage := NewStandAloneStorage(conf)
	assert.NotNil(t, storage)
	assert.Nil(t, storage.Start())
	defer storage.Stop()

	cf := "cf1"
	key := []byte("key1")
	val := []byte("hello, tiny thing")
	batch := []st.Modify{
		{Data: st.Put{Key: key, Value: val, Cf: cf}},
	}
	err = storage.Write(nil, batch)
	assert.Nil(t, err)

	reader, err := storage.Reader(nil)
	assert.Nil(t, err)
	res, err := reader.GetCF(cf, key)
	assert.Equal(t, res, val)
	log.Printf("res:%s, val:%s\n", string(res), string(val))
	reader.Close()
}

func TestStandAloneReader_IterCF(t *testing.T) {
	tmpDir, err := ioutil.TempDir("/tmp", "tinykv")
	assert.Nil(t, err)
	defer os.RemoveAll(tmpDir)
	conf := &config.Config{
		DBPath: tmpDir,
	}
	storage := NewStandAloneStorage(conf)
	assert.NotNil(t, storage)
	assert.Nil(t, storage.Start())
	defer storage.Stop()

	var cfPrefix = "cf"
	var batch []st.Modify
	answer := make(map[string][]byte)
	cfNum := 10
	keyNum := 20
	for i := 0; i < cfNum; i++ {
		cf := cfPrefix + strconv.Itoa(i)
		for j := 0; j < keyNum; j++ {
			key := "key" + strconv.Itoa(j)
			val := generateRandVal(int(rand.Int31n(100) + 10))
			batch = append(batch, st.Modify{Data: st.Put{Cf: cf, Key: []byte(key), Value: val}})
			insertKey := cf + "_" + key
			answer[insertKey] = val
		}
	}
	err = storage.Write(nil, batch)
	assert.Nil(t, err)

	reader, err := storage.Reader(nil)
	defer reader.Close()
	assert.Nil(t, err)
	for i := 0; i < cfNum; i++ {
		cf := cfPrefix + strconv.Itoa(i)
		it := reader.IterCF(cf)
		for it.Seek([]byte(cfPrefix)); it.Valid(); it.Next() {
			item := it.Item()
			val, _ := item.Value()
			//log.Printf("read key:%s, val:%s\n", string(item.Key()), string(val))
			insertKey := cf + "_" + string(item.Key())
			assert.Equal(t, val, answer[insertKey])
		}
		it.Close()
	}

}

func generateRandVal(length int) []byte {
	res := make([]byte, 0, length)
	charset := []byte("abcdefghijklmnopqrstuvwxyz")
	for i := 0; i < length; i++ {
		index := rand.Intn(len(charset))
		res = append(res, charset[index])
	}
	return res
}

