package buntdb_test

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tidwall/buntdb"
)

func TestBuntdb_Open_file(t *testing.T) {
	db, err := buntdb.Open("open.db")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
}

func TestBuntdb_Open_memory(t *testing.T) {
	db, err := buntdb.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
}

func TestBuntdb_Set_Get(t *testing.T) {
	db, err := buntdb.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	err = db.Update(func(tx *buntdb.Tx) error {
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("name:%v", i)
			_, _, err := tx.Set(k, Sha1B64(k), nil)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(tx *buntdb.Tx) error {

		k := fmt.Sprintf("name:%v", "x")
		v, err := tx.Get(k)
		if err != nil {
			return err
		}

		t.Log(k, v)

		return nil
	})
	if err != nil {
		t.Log(err)
	}

	err = db.View(func(tx *buntdb.Tx) error {
		for i := 0; i < 10; i++ {
			k := fmt.Sprintf("name:%v", i)
			v, err := tx.Get(k)
			if err != nil {
				return err
			}

			t.Log(k, v)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

}

func Sha1B64(s string) string {
	hash := sha1.New()
	hash.Write([]byte(s))
	sum := hash.Sum(nil)
	b64 := base64.StdEncoding.EncodeToString(sum)
	return strings.ReplaceAll(b64, "=", "")
}

func Sha1Int(s string) string {
	hash := sha1.New()
	hash.Write([]byte(s))
	sum := hash.Sum(nil)
	bi := big.Int{}
	bi.SetBytes(sum)
	return bi.String()
}

func TestBuntdb_Json(t *testing.T) {
	db, err := buntdb.Open("test.db")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	db.View(func(tx *buntdb.Tx) error {
		tx.AscendKeys("*", func(key, value string) bool {
			fmt.Println(key, value)
			return true
		})
		return nil
	})

	type Name struct {
		First string `json:"first"`
		Last  string `json:"last"`
	}
	type User struct {
		ID   int  `json:"id"`
		Name Name `json:"name"`
		Age  uint `json:"age"`
	}

	users := []User{
		{1, Name{"tom", "Johnson"}, 38},
		{2, Name{"Janet", "Prichard"}, 47},
		{3, Name{"Carol", "Anderson"}, 52},
		{4, Name{"Alan", "Cooper"}, 28},
	}
	db.CreateIndex("last_name", "*", buntdb.IndexJSON("name.last"))
	db.CreateIndex("age", "*", buntdb.IndexJSON("age"))
	err = db.Update(func(tx *buntdb.Tx) error {
		for i, user := range users {
			b, err := json.Marshal(user)
			if err != nil {
				return err
			}
			_, _, err = tx.Set(strconv.Itoa(i), string(b), nil)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	err = db.View(func(tx *buntdb.Tx) error {
		for i, user := range users {

			v, err := tx.Get(strconv.Itoa(i))
			if err != nil {
				return err
			}
			var user_ = User{}
			err = json.Unmarshal([]byte(v), &user_)
			if err != nil {
				return err
			}

			t.Log(i, user)
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	db.View(func(tx *buntdb.Tx) error {
		fmt.Println("Order by last name")
		tx.Ascend("last_name", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age")
		tx.Ascend("age", func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		fmt.Println("Order by age range 30-50")
		tx.AscendRange("age", `{"age":30}`, `{"age":50}`, func(key, value string) bool {
			fmt.Printf("%s: %s\n", key, value)
			return true
		})
		return nil
	})

	db.Update(func(tx *buntdb.Tx) error {
		tx.Delete("1")
		return nil
	})
}

func TestBuntdb_MakeBigFile(t *testing.T) {
	os.Remove("big.db")
	db, err := buntdb.Open("big.db")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	s := strings.Repeat("A", 1<<12)

	err = db.Update(func(tx *buntdb.Tx) error {
		for i := 0; i < 1000000; i++ {
			_, _, err = tx.Set(strconv.Itoa(i), string(s), nil)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

}

func TestBuntdb_LoadBigFile(t *testing.T) {
	db, err := buntdb.Open("big.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.View(func(tx *buntdb.Tx) error {
		value, err := tx.Get("999999")
		if err != nil {
			return err
		}

		t.Log(value)

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

}

func TestBuntdb_Snapshot(t *testing.T) {
	db, err := buntdb.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	db2, err := buntdb.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	s := strings.Repeat("A", 1<<12)

	err = db.Update(func(tx *buntdb.Tx) error {
		for i := 0; i < 10; i++ {
			_, _, err = tx.Set(strconv.Itoa(i), string(s), nil)
			if err != nil {
				return err
			}
		}

		return nil
	})

	fd, err := os.Create("snapshot.db")
	if err != nil {
		t.Fatal(err)
	}
	defer fd.Close()

	fd2, err := os.Create("snapshot2.db")
	if err != nil {
		t.Fatal(err)
	}
	defer fd2.Close()

	err = db.Save(fd)
	if err != nil {
		t.Fatal(err)
	}

	err = db2.Load(fd)
	if err != nil {
		t.Fatal(err)
	}

	err = db2.Update(func(tx *buntdb.Tx) error {
		for i := 10; i < 20; i++ {
			_, _, err = tx.Set(strconv.Itoa(i), string(s), nil)
			if err != nil {
				return err
			}
		}

		return nil
	})

	err = db2.Save(fd2)
	if err != nil {
		t.Fatal(err)
	}
}

func TestBuntdb_Expiratin(t *testing.T) {
	db, err := buntdb.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var exCount int32
	var config buntdb.Config
	if err := db.ReadConfig(&config); err == nil {
		config.OnExpiredSync = func(key, value string, tx *buntdb.Tx) error {
			if _, err := tx.Delete(key); err != nil {
				// it's ok to get a "not found" because the
				// 'Delete' method reports "not found" for
				// expired items.
				if err != buntdb.ErrNotFound {
					return err
				}
			}

			atomic.AddInt32(&exCount, 1)

			return nil
		}
		// config.OnExpired = func(keys []string) {
		// 	atomic.AddInt32(&exCount, Len32(keys))
		// }
		db.SetConfig(config)
	}

	s := strings.Repeat("A", 1<<12)

	err = db.Update(func(tx *buntdb.Tx) error {
		for i := 0; i < 10; i++ {
			_, _, err = tx.Set(strconv.Itoa(i), string(s), &buntdb.SetOptions{Expires: true, TTL: time.Millisecond * 100})
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	<-time.After(time.Second * 2)

	t.Log("expiration:", exCount)

}

func Len32[T any](v []T) int32 {
	return int32(len(v))
}
