package local_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/kode4food/timebox/message"
	"github.com/kode4food/timebox/store"
	"github.com/kode4food/timebox/store/local"
	st "github.com/kode4food/timebox/store/test"
	"git.mills.io/prologic/bitcask"
	"github.com/stretchr/testify/assert"
)

var testEvents = message.List{
	message.New("string", "123"),
	message.New("string", "456"),
	message.New("string", "789"),
	message.New("string", "101112"),
}

func TestStore(t *testing.T) {
	dir, _ := ioutil.TempDir(os.TempDir(), "minibase")
	st.PerformTestStore(t, local.Open, local.Path(dir))
}

func TestReopen(t *testing.T) {
	as := assert.New(t)
	dir, _ := ioutil.TempDir(os.TempDir(), "minibase")
	s, err := local.Open(local.Path(dir))
	as.NotNil(s)
	as.Nil(err)

	res, err := s.All(store.NewID())
	as.Nil(err)

	res, err = res.Append(testEvents[0:2])
	as.Nil(err)
	st.MaybeClose(t, s)

	s, err = local.Open(local.Path(dir))
	as.NotNil(s)
	as.Nil(err)

	res, err = s.All(res.ID())
	as.Nil(err)
	as.Equal(store.Version(2), res.NextVersion())
	res, err = res.Rest()
	as.Nil(err)

	ev, err := res.Events()
	as.Nil(err)
	as.Equal(0, len(ev))

	res, err = res.Append(testEvents[2:4])
	as.Nil(err)

	events, err := res.Events()
	as.Nil(err)
	testEqualEvents(t, testEvents[2:], events)
	st.MaybeClose(t, s)
}

func TestStoreLocking(t *testing.T) {
	as := assert.New(t)

	dir, _ := ioutil.TempDir(os.TempDir(), "minibase")
	s1, err := local.Open(local.Path(dir))
	as.NotNil(s1)
	as.Nil(err)

	s2, err := local.Open(local.Path(dir))
	as.Nil(s2)
	as.NotNil(err)
	// TODO: This is a leaky abstraction
	as.Equal(err, bitcask.ErrDatabaseLocked)

	st.MaybeClose(t, s1)
	s3, err := local.Open(local.Path(dir))
	as.NotNil(s3)
	as.Nil(err)
	st.MaybeClose(t, s3)
}

func TestVersionInconsistency(t *testing.T) {
	dir, _ := ioutil.TempDir(os.TempDir(), "minibase")
	st.PerformTestVersionInconsistency(t, local.Open, local.Path(dir))
}

func TestPutNothing(t *testing.T) {
	dir, _ := ioutil.TempDir(os.TempDir(), "minibase")
	st.PerformTestPutNothing(t, local.Open, local.Path(dir))
}

func TestBefore(t *testing.T) {
	dir, _ := ioutil.TempDir(os.TempDir(), "minibase")
	st.PerformTestBefore(t, local.Open, local.Path(dir))
}

func TestBadBefore(t *testing.T) {
	dir, _ := ioutil.TempDir(os.TempDir(), "minibase")
	st.PerformTestBadBefore(t, local.Open, local.Path(dir))
}

func TestSinking(t *testing.T) {
	dir, _ := ioutil.TempDir(os.TempDir(), "minibase")
	st.PerformSinking(t, local.Open, local.Path(dir))
}

func testEqualEvents(t *testing.T, list1 message.List, list2 message.List) {
	as := assert.New(t)

	as.Equal(len(list1), len(list2))
	for i := 0; i < len(list1); i++ {
		e1, e2 := list1[i], list2[i]
		as.Equal(e1.ID, e2.ID)
		as.Equal(e1.Type, e2.Type)
		as.Equal(e1.CreatedAt, e2.CreatedAt)
	}
}
