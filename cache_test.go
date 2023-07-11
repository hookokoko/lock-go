package lock_go

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_New(t *testing.T) {
	c := New[string]()
	err := c.Set(context.Background(), "key1", "value1", 5*time.Second)
	assert.Nil(t, err)
	v, err := c.Get(context.Background(), "key1")
	assert.Nil(t, err)
	assert.Equal(t, "value1", v)

	_, err = c.Get(context.Background(), "keyxxxx")
	assert.Equal(t, errors.New("key not exist"), err)

	time.Sleep(5 * time.Second)
	vvv, err := c.Get(context.Background(), "key1")
	assert.Equal(t, "", vvv)
	assert.Equal(t, errors.New("key has expiated"), err)

	c.Close()
}
