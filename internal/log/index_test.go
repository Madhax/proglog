package log

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	//f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	c := Config{}
	c.Segment.MaxIndexBytes = 24
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}
	fmt.Fprintln(os.Stdout, "idx.size", idx.size)
	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}
	fmt.Fprintln(os.Stdout, "idx.size", idx.size)
	// index and scanner should error when reading past existing entries

	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()
	// index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	fmt.Fprintln(os.Stdout, "idx.size", idx.size)

	require.NoError(t, err)

	// First entry
	off, pos, err := idx.Read(0)
	require.NoError(t, err)
	require.Equal(t, uint32(0), off)
	require.Equal(t, entries[0].Pos, pos)

	// Second entry
	off, pos, err = idx.Read(1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)

	// Last entry
	off, pos, err = idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
