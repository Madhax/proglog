package log

import "testing"

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, log *Log,
	){
		"append and read a record succeeds": testAppendRead,
		"offset out of range error":         testOutOfRangeErr,
		"init with existing segments":       testInitExisting,
		"reader:":                           testReader,
		"truncate":                          testTruncate,
	} {
		t.Run(scenario, func(t *testing.T)) {
			dir, err := ioutil.TempDir("", "store-test")
			require.noError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		}
	}
}
