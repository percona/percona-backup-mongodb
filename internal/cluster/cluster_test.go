package cluster

import "testing"

func TestParseShardURI(t *testing.T) {
	uri := "rs/127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019"
	rs, hosts := parseShardURI(uri)
	if rs == "" {
		t.Fatal("Got empty replset name from .parseShardURI()")
	} else if len(hosts) != 3 {
		t.Fatalf("Expected %d hosts but got %d from .parseShardURI()", 3, len(hosts))
	}

	// too many '/'
	rs, hosts = parseShardURI("rs///")
	if rs != "" || len(hosts) > 0 {
		t.Fatal("Expected empty results from .parseShardURI()")
	}

	// missing replset
	rs, hosts = parseShardURI("127.0.0.1:27017")
	if rs != "" || len(hosts) > 0 {
		t.Fatal("Expected empty results from .parseShardURI()")
	}
}
