package detector

import (
	"errors"
	"testing"
	"time"
)

type hare struct {
	sleep time.Duration
	url   string
	err   error
}

func (h *hare) Detect() error {
	if h.err != nil {
		return h.err
	}
	time.Sleep(h.sleep)
	return nil
}

func (h *hare) URL() string {
	return h.url
}

var (
	fastest  = &hare{sleep: 10 * time.Millisecond, url: "fastest"}
	ordinary = &hare{sleep: 50 * time.Millisecond, url: "ordinary"}
	slowest  = &hare{sleep: 100 * time.Millisecond, url: "slowest"}
	mis      = &hare{err: errors.New("oops"), url: "mis"}
)

func newDetector() Detector {
	detector := NewSimpleDetector()
	detector.Add(mis, ordinary, fastest, slowest)
	return detector
}

func TestDetectorUndected(t *testing.T) {
	detector := newDetector()
	if picked, err := detector.PickFastest(); err != nil {
		t.Fatal("pick failed")
	} else if mis != picked {
		t.Fatal("order without detect is wrong")
	}
}

func TestDetectorDetected(t *testing.T) {
	detector := newDetector()
	detector.DetectAll()
	if picked, err := detector.PickFastest(); err != nil {
		t.Fatal("pick failed")
	} else if picked != fastest {
		t.Fatal("fastest shall be returned")
	}
}

func TestDetectorFailover(t *testing.T) {
	detector := newDetector()
	detector.DetectAll()

	detector.Failover()

	if picked, err := detector.PickFastest(); err != nil {
		t.Fatal("pick failed")
	} else if picked != ordinary {
		t.Fatal("ordinary shall be returned")
	}

	detector.Failover()
	if picked, err := detector.PickFastest(); err != nil {
		t.Fatal("pick failed")
	} else if picked != slowest {
		t.Fatalf("slowest shall be returned: %s", picked)
	}

	detector.Failover()

	if picked, err := detector.PickFastest(); err != nil {
		t.Fatal("pick failed")
	} else if picked != mis {
		t.Fatal("error shall be returned")
	}
}

func TestPickNth(t *testing.T) {
	detector := newDetector()
	detector.DetectAll()

	if picked, err := detector.PickNth(1); err != nil {
		t.Fatal("pick failed")
	} else if picked != ordinary {
		t.Fatal("ordinary shall be returned")
	}

	if picked, err := detector.PickNth(2); err != nil {
		t.Fatal("pick failed")
	} else if picked != slowest {
		t.Fatalf("slowest shall be returned: %v", picked)
	}

	if picked, err := detector.PickNth(3); err != nil {
		t.Fatal("pick failed")
	} else if picked != mis {
		t.Fatal("error shall be returned")
	}
}

func TestWithRetryNeverSuccess(t *testing.T) {
	detector := newDetector()
	detector.DetectAll()

	urls := make([]string, 0, 4)

	expected_err := errors.New("network error")

	err := detector.WithRetry(4, func(node Node) error {
		urls = append(urls, node.URL())
		return expected_err
	})

	if len(urls) != 4 {
		t.Fatalf("expected 4 urls but got %d", len(urls))
	}

	if urls[0] != "fastest" || urls[1] != "ordinary" || urls[2] != "slowest" || urls[3] != "mis" {
		t.Fatal("the retry order is wrong")
	}
	if err != expected_err {
		t.Fatalf("returned error is not expected, %v returned", err)
	}

	// Now we shall start over.
	urls = make([]string, 0, 4)

	err = detector.WithRetry(4, func(node Node) error {
		urls = append(urls, node.URL())
		return expected_err
	})

	if urls[0] != "fastest" || urls[1] != "ordinary" || urls[2] != "slowest" || urls[3] != "mis" {
		t.Fatal("didn't start over")
	}
	if err != expected_err {
		t.Fail()
	}

	// We only retry on the top 3 nodes.
	urls = make([]string, 0, 3)

	err = detector.WithRetry(3, func(node Node) error {
		urls = append(urls, node.URL())
		return expected_err
	})
	if len(urls) != 3 {
		t.Fail()
	}

	if urls[0] != "fastest" || urls[1] != "ordinary" || urls[2] != "slowest" {
		t.Fatal("didn't start over")
	}
	if err != expected_err {
		t.Fail()
	}
}

func TestWithRetryOrdinarySuccess(t *testing.T) {
	detector := newDetector()
	detector.DetectAll()

	urls := make([]string, 0, 4)

	err := detector.WithRetry(4, func(node Node) error {
		urls = append(urls, node.URL())
		if node.URL() == "ordinary" {
			return nil
		}
		return errors.New("network error")
	})

	if err != nil {
		t.Fatalf("expected no error but %v has returned", err)

	}
	if len(urls) != 2 {
		t.Fatal("too may attempations")
	}

	if urls[0] != "fastest" || urls[1] != "ordinary" {
		t.Fatal("attempations is not in order")
	}

	// Then always use ordinary unless another dectect or this one failed.
	urls = make([]string, 0, 4)

	err = detector.WithRetry(4, func(node Node) error {
		urls = append(urls, node.URL())
		return nil
	})
	if err != nil || len(urls) != 1 || urls[0] != "ordinary" {
		t.Fatal("try too many")
	}

	// Now we are using the fastest.
	detector.DetectAll()
	urls = make([]string, 0, 4)

	err = detector.WithRetry(4, func(node Node) error {
		urls = append(urls, node.URL())
		return nil
	})
	if err != nil || len(urls) != 1 || urls[0] != "fastest" {
		t.Fail()
	}
}

func TestRetryWithoutNodes(t *testing.T) {
	detector := NewSimpleDetector()
	err := detector.WithRetry(4, func(Node) error {
		return nil
	})
	if err != ErrRingEmpty {
		t.Fail()
	}
}

func TestWithRetryFailover(t *testing.T) {
	detector := newDetector()
	detector.DetectAll()

	urls := make([]string, 0, 4)

	expected_err := errors.New("network error")

	err := detector.WithRetry(3, func(node Node) error {
		urls = append(urls, node.URL())
		return expected_err
	})

	if len(urls) != 3 {
		t.Fatalf("expected 3 urls but got %d", len(urls))
	}

	if urls[0] != "fastest" || urls[1] != "ordinary" || urls[2] != "slowest" {
		t.Fatal("the retry order is wrong")
	}
	if err != expected_err {
		t.Fatalf("returned error is not expected, %v returned", err)
	}

	// Next 3 times we'll start over.
	err = detector.WithRetry(3, func(node Node) error {
		urls = append(urls, node.URL())
		return expected_err
	})

	if len(urls) != 6 {
		t.Fail()
	}

	if urls[0] != "fastest" || urls[1] != "ordinary" || urls[2] != "slowest" || urls[3] != "mis" || urls[4] != "fastest" || urls[5] != "ordinary" {
		t.Fatal("the retry order is wrong")
	}
}
