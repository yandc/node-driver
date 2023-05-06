package detector

import (
	"errors"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

type hare struct {
	height uint64
	sleep  time.Duration
	url    string
	err    error
}

func (h *hare) GetBlockHeight() (uint64, error) {
	height := atomic.LoadUint64(&h.height)
	if h.err != nil {
		return height, h.err
	}
	time.Sleep(h.sleep)
	return height, nil
}

func (h *hare) URL() string {
	return h.url
}

func (h *hare) Recover(interface{}) error {
	return nil
}

func (h *hare) RetryAfter() time.Time {
	return time.Now()
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

func startIncrHeight(h *hare, interval time.Duration) {
	go func() {
		tick := time.NewTicker(interval)
		for range tick.C {
			atomic.AddUint64(&h.height, 1)
		}
	}()
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
		t.Fatalf("fastest shall be returned: %v", picked)
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
		t.Fatalf("slowest shall be returned: %v", picked)
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

	for _, u := range urls {
		t.Log(u)
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
	for _, u := range urls {
		t.Log(u)
	}
	if err != expected_err {
		t.Fatalf("got unexpected error: %v", err)
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
		t.Fatalf("err: %v, urls len: %d, urls[0]: %s", err, len(urls), urls[0])
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

	if len(urls) < 5 {
		t.Fatalf("expected len: 6, actual: %d", len(urls))
	}

	if urls[0] != "fastest" || urls[1] != "ordinary" || urls[2] != "slowest" || urls[3] != "mis" || urls[4] != "fastest" {
		t.Fatal("the retry order is wrong")
	}
}

func TestHeightDeltaFactor(t *testing.T) {
	var (
		ifastest  = &hare{sleep: 10 * time.Millisecond, url: "fastest"}
		iordinary = &hare{sleep: 50 * time.Millisecond, url: "ordinary"}
		islowest  = &hare{sleep: 100 * time.Millisecond, url: "slowest"}
		imis      = &hare{err: errors.New("oops"), url: "mis"}
	)
	detector := NewSimpleDetector()
	detector.Add(imis, iordinary, ifastest, islowest)

	startIncrHeight(islowest, time.Second)
	startIncrHeight(ifastest, time.Millisecond*500)
	startIncrHeight(iordinary, time.Millisecond*800)
	time.Sleep(time.Second * 2)

	detector.DetectAll()

	go detector.StartDetectPlan(3*time.Second, 0, 3*time.Second)
	go detector.StartDetectPlan(10*time.Second, 3*time.Second, 10*time.Second)
	go detector.StartDetectPlan(20*time.Second, 10*time.Second, math.MaxInt64)

	go func() {
		for {
			time.Sleep(time.Second)
			detector.WithRetry(4, func(node Node) error {
				return nil
			})
			detector.Each(func(i int, node Node) (terminate bool) {
				height, _ := node.GetBlockHeight()
				println(node.URL(), height)
				return false
			})
		}
	}()

	if node, err := detector.PickFastest(); err != nil {
		t.Fatalf("unexpected error %v", err)
	} else if node != ifastest {
		t.Fatalf("got unexpected node: %s", node.URL())
	}

	// make the height increment of the islowest node faster
	startIncrHeight(islowest, time.Millisecond*250)
	time.Sleep(time.Second * 25)

	if node, err := detector.PickFastest(); err != nil {
		t.Fatalf("unexpected error %v", err)
	} else if node != islowest {
		t.Fatalf("got unexpected node: %s", node.URL())
	}
}

func TestGetHeightDeltaFactor(t *testing.T) {
	if v := heightDeltaFactor(-5); v != 0 {
		t.Fail()
	}
	if v := heightDeltaFactor(-10); v != -1 {
		t.Fail()
	}

	if v := heightDeltaFactor(5); v != 0 {
		t.Fail()
	}

	if v := heightDeltaFactor(10); v != 1 {
		t.Fail()
	}

	if v := heightDeltaFactor(22); v != 8 {
		t.Fail()
	}

	if v := heightDeltaFactor(100); v != 1000 {
		t.Fail()
	}
}
