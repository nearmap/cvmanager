// Package datadog provides stats logging using datadog.
package datadog

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/golang/glog"
)

// Stats is statsd client wrapper
type Stats struct {
	sync.Mutex

	client *statsd.Client
}

// New returns a DataDog stats instance that implements the Stats interface and sends
// data to the provided datadog address.
func New(address, namespace string, tags ...string) (*Stats, error) {
	if address == "" {
		return &Stats{}, fmt.Errorf("no address provided for stats")
	}
	address = addDefaultPort(address)

	if !strings.HasSuffix(namespace, ".") {
		namespace = namespace + "."
	}

	glog.V(1).Infof("Sending stats to %s with namespace %s", address, namespace)

	client, err := statsd.NewBuffered(address, 1432)
	if err != nil {
		return &Stats{}, fmt.Errorf("failed to connect to statsd port at %s: %v", address, err)
	}
	client.Namespace = namespace
	client.Tags = tags

	return &Stats{
		client: client,
	}, nil
}

// Close closes statsd client
func (s *Stats) Close() error {
	return s.client.Close()
}

// IncCount is used to capture stats are are continuous and increment it by 1
func (s *Stats) IncCount(name string, tags ...string) {
	glog.V(4).Infof("sending %s metric of tag %s", name, tags)
	err := s.client.Incr(name, tags, 1.0)
	if err != nil {
		glog.Errorf("Error sending %s metric: %+v", name, err)
	}
}

// IncCountBy decrement stats by specified value
func (s *Stats) IncCountBy(name string, value int64, tags ...string) {
	err := s.client.Count(name, value, tags, 1.0)
	if err != nil {
		glog.Errorf("Error sending %s metric: %+v", name, err)
	}
}

// DecCount decrement stats by 1
func (s *Stats) DecCount(name string, tags ...string) {
	err := s.client.Decr(name, tags, 1.0)
	if err != nil {
		glog.Errorf("Error sending %s metric: %+v", name, err)
	}
}

// DecCountBy decrement stats by specified value
func (s *Stats) DecCountBy(name string, value int64, tags ...string) {
	err := s.client.Count(name, -1*value, tags, 1.0)
	if err != nil {
		glog.Errorf("Error sending %s metric: %+v", name, err)
	}
}

// ElapsedTime captures the timing based on start time and current time
func (s *Stats) ElapsedTime(start time.Time, name string, tags ...string) {
	glog.V(4).Infof("sending %s metric of tag %s", name, tags)
	elapsed := time.Since(start)
	s.Duration(elapsed, name, tags...)
}

// Duration captures the duration as timing stats
func (s *Stats) Duration(duration time.Duration, name string, tags ...string) {
	// time.Nanosecond / time.Milisecond below
	// gives us the ratio between the two constants, which we multiply by
	// duration in nanoseconds to convert to duration in miliseconds.
	err := s.client.Timing(name+".timing", duration, tags, 1.0)
	if err != nil {
		glog.Errorf("Error sending timing metric %s: %+v", name, err)
	}
}

// Gauge generates Gauge
func (s *Stats) Gauge(name string, value int64, tags ...string) {
	err := s.client.Gauge(name, float64(value), tags, 1.0)
	if err != nil {
		glog.Errorf("Error sending %s gauge: %+v", name, err)
	}
}

// Histogram generates Histogram
func (s *Stats) Histogram(name string, value int64, tags ...string) {
	err := s.client.Histogram(name, float64(value), tags, 1.0)
	if err != nil {
		glog.Errorf("Error sending histogram %s: %+v", name, err)
	}
}

// Event contains details of an activity that was captured and
// notifies it to stats backend
func (s *Stats) Event(title, mesg, aggKey, typ string, timestamp time.Time, tags ...string) {
	glog.V(4).Infof("sending %s event metric of value %s and tag %s", title, mesg, tags)
	event := &statsd.Event{
		Title:          fmt.Sprintf("%s.event", title),
		Text:           mesg,
		Timestamp:      timestamp,
		AggregationKey: aggKey,
		Tags:           tags,
	}
	switch typ {
	case "info":
		event.AlertType = statsd.Info
		break
	case "error":
		event.AlertType = statsd.Error
		break
	case "warning":
		event.AlertType = statsd.Warning
		break
	case "success":
		event.AlertType = statsd.Success
		break
	default:
		glog.Errorf("Event type %s is unrecognized. Failed to send event %s \n", typ, title)
		return
	}
	err := s.client.Event(event)
	if err != nil {
		glog.Errorf("Error sending event %s: %+v", title, err)
	}
}

// ServiceCheck notifies monitoring backend about the status of
// service (Ok/Warning/Critical/Unknow).
func (s *Stats) ServiceCheck(name, mesg string, status int, timestamp time.Time, tags ...string) {
	glog.V(4).Infof("sending %s service check status %d and tag %s", name, status, tags)
	sc := &statsd.ServiceCheck{
		Name:      fmt.Sprintf("%s.sc", name),
		Message:   mesg,
		Timestamp: timestamp,
		Tags:      tags,
	}
	switch status {
	case 0:
		sc.Status = statsd.Ok
		break
	case 1:
		sc.Status = statsd.Warn
		break
	case 2:
		sc.Status = statsd.Critical
		break
	case 4:
		sc.Status = statsd.Unknown
		break
	default:
		glog.Errorf("Service check status %v is unrecognized. Failed to send service check status %s \n", status, name)
		return
	}
	err := s.client.ServiceCheck(sc)
	if err != nil {
		glog.Errorf("Error sending event %s: %+v", name, err)
	}
}

// addDefaultPort adds default statsd port 8125 if port is not already specified
// to a given hostname
func addDefaultPort(host string) string {
	if h, p, err := net.SplitHostPort(host); err != nil {
		host = fmt.Sprintf("%s:8125", host)
	} else if p == "" {
		host = fmt.Sprintf("%s:8125", h)
	}
	return host
}
