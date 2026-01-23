// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package surveyor is used to garner data from a NATS deployment for Prometheus
package surveyor

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
)

// MetaSnapshotStats contains information about the meta cluster snapshot state
type MetaSnapshotStats struct {
	PendingEntries int       `json:"pending_entries"`
	PendingSize    uint64    `json:"pending_size"`
	LastTime       time.Time `json:"last_time"`
	LastDuration   int64     `json:"last_duration"`
}

// MetaClusterInfo contains information about the JetStream meta cluster
type MetaClusterInfo struct {
	Name     string             `json:"name,omitempty"`
	Leader   string             `json:"leader,omitempty"`
	Replicas []*PeerInfo        `json:"replicas,omitempty"`
	Snapshot *MetaSnapshotStats `json:"snapshot,omitempty"`
}

// PeerInfo contains information about a peer in the cluster
type PeerInfo struct {
	Name    string        `json:"name"`
	Current bool          `json:"current"`
	Offline bool          `json:"offline,omitempty"`
	Active  time.Duration `json:"active"`
	Lag     uint64        `json:"lag,omitempty"`
}

// JSzOptions contains options for JSz requests
type JSzOptions struct {
	Account    string `json:"account,omitempty"`
	Streams    bool   `json:"streams,omitempty"`
	Consumers  bool   `json:"consumers,omitempty"`
	Config     bool   `json:"config,omitempty"`
	Leader     bool   `json:"leader,omitempty"`
	Offset     int    `json:"offset,omitempty"`
	Limit      int    `json:"limit,omitempty"`
	RaftGroups bool   `json:"raft,omitempty"`
}

// JSInfo contains JetStream information from a server
type JSInfo struct {
	ID       string           `json:"server_id"`
	Now      time.Time        `json:"now"`
	Disabled bool             `json:"disabled,omitempty"`
	Config   JetStreamConfig  `json:"config,omitempty"`
	Streams  int              `json:"streams"`
	Memory   uint64           `json:"memory"`
	Store    uint64           `json:"storage"`
	API      JetStreamAPIStats `json:"api"`
	Meta     *MetaClusterInfo `json:"meta_cluster,omitempty"`
}

// JetStreamConfig contains JetStream configuration
type JetStreamConfig struct {
	MaxMemory    int64  `json:"max_memory"`
	MaxStore     int64  `json:"max_storage"`
	StoreDir     string `json:"store_dir,omitempty"`
	Domain       string `json:"domain,omitempty"`
	CompressOK   bool   `json:"compress_ok,omitempty"`
	UniqueTag    string `json:"unique_tag,omitempty"`
	SyncInterval int64  `json:"sync_interval,omitempty"`
}

// JetStreamAPIStats contains API statistics
type JetStreamAPIStats struct {
	Total    uint64 `json:"total"`
	Errors   uint64 `json:"errors"`
	Inflight uint64 `json:"inflight,omitempty"`
}

// ServerInfo identifies a server
type ServerInfo struct {
	Name      string    `json:"name"`
	Host      string    `json:"host"`
	ID        string    `json:"id"`
	Cluster   string    `json:"cluster,omitempty"`
	Domain    string    `json:"domain,omitempty"`
	Version   string    `json:"ver"`
	Tags      []string  `json:"tags,omitempty"`
	Seq       uint64    `json:"seq"`
	JetStream bool      `json:"jetstream"`
	Time      time.Time `json:"time"`
}

// JSzResponse is the response from a JSz request
type JSzResponse struct {
	Server ServerInfo `json:"server"`
	JSInfo *JSInfo    `json:"data,omitempty"`
	Error  *ApiError  `json:"error,omitempty"`
}

// ApiError represents an API error
type ApiError struct {
	Code        int    `json:"code"`
	ErrCode     uint16 `json:"err_code,omitempty"`
	Description string `json:"description,omitempty"`
}

// jszDescs holds the JetStream metric descriptions
type jszDescs struct {
	// Meta Cluster Snapshot Stats (from nats-server commit 5ed0c1498e46f93ce)
	MetaClusterSnapshotPendingEntries *prometheus.Desc
	MetaClusterSnapshotPendingBytes   *prometheus.Desc
	MetaClusterSnapshotLastTime       *prometheus.Desc
	MetaClusterSnapshotLastDuration   *prometheus.Desc
}

// JSzCollector collects JetStream information from a NATS deployment
type JSzCollector struct {
	sync.Mutex
	nc          *nats.Conn
	start       time.Time
	stats       []*JSzResponse
	pollTimeout time.Duration
	reply       string
	polling     bool
	pollkey     string
	numServers  int
	servers     map[string]bool
	doneCh      chan struct{}
	descs       jszDescs
	natsUp      *prometheus.Desc

	surveyedCnt *prometheus.GaugeVec
	expectedCnt *prometheus.GaugeVec
	pollErrCnt  *prometheus.CounterVec
	pollTime    *prometheus.SummaryVec
}

var (
	jszMetaClusterLabels = []string{"server_cluster", "server_name", "server_id", "meta_cluster_name"}
)

func jszMetaClusterLabelValues(resp *JSzResponse, metaClusterName string) []string {
	return []string{resp.Server.Cluster, resp.Server.Name, resp.Server.ID, metaClusterName}
}

func buildJSzDescs(jc *JSzCollector) {
	newPromDesc := func(name, help string, labels []string) *prometheus.Desc {
		return prometheus.NewDesc(
			prometheus.BuildFQName("nats", "jetstream", name), help, labels, nil)
	}

	// A unlabelled description for the up/down
	jc.natsUp = prometheus.NewDesc(prometheus.BuildFQName("nats", "jetstream", "nats_up"),
		"1 if connected to NATS, 0 otherwise. A gauge.", nil, nil)

	// Meta Cluster Snapshot Stats (the new metrics from nats-server commit 5ed0c1498e46f93ce)
	jc.descs.MetaClusterSnapshotPendingEntries = newPromDesc("meta_cluster_snapshot_pending_entries", "Number of pending entries awaiting meta cluster snapshot", jszMetaClusterLabels)
	jc.descs.MetaClusterSnapshotPendingBytes = newPromDesc("meta_cluster_snapshot_pending_bytes", "Size in bytes of pending entries awaiting meta cluster snapshot", jszMetaClusterLabels)
	jc.descs.MetaClusterSnapshotLastTime = newPromDesc("meta_cluster_snapshot_last_time", "Unix timestamp of the last meta cluster snapshot", jszMetaClusterLabels)
	jc.descs.MetaClusterSnapshotLastDuration = newPromDesc("meta_cluster_snapshot_last_duration_ns", "Duration of the last meta cluster snapshot in nanoseconds", jszMetaClusterLabels)

	// Surveyor
	jc.surveyedCnt = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("nats", "jetstream_survey", "surveyed_count"),
		Help: "Number of JetStream-enabled servers successfully surveyed",
	}, []string{})

	jc.expectedCnt = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: prometheus.BuildFQName("nats", "jetstream_survey", "expected_count"),
		Help: "Number of JetStream-enabled servers expected to respond",
	}, []string{})

	jc.pollTime = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name: prometheus.BuildFQName("nats", "jetstream_survey", "duration_seconds"),
		Help: "Time it took to gather the JetStream surveyed data",
	}, []string{})

	jc.pollErrCnt = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: prometheus.BuildFQName("nats", "jetstream_survey", "poll_error_count"),
		Help: "The number of times the JetStream poller encountered errors",
	}, []string{})
}

// NewJSzCollector creates a new JetStream Collector
func NewJSzCollector(nc *nats.Conn, numServers int, pollTimeout time.Duration) *JSzCollector {
	jc := &JSzCollector{
		nc:          nc,
		numServers:  numServers,
		reply:       nc.NewRespInbox(),
		pollTimeout: pollTimeout,
		servers:     make(map[string]bool, numServers),
		doneCh:      make(chan struct{}, 1),
	}
	buildJSzDescs(jc)

	jc.expectedCnt.WithLabelValues().Set(float64(numServers))

	nc.Subscribe(jc.reply+".*", jc.handleResponse)
	return jc
}

// Polling determines if the collector is in a polling cycle
func (jc *JSzCollector) Polling() bool {
	jc.Lock()
	defer jc.Unlock()
	return jc.polling
}

func (jc *JSzCollector) handleResponse(msg *nats.Msg) {
	resp := &JSzResponse{}
	if err := json.Unmarshal(msg.Data, resp); err != nil {
		log.Printf("Error unmarshalling jsz json: %v", err)
		return
	}

	jc.Lock()
	isCurrent := strings.HasSuffix(msg.Subject, jc.pollkey)
	if jc.polling && isCurrent {
		jc.stats = append(jc.stats, resp)
		if len(jc.stats) == jc.numServers {
			jc.polling = false
			jc.doneCh <- struct{}{}
		}
	}
	jc.Unlock()
}

// poll will only fail if there is a NATS publishing error
func (jc *JSzCollector) poll() error {
	jc.Lock()
	jc.start = time.Now()
	jc.polling = true
	jc.pollkey = strconv.Itoa(int(jc.start.UnixNano()))
	jc.stats = nil
	jc.Unlock()

	defer func() {
		jc.Lock()
		jc.polling = false
		jc.Unlock()
	}()

	if !jc.nc.IsConnected() {
		return fmt.Errorf("no connection to NATS")
	}

	// Request JSz from all servers
	opts := JSzOptions{Leader: true}
	reqData, err := json.Marshal(opts)
	if err != nil {
		return err
	}

	if err := jc.nc.PublishRequest("$SYS.REQ.SERVER.PING.JSZ", jc.reply+"."+jc.pollkey, reqData); err != nil {
		return err
	}

	select {
	case <-jc.doneCh:
	case <-time.After(jc.pollTimeout):
		log.Printf("JSz poll timeout after %v while waiting for %d responses\n", jc.pollTimeout, jc.numServers)
	}

	jc.Lock()
	jc.polling = false
	ns := len(jc.stats)
	jc.Unlock()

	if ns != jc.numServers {
		log.Printf("JSz expected %d servers, only saw responses from %d", jc.numServers, ns)
	}

	return nil
}

// Describe is the Prometheus interface to describe metrics
func (jc *JSzCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- jc.natsUp
	ch <- jc.descs.MetaClusterSnapshotPendingEntries
	ch <- jc.descs.MetaClusterSnapshotPendingBytes
	ch <- jc.descs.MetaClusterSnapshotLastTime
	ch <- jc.descs.MetaClusterSnapshotLastDuration

	jc.surveyedCnt.Describe(ch)
	jc.expectedCnt.Describe(ch)
	jc.pollErrCnt.Describe(ch)
	jc.pollTime.Describe(ch)
}

func newJSzGaugeMetric(desc *prometheus.Desc, value float64, labels []string) prometheus.Metric {
	return prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, value, labels...)
}

func (jc *JSzCollector) newNatsUpGaugeMetric(value bool) prometheus.Metric {
	var fval float64
	if value {
		fval = 1
	}
	return prometheus.MustNewConstMetric(jc.natsUp, prometheus.GaugeValue, fval)
}

// Collect gathers the JetStream server metrics
func (jc *JSzCollector) Collect(ch chan<- prometheus.Metric) {
	timer := prometheus.NewTimer(jc.pollTime.WithLabelValues())
	defer func() {
		timer.ObserveDuration()
		jc.pollTime.Collect(ch)
		jc.pollErrCnt.Collect(ch)
		jc.surveyedCnt.Collect(ch)
		jc.expectedCnt.Collect(ch)
	}()

	if err := jc.poll(); err != nil {
		log.Printf("Error polling NATS server for JSz: %v", err)
		jc.pollErrCnt.WithLabelValues().Inc()
		ch <- jc.newNatsUpGaugeMetric(false)
		return
	}

	jc.Lock()
	defer jc.Unlock()

	ch <- jc.newNatsUpGaugeMetric(true)
	jc.surveyedCnt.WithLabelValues().Set(0)

	for _, resp := range jc.stats {
		if resp.Error != nil {
			log.Printf("JSz error from server %s: %s", resp.Server.Name, resp.Error.Description)
			continue
		}

		if resp.JSInfo == nil {
			continue
		}

		jc.surveyedCnt.WithLabelValues().Inc()

		// Meta cluster snapshot stats (the new metrics from nats-server commit 5ed0c1498e46f93ce)
		if resp.JSInfo.Meta != nil && resp.JSInfo.Meta.Snapshot != nil {
			metaClusterName := resp.JSInfo.Meta.Name
			if metaClusterName == "" {
				metaClusterName = "unknown"
			}

			metaLabels := jszMetaClusterLabelValues(resp, metaClusterName)
			snapshot := resp.JSInfo.Meta.Snapshot

			ch <- newJSzGaugeMetric(jc.descs.MetaClusterSnapshotPendingEntries, float64(snapshot.PendingEntries), metaLabels)
			ch <- newJSzGaugeMetric(jc.descs.MetaClusterSnapshotPendingBytes, float64(snapshot.PendingSize), metaLabels)
			ch <- newJSzGaugeMetric(jc.descs.MetaClusterSnapshotLastTime, float64(snapshot.LastTime.UnixNano()), metaLabels)
			ch <- newJSzGaugeMetric(jc.descs.MetaClusterSnapshotLastDuration, float64(snapshot.LastDuration), metaLabels)
		}
	}
}
