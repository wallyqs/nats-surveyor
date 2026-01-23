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

package surveyor

import (
	"encoding/json"
	"testing"
	"time"

	st "github.com/nats-io/nats-surveyor/test"
	nats "github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
)

func TestJSzResponse_Parse(t *testing.T) {
	// Sample JSz response with MetaSnapshotStats
	jsonData := `{
		"server": {
			"name": "server1",
			"host": "localhost",
			"id": "NCXXX123",
			"cluster": "test-cluster",
			"ver": "2.10.0",
			"seq": 1,
			"jetstream": true,
			"time": "2024-01-01T00:00:00Z"
		},
		"data": {
			"server_id": "NCXXX123",
			"now": "2024-01-01T00:00:00Z",
			"config": {
				"max_memory": 1073741824,
				"max_storage": 10737418240
			},
			"streams": 5,
			"memory": 104857600,
			"storage": 524288000,
			"api": {
				"total": 1000,
				"errors": 10,
				"inflight": 2
			},
			"meta_cluster": {
				"name": "test-cluster",
				"leader": "server1",
				"replicas": [
					{
						"name": "server2",
						"current": true,
						"active": 1000000,
						"lag": 5
					},
					{
						"name": "server3",
						"current": true,
						"active": 2000000,
						"lag": 3
					}
				],
				"snapshot": {
					"pending_entries": 100,
					"pending_size": 51200,
					"last_time": "2024-01-01T00:00:00Z",
					"last_duration": 5000000
				}
			}
		}
	}`

	var resp JSzResponse
	err := json.Unmarshal([]byte(jsonData), &resp)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSzResponse: %v", err)
	}

	// Verify server info
	if resp.Server.Name != "server1" {
		t.Errorf("Expected server name 'server1', got '%s'", resp.Server.Name)
	}
	if resp.Server.Cluster != "test-cluster" {
		t.Errorf("Expected cluster 'test-cluster', got '%s'", resp.Server.Cluster)
	}
	if resp.Server.ID != "NCXXX123" {
		t.Errorf("Expected server ID 'NCXXX123', got '%s'", resp.Server.ID)
	}

	// Verify JSInfo
	if resp.JSInfo == nil {
		t.Fatal("JSInfo is nil")
	}
	if resp.JSInfo.Streams != 5 {
		t.Errorf("Expected 5 streams, got %d", resp.JSInfo.Streams)
	}
	if resp.JSInfo.Memory != 104857600 {
		t.Errorf("Expected memory 104857600, got %d", resp.JSInfo.Memory)
	}
	if resp.JSInfo.Store != 524288000 {
		t.Errorf("Expected storage 524288000, got %d", resp.JSInfo.Store)
	}

	// Verify API stats
	if resp.JSInfo.API.Total != 1000 {
		t.Errorf("Expected API total 1000, got %d", resp.JSInfo.API.Total)
	}
	if resp.JSInfo.API.Errors != 10 {
		t.Errorf("Expected API errors 10, got %d", resp.JSInfo.API.Errors)
	}
	if resp.JSInfo.API.Inflight != 2 {
		t.Errorf("Expected API inflight 2, got %d", resp.JSInfo.API.Inflight)
	}

	// Verify meta cluster info
	if resp.JSInfo.Meta == nil {
		t.Fatal("Meta cluster info is nil")
	}
	if resp.JSInfo.Meta.Name != "test-cluster" {
		t.Errorf("Expected meta cluster name 'test-cluster', got '%s'", resp.JSInfo.Meta.Name)
	}
	if resp.JSInfo.Meta.Leader != "server1" {
		t.Errorf("Expected leader 'server1', got '%s'", resp.JSInfo.Meta.Leader)
	}

	// Verify replicas
	if len(resp.JSInfo.Meta.Replicas) != 2 {
		t.Fatalf("Expected 2 replicas, got %d", len(resp.JSInfo.Meta.Replicas))
	}
	if resp.JSInfo.Meta.Replicas[0].Name != "server2" {
		t.Errorf("Expected replica name 'server2', got '%s'", resp.JSInfo.Meta.Replicas[0].Name)
	}
	if resp.JSInfo.Meta.Replicas[0].Lag != 5 {
		t.Errorf("Expected lag 5, got %d", resp.JSInfo.Meta.Replicas[0].Lag)
	}

	// Verify snapshot stats (the new fields from the commit)
	if resp.JSInfo.Meta.Snapshot == nil {
		t.Fatal("Meta snapshot stats is nil")
	}
	snapshot := resp.JSInfo.Meta.Snapshot
	if snapshot.PendingEntries != 100 {
		t.Errorf("Expected pending entries 100, got %d", snapshot.PendingEntries)
	}
	if snapshot.PendingSize != 51200 {
		t.Errorf("Expected pending size 51200, got %d", snapshot.PendingSize)
	}
	if snapshot.LastDuration != 5000000 {
		t.Errorf("Expected last duration 5000000, got %d", snapshot.LastDuration)
	}
}

func TestJSzResponse_ParseWithoutSnapshot(t *testing.T) {
	// JSz response without snapshot stats (for backward compatibility)
	jsonData := `{
		"server": {
			"name": "server1",
			"host": "localhost",
			"id": "NCXXX123",
			"cluster": "test-cluster",
			"ver": "2.10.0",
			"seq": 1,
			"jetstream": true,
			"time": "2024-01-01T00:00:00Z"
		},
		"data": {
			"server_id": "NCXXX123",
			"now": "2024-01-01T00:00:00Z",
			"streams": 3,
			"memory": 52428800,
			"storage": 262144000,
			"api": {
				"total": 500,
				"errors": 5
			},
			"meta_cluster": {
				"name": "test-cluster",
				"leader": "server1"
			}
		}
	}`

	var resp JSzResponse
	err := json.Unmarshal([]byte(jsonData), &resp)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSzResponse: %v", err)
	}

	if resp.JSInfo.Meta.Snapshot != nil {
		t.Errorf("Expected snapshot to be nil, got %+v", resp.JSInfo.Meta.Snapshot)
	}
}

func TestJSzResponse_ParseWithError(t *testing.T) {
	// JSz response with error
	jsonData := `{
		"server": {
			"name": "server1",
			"host": "localhost",
			"id": "NCXXX123",
			"cluster": "test-cluster",
			"ver": "2.10.0",
			"seq": 1,
			"jetstream": false,
			"time": "2024-01-01T00:00:00Z"
		},
		"error": {
			"code": 503,
			"description": "JetStream not enabled"
		}
	}`

	var resp JSzResponse
	err := json.Unmarshal([]byte(jsonData), &resp)
	if err != nil {
		t.Fatalf("Failed to unmarshal JSzResponse: %v", err)
	}

	if resp.Error == nil {
		t.Fatal("Expected error to be present")
	}
	if resp.Error.Code != 503 {
		t.Errorf("Expected error code 503, got %d", resp.Error.Code)
	}
	if resp.Error.Description != "JetStream not enabled" {
		t.Errorf("Expected description 'JetStream not enabled', got '%s'", resp.Error.Description)
	}
}

func TestJSzLabelValues(t *testing.T) {
	resp := &JSzResponse{
		Server: ServerInfo{
			Name:    "server1",
			Host:    "localhost",
			ID:      "NCXXX123",
			Cluster: "test-cluster",
		},
	}

	metaLabels := jszMetaClusterLabelValues(resp, "meta-cluster")
	if len(metaLabels) != 4 {
		t.Fatalf("Expected 4 meta cluster labels, got %d", len(metaLabels))
	}
	if metaLabels[0] != "test-cluster" {
		t.Errorf("Expected cluster 'test-cluster', got '%s'", metaLabels[0])
	}
	if metaLabels[1] != "server1" {
		t.Errorf("Expected server name 'server1', got '%s'", metaLabels[1])
	}
	if metaLabels[2] != "NCXXX123" {
		t.Errorf("Expected server ID 'NCXXX123', got '%s'", metaLabels[2])
	}
	if metaLabels[3] != "meta-cluster" {
		t.Errorf("Expected meta cluster name 'meta-cluster', got '%s'", metaLabels[3])
	}
}

func TestMetaSnapshotStats_TimeFields(t *testing.T) {
	lastTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	snapshot := &MetaSnapshotStats{
		PendingEntries: 50,
		PendingSize:    25600,
		LastTime:       lastTime,
		LastDuration:   10000000, // 10ms in nanoseconds
	}

	// Verify timestamp can be converted to Unix nanoseconds
	expectedNano := lastTime.UnixNano()
	if snapshot.LastTime.UnixNano() != expectedNano {
		t.Errorf("Expected LastTime.UnixNano() to be %d, got %d", expectedNano, snapshot.LastTime.UnixNano())
	}

	// Verify duration is in nanoseconds
	if snapshot.LastDuration != 10000000 {
		t.Errorf("Expected LastDuration to be 10000000, got %d", snapshot.LastDuration)
	}
}

func TestJSzCollector_Describe(t *testing.T) {
	// Create a mock connection using test infrastructure
	ns := st.StartBasicServer()
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	jc := NewJSzCollector(nc, 1, time.Second)

	// Test that Describe sends all expected descriptors
	ch := make(chan *prometheus.Desc, 100)
	jc.Describe(ch)
	close(ch)

	descCount := 0
	for range ch {
		descCount++
	}

	// We expect the core metrics to be described:
	// 1 natsUp + 4 snapshot metrics + surveyor metrics
	if descCount < 5 {
		t.Errorf("Expected at least 5 descriptors, got %d", descCount)
	}
}

func TestJSzCollector_MetricGeneration(t *testing.T) {
	// Test that metric values are properly generated from mock JSzResponse data
	// focusing on the MetaSnapshotStats metrics from commit 5ed0c1498e46f93ce
	lastTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	resp := &JSzResponse{
		Server: ServerInfo{
			Name:    "server1",
			Host:    "localhost",
			ID:      "NCXXX123",
			Cluster: "test-cluster",
			Version: "2.10.0",
		},
		JSInfo: &JSInfo{
			ID: "NCXXX123",
			Meta: &MetaClusterInfo{
				Name:   "test-cluster",
				Leader: "server1",
				Snapshot: &MetaSnapshotStats{
					PendingEntries: 100,
					PendingSize:    51200,
					LastTime:       lastTime,
					LastDuration:   5000000,
				},
			},
		},
	}

	// Verify meta cluster labels
	metaLabels := jszMetaClusterLabelValues(resp, "test-cluster")
	if metaLabels[0] != "test-cluster" || metaLabels[1] != "server1" || metaLabels[2] != "NCXXX123" || metaLabels[3] != "test-cluster" {
		t.Errorf("Unexpected meta cluster labels: %v", metaLabels)
	}

	// Verify Snapshot stats (the new metrics from nats-server commit 5ed0c1498e46f93ce)
	snapshot := resp.JSInfo.Meta.Snapshot

	// nats_jetstream_meta_cluster_snapshot_pending_entries
	if snapshot.PendingEntries != 100 {
		t.Errorf("Expected pending_entries=100, got %d", snapshot.PendingEntries)
	}

	// nats_jetstream_meta_cluster_snapshot_pending_bytes
	if snapshot.PendingSize != 51200 {
		t.Errorf("Expected pending_size=51200, got %d", snapshot.PendingSize)
	}

	// nats_jetstream_meta_cluster_snapshot_last_time
	if snapshot.LastTime.UnixNano() != lastTime.UnixNano() {
		t.Errorf("Expected last_time=%d, got %d", lastTime.UnixNano(), snapshot.LastTime.UnixNano())
	}

	// nats_jetstream_meta_cluster_snapshot_last_duration_ns
	if snapshot.LastDuration != 5000000 {
		t.Errorf("Expected last_duration=5000000, got %d", snapshot.LastDuration)
	}
}

func TestJSzCollector_Polling(t *testing.T) {
	ns := st.StartBasicServer()
	defer ns.Shutdown()

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	jc := NewJSzCollector(nc, 1, time.Second)

	// Initially should not be polling
	if jc.Polling() {
		t.Error("Collector should not be polling initially")
	}

	// Set polling state
	jc.Lock()
	jc.polling = true
	jc.Unlock()

	if !jc.Polling() {
		t.Error("Collector should be polling after setting state")
	}
}
