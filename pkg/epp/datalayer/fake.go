/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datalayer

import (
	"context"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/types"
	fwkdl "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/datalayer"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/epp/framework/interface/plugin"
)

const (
	fakeSource = "fake-data-source"
)

type FakeDataSource struct {
	typedName *plugin.TypedName
	callCount int64
	mu        sync.RWMutex
	Metrics   map[types.NamespacedName]*fwkdl.Metrics
	Errors    map[types.NamespacedName]error
}

func (fds *FakeDataSource) TypedName() plugin.TypedName {
	if fds.typedName != nil {
		return *fds.typedName
	}
	return plugin.TypedName{
		Type: fakeSource,
		Name: fakeSource,
	}
}
func (fds *FakeDataSource) Extractors() []string                 { return []string{} }
func (fds *FakeDataSource) AddExtractor(_ fwkdl.Extractor) error { return nil }

func (fds *FakeDataSource) Collect(ctx context.Context, ep fwkdl.Endpoint) error {
	atomic.AddInt64(&fds.callCount, 1)
	nn := ep.GetMetadata().Clone().NamespacedName

	fds.mu.RLock()
	epErr, hasErr := fds.Errors[nn]
	fds.mu.RUnlock()

	if hasErr {
		return epErr
	}
	if metrics, ok := fds.Metrics[nn]; ok {
		ep.UpdateMetrics(metrics)
	}
	return nil
}

// SetErrors sets the error map for the fake data source (thread-safe).
func (fds *FakeDataSource) SetErrors(errors map[types.NamespacedName]error) {
	fds.mu.Lock()
	defer fds.mu.Unlock()
	fds.Errors = errors
}
