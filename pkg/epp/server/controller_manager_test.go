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

package server

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	v1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/common"
)

func TestResolveGracefulShutdownTimeout(t *testing.T) {
	tests := []struct {
		name    string
		envVal  string
		envSet  bool
		want    *time.Duration
		wantErr bool
	}{
		{
			name:   "env var unset returns nil with no error",
			envSet: false,
			want:   nil,
		},
		{
			name:   "empty env var returns nil with no error",
			envSet: true,
			envVal: "",
			want:   nil,
		},
		{
			name:   "valid positive duration is parsed",
			envSet: true,
			envVal: "30m",
			want:   durPtr(30 * time.Minute),
		},
		{
			name:   "negative duration passes through (controller-runtime convention for wait-forever)",
			envSet: true,
			envVal: "-1s",
			want:   durPtr(-1 * time.Second),
		},
		{
			name:    "invalid duration returns an error",
			envSet:  true,
			envVal:  "garbage",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envSet {
				t.Setenv(gracefulShutdownTimeoutEnvVar, tt.envVal)
			} else {
				// Defensive: unset even if leaked from runner env.
				t.Setenv(gracefulShutdownTimeoutEnvVar, "")
			}

			got, err := resolveGracefulShutdownTimeout()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (got=%v)", got)
				}
				if got != nil {
					t.Errorf("expected nil pointer on error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch {
			case tt.want == nil && got != nil:
				t.Errorf("expected nil, got %v", got)
			case tt.want != nil && got == nil:
				t.Errorf("expected %v, got nil", *tt.want)
			case tt.want != nil && got != nil && *tt.want != *got:
				t.Errorf("expected %v, got %v", *tt.want, *got)
			}
		})
	}
}

func TestDefaultManagerOptions_GracefulShutdownTimeout(t *testing.T) {
	gknn := common.GKNN{
		NamespacedName: types.NamespacedName{Name: "p1", Namespace: "ns1"},
		GroupKind:      schema.GroupKind{Group: v1.GroupName, Kind: "InferencePool"},
	}

	t.Run("env set to valid duration injects the pointer", func(t *testing.T) {
		t.Setenv(gracefulShutdownTimeoutEnvVar, "7m")
		opts, err := defaultManagerOptions(ControllerConfig{}, gknn, metricsserver.Options{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.GracefulShutdownTimeout == nil {
			t.Fatal("expected GracefulShutdownTimeout to be set, got nil")
		}
		if *opts.GracefulShutdownTimeout != 7*time.Minute {
			t.Errorf("GracefulShutdownTimeout = %v, want %v", *opts.GracefulShutdownTimeout, 7*time.Minute)
		}
	})

	t.Run("env unset leaves GracefulShutdownTimeout nil (controller-runtime default)", func(t *testing.T) {
		t.Setenv(gracefulShutdownTimeoutEnvVar, "")
		opts, err := defaultManagerOptions(ControllerConfig{}, gknn, metricsserver.Options{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.GracefulShutdownTimeout != nil {
			t.Errorf("expected nil GracefulShutdownTimeout, got %v", *opts.GracefulShutdownTimeout)
		}
	})

	t.Run("invalid env returns an error from defaultManagerOptions", func(t *testing.T) {
		t.Setenv(gracefulShutdownTimeoutEnvVar, "garbage")
		_, err := defaultManagerOptions(ControllerConfig{}, gknn, metricsserver.Options{})
		if err == nil {
			t.Fatal("expected error from defaultManagerOptions, got nil")
		}
	})
}

func durPtr(d time.Duration) *time.Duration {
	return &d
}
