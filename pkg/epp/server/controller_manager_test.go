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
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	v1 "sigs.k8s.io/gateway-api-inference-extension/api/v1"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/common"
)

// envMode distinguishes "env var entirely absent from the process environment"
// from "env var set to the empty string" — a distinction t.Setenv alone cannot
// express. Both currently map to the same behavior in
// resolveGracefulShutdownTimeout (because os.Getenv treats them identically),
// but covering them separately guards against accidental regressions if the
// resolver is ever switched to os.LookupEnv.
type envMode int

const (
	envUnset envMode = iota
	envEmpty
	envSet
)

// applyEnv configures the env var according to mode for the duration of the
// test. For envUnset it removes the variable from the process environment and
// restores any prior value via t.Cleanup.
func applyEnv(t *testing.T, mode envMode, val string) {
	t.Helper()
	switch mode {
	case envUnset:
		prev, hadPrev := os.LookupEnv(gracefulShutdownTimeoutEnvVar)
		if err := os.Unsetenv(gracefulShutdownTimeoutEnvVar); err != nil {
			t.Fatalf("os.Unsetenv: %v", err)
		}
		t.Cleanup(func() {
			if hadPrev {
				_ = os.Setenv(gracefulShutdownTimeoutEnvVar, prev)
			} else {
				_ = os.Unsetenv(gracefulShutdownTimeoutEnvVar)
			}
		})
	case envEmpty:
		t.Setenv(gracefulShutdownTimeoutEnvVar, "")
	case envSet:
		t.Setenv(gracefulShutdownTimeoutEnvVar, val)
	}
}

func TestResolveGracefulShutdownTimeout(t *testing.T) {
	tests := []struct {
		name    string
		mode    envMode
		envVal  string
		want    *time.Duration
		wantErr bool
	}{
		{
			name: "env var truly unset returns nil with no error",
			mode: envUnset,
			want: nil,
		},
		{
			name: "env var set to empty string returns nil with no error",
			mode: envEmpty,
			want: nil,
		},
		{
			name:   "valid positive duration is parsed",
			mode:   envSet,
			envVal: "30m",
			want:   durPtr(30 * time.Minute),
		},
		{
			name:   "negative duration passes through (controller-runtime convention for wait-forever)",
			mode:   envSet,
			envVal: "-1s",
			want:   durPtr(-1 * time.Second),
		},
		{
			name:   "zero duration passes through (controller-runtime convention for disable-graceful)",
			mode:   envSet,
			envVal: "0s",
			want:   durPtr(0),
		},
		{
			name:    "invalid duration returns an error",
			mode:    envSet,
			envVal:  "garbage",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			applyEnv(t, tt.mode, tt.envVal)

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
		applyEnv(t, envSet, "7m")
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

	t.Run("env truly unset leaves GracefulShutdownTimeout nil (controller-runtime applies its default)", func(t *testing.T) {
		applyEnv(t, envUnset, "")
		opts, err := defaultManagerOptions(ControllerConfig{}, gknn, metricsserver.Options{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.GracefulShutdownTimeout != nil {
			t.Errorf("expected nil GracefulShutdownTimeout, got %v", *opts.GracefulShutdownTimeout)
		}
	})

	t.Run("env set to empty string leaves GracefulShutdownTimeout nil", func(t *testing.T) {
		applyEnv(t, envEmpty, "")
		opts, err := defaultManagerOptions(ControllerConfig{}, gknn, metricsserver.Options{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if opts.GracefulShutdownTimeout != nil {
			t.Errorf("expected nil GracefulShutdownTimeout, got %v", *opts.GracefulShutdownTimeout)
		}
	})

	t.Run("invalid env returns an error from defaultManagerOptions", func(t *testing.T) {
		applyEnv(t, envSet, "garbage")
		_, err := defaultManagerOptions(ControllerConfig{}, gknn, metricsserver.Options{})
		if err == nil {
			t.Fatal("expected error from defaultManagerOptions, got nil")
		}
	})
}

func durPtr(d time.Duration) *time.Duration {
	return &d
}
