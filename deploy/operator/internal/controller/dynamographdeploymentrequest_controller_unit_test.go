/*
 * SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

package controller

import (
	"context"
	"strings"
	"testing"

	configv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/config/v1alpha1"
	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testControllerScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := nvidiacomv1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1 scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1 scheme: %v", err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatalf("add batchv1 scheme: %v", err)
	}

	return scheme
}

func TestCreateProfilingJobDisablesRetriesAndPersistsFailureMetadata(t *testing.T) {
	ctx := context.Background()
	scheme := testControllerScheme(t)

	dgdr := &nvidiacomv1beta1.DynamoGraphDeploymentRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "unit-no-retries",
			Namespace:  "default",
			UID:        types.UID("unit-no-retries-uid"),
			Generation: 1,
		},
		Spec: nvidiacomv1beta1.DynamoGraphDeploymentRequestSpec{
			Model:   "test-model",
			Backend: "trtllm",
			Image:   "test-profiler:latest",
			Hardware: &nvidiacomv1beta1.HardwareSpec{
				GPUSKU:         nvidiacomv1beta1.GPUSKUTypeH100SXM,
				NumGPUsPerNode: ptr.To[int32](8),
				VRAMMB:         ptr.To(81920.0),
				TotalGPUs:      ptr.To[int32](8),
			},
			SLA: &nvidiacomv1beta1.SLASpec{
				TTFT: ptr.To(50.0),
				ITL:  ptr.To(10.0),
			},
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(dgdr).
		Build()

	reconciler := &DynamoGraphDeploymentRequestReconciler{
		Client:    client,
		APIReader: client,
		Recorder:  record.NewFakeRecorder(10),
		Config: &configv1alpha1.OperatorConfiguration{
			Namespace: configv1alpha1.NamespaceConfiguration{
				Restricted: "restricted",
			},
		},
	}

	var fetched nvidiacomv1beta1.DynamoGraphDeploymentRequest
	if err := client.Get(ctx, types.NamespacedName{Name: dgdr.Name, Namespace: dgdr.Namespace}, &fetched); err != nil {
		t.Fatalf("get dgdr: %v", err)
	}

	if err := reconciler.createProfilingJob(ctx, &fetched); err != nil {
		t.Fatalf("create profiling job: %v", err)
	}

	var job batchv1.Job
	if err := client.Get(ctx, types.NamespacedName{Name: getProfilingJobName(&fetched), Namespace: dgdr.Namespace}, &job); err != nil {
		t.Fatalf("get profiling job: %v", err)
	}

	if job.Spec.BackoffLimit == nil || *job.Spec.BackoffLimit != 0 {
		t.Fatalf("expected backoffLimit 0, got %#v", job.Spec.BackoffLimit)
	}

	var script string
	for _, container := range job.Spec.Template.Spec.Containers {
		if container.Name == ContainerNameOutputCopier {
			if len(container.Args) != 1 {
				t.Fatalf("expected sidecar script arg, got %d args", len(container.Args))
			}
			script = container.Args[0]
			break
		}
	}
	if script == "" {
		t.Fatal("output-copier sidecar not found")
	}

	for _, needle := range []string{
		`echo "  status: \"$STATUS\"" >> /tmp/cm.yaml`,
		`echo "  error: \"$ERROR\"" >> /tmp/cm.yaml`,
		`if [ "$STATUS" = "success" ]; then`,
	} {
		if !strings.Contains(script, needle) {
			t.Fatalf("expected sidecar script to contain %q", needle)
		}
	}

	failedStart := strings.Index(script, "  failed)")
	runningStart := strings.Index(script, "  running)")
	if failedStart < 0 || runningStart <= failedStart {
		t.Fatalf("failed to isolate failed() block in sidecar script")
	}
	if strings.Contains(script[failedStart:runningStart], "exit 1") {
		t.Fatalf("failed() block should not exit 1:\n%s", script[failedStart:runningStart])
	}
}

func TestHandleProfilingPhaseFailsFromOutputConfigMapStatus(t *testing.T) {
	ctx := context.Background()
	scheme := testControllerScheme(t)

	dgdr := &nvidiacomv1beta1.DynamoGraphDeploymentRequest{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "unit-output-status-fail",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: nvidiacomv1beta1.DynamoGraphDeploymentRequestSpec{
			Model:   "test-model",
			Backend: "vllm",
			Image:   "test-profiler:latest",
			Hardware: &nvidiacomv1beta1.HardwareSpec{
				GPUSKU:         nvidiacomv1beta1.GPUSKUTypeH100SXM,
				NumGPUsPerNode: ptr.To[int32](8),
				VRAMMB:         ptr.To(81920.0),
				TotalGPUs:      ptr.To[int32](8),
			},
			SLA: &nvidiacomv1beta1.SLASpec{
				TTFT: ptr.To(100.0),
				ITL:  ptr.To(1500.0),
			},
		},
		Status: nvidiacomv1beta1.DynamoGraphDeploymentRequestStatus{
			Phase:          nvidiacomv1beta1.DGDRPhaseProfiling,
			ProfilingPhase: nvidiacomv1beta1.ProfilingPhaseInitializing,
		},
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getProfilingJobName(dgdr),
			Namespace: dgdr.Namespace,
		},
		Status: batchv1.JobStatus{
			Conditions: []batchv1.JobCondition{{
				Type:   batchv1.JobComplete,
				Status: corev1.ConditionTrue,
			}},
		},
	}

	outputCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getOutputConfigMapName(dgdr),
			Namespace: dgdr.Namespace,
		},
		Data: map[string]string{
			"phase":   string(nvidiacomv1beta1.ProfilingPhaseSweepingDecode),
			"message": "Profiler stopped during decode sweep",
			"status":  "failed",
			"error":   "OOM while sweeping decode configs",
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(dgdr).
		WithObjects(dgdr, job, outputCM).
		Build()

	reconciler := &DynamoGraphDeploymentRequestReconciler{
		Client:    client,
		APIReader: client,
		Recorder:  record.NewFakeRecorder(10),
		Config:    &configv1alpha1.OperatorConfiguration{},
	}

	var fetched nvidiacomv1beta1.DynamoGraphDeploymentRequest
	if err := client.Get(ctx, types.NamespacedName{Name: dgdr.Name, Namespace: dgdr.Namespace}, &fetched); err != nil {
		t.Fatalf("get dgdr: %v", err)
	}

	result, err := reconciler.handleProfilingPhase(ctx, &fetched)
	if err != nil {
		t.Fatalf("handle profiling phase: %v", err)
	}
	if !result.Requeue {
		t.Fatalf("expected requeue on profiler-reported failure")
	}

	var updated nvidiacomv1beta1.DynamoGraphDeploymentRequest
	if err := client.Get(ctx, types.NamespacedName{Name: dgdr.Name, Namespace: dgdr.Namespace}, &updated); err != nil {
		t.Fatalf("get updated dgdr: %v", err)
	}

	if updated.Status.Phase != nvidiacomv1beta1.DGDRPhaseFailed {
		t.Fatalf("expected phase Failed, got %q", updated.Status.Phase)
	}
	if updated.Status.ProfilingPhase != nvidiacomv1beta1.ProfilingPhaseSweepingDecode {
		t.Fatalf("expected profilingPhase SweepingDecode, got %q", updated.Status.ProfilingPhase)
	}

	var profilingCond, succeededCond *metav1.Condition
	for i := range updated.Status.Conditions {
		cond := &updated.Status.Conditions[i]
		switch cond.Type {
		case nvidiacomv1beta1.ConditionTypeProfiling:
			profilingCond = cond
		case nvidiacomv1beta1.ConditionTypeSucceeded:
			succeededCond = cond
		}
	}

	if profilingCond == nil {
		t.Fatal("profiling condition missing")
	}
	if profilingCond.Reason != nvidiacomv1beta1.ProfilingReasonSweepingDecodeFailed {
		t.Fatalf("expected profiling reason %q, got %q", nvidiacomv1beta1.ProfilingReasonSweepingDecodeFailed, profilingCond.Reason)
	}
	if profilingCond.Message != "OOM while sweeping decode configs" {
		t.Fatalf("expected profiling message to use output ConfigMap error, got %q", profilingCond.Message)
	}

	if succeededCond == nil {
		t.Fatal("succeeded condition missing")
	}
	if succeededCond.Reason != nvidiacomv1beta1.ProfilingReasonSweepingDecodeFailed {
		t.Fatalf("expected succeeded reason %q, got %q", nvidiacomv1beta1.ProfilingReasonSweepingDecodeFailed, succeededCond.Reason)
	}
	if succeededCond.Message != "OOM while sweeping decode configs" {
		t.Fatalf("expected succeeded message to use output ConfigMap error, got %q", succeededCond.Message)
	}
}
