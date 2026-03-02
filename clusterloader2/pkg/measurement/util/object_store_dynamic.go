/*
Copyright 2023 The Kubernetes Authors.

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

/*
This file is copy of https://github.com/kubernetes/kubernetes/blob/master/test/utils/pod_store.go
with slight changes regarding labelSelector and flagSelector applied.
*/

package util

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

// ConditionFieldMapping allows overriding the default field names used to
// locate and interpret condition-like entries within .status.
type ConditionFieldMapping struct {
	// StatusPath is the field name under .status that holds the conditions
	// array. Defaults to "conditions".
	StatusPath string
	// TypeField is the field name within each condition entry that serves
	// as the condition type. Defaults to "type".
	TypeField string
	// StatusField is the field name within each condition entry that serves
	// as the condition status value. Defaults to "status".
	StatusField string
}

// DefaultConditionFieldMapping returns the mapping that matches the standard
// metav1.Condition layout (status.conditions[].type / status).
func DefaultConditionFieldMapping() ConditionFieldMapping {
	return ConditionFieldMapping{
		StatusPath:  "conditions",
		TypeField:   "type",
		StatusField: "status",
	}
}

// GenericCondition is a type/status pair extracted from an object's status
// using the configured ConditionFieldMapping.
type GenericCondition struct {
	Type   string
	Status string
}

const (
	defaultResyncInterval = 10 * time.Second
)

// DynamicObjectStore is a convenient wrapper around cache.GenericLister.
type DynamicObjectStore struct {
	cache.GenericLister
	namespaces   map[string]bool
	fieldMapping ConditionFieldMapping
}

// NewDynamicObjectStore creates DynamicObjectStore based on given object version resource and selector.
func NewDynamicObjectStore(ctx context.Context, dynamicClient dynamic.Interface, gvr schema.GroupVersionResource, namespaces map[string]bool, fieldMapping ConditionFieldMapping) (*DynamicObjectStore, error) {
	informerFactory := dynamicinformer.NewDynamicSharedInformerFactory(dynamicClient, defaultResyncInterval)
	lister := informerFactory.ForResource(gvr).Lister()
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())

	return &DynamicObjectStore{
		GenericLister: lister,
		namespaces:    namespaces,
		fieldMapping:  fieldMapping,
	}, nil
}

// ListObjectSimplifications returns list of objects with conditions for each object that was returned by lister.
func (s *DynamicObjectStore) ListObjectSimplifications() ([]ObjectSimplification, error) {
	objects, err := s.GenericLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}

	result := make([]ObjectSimplification, 0, len(objects))
	for _, o := range objects {
		os, err := getObjectSimplification(o, s.fieldMapping)
		if err != nil {
			return nil, err
		}
		if !s.namespaces[os.Metadata.Namespace] {
			continue
		}
		result = append(result, os)
	}
	return result, nil
}

// ObjectSimplification represents the content of the object
// that is needed to be handled by this measurement.
type ObjectSimplification struct {
	Metadata   metav1.ObjectMeta
	Conditions []GenericCondition
}

func (o ObjectSimplification) String() string {
	return fmt.Sprintf("%s/%s", o.Metadata.Namespace, o.Metadata.Name)
}

func getObjectSimplification(o runtime.Object, mapping ConditionFieldMapping) (ObjectSimplification, error) {
	dataMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(o)
	if err != nil {
		return ObjectSimplification{}, err
	}

	metadataJSON, err := json.Marshal(dataMap["metadata"])
	if err != nil {
		return ObjectSimplification{}, err
	}
	var metadata metav1.ObjectMeta
	if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
		return ObjectSimplification{}, err
	}

	var conditions []GenericCondition
	if statusMap, ok := dataMap["status"].(map[string]interface{}); ok {
		if condList, ok := statusMap[mapping.StatusPath].([]interface{}); ok {
			for _, item := range condList {
				condMap, ok := item.(map[string]interface{})
				if !ok {
					continue
				}
				cond := GenericCondition{}
				if t, ok := condMap[mapping.TypeField].(string); ok {
					cond.Type = t
				}
				if s, ok := condMap[mapping.StatusField].(string); ok {
					cond.Status = s
				}
				conditions = append(conditions, cond)
			}
		}
	}

	return ObjectSimplification{
		Metadata:   metadata,
		Conditions: conditions,
	}, nil
}
