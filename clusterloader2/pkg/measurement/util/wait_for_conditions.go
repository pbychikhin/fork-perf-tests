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

package util

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
)

// WaitForGenericK8sObjectsOptions is an options object used by WaitForGenericK8sObjectsNodes methods.
type WaitForGenericK8sObjectsOptions struct {
	// GroupVersionResource identifies the resource to fetch.
	GroupVersionResource schema.GroupVersionResource
	// Namespaces identifies namespaces which should be observed.
	Namespaces NamespacesRange
	// SuccessfulConditions lists conditions to look for in the objects denoting good objects.
	// Formatted as `ConditionType=ConditionStatus`, e.g. `Scheduled=true`.
	SuccessfulConditions []string
	// OptionalSuccessfulConditions lists conditions that are only checked when
	// an object already satisfies SuccessfulConditions. If a condition's Type
	// is not present on the object it is silently ignored; if it IS present
	// its Status must match. When matchAll is true every present optional
	// condition must match; when false at least one present must match (or
	// none are present).
	OptionalSuccessfulConditions []string
	// FailedConditions lists conditions to look for in the objects denoting bad objects.
	// Formatted as `ConditionType=ConditionStatus`, e.g. `Failed=true`.
	FailedConditions []string
	// MinDesiredObjectCount describes minimum number of objects that should contain
	// successful or failed condition.
	MinDesiredObjectCount int
	// MaxFailedObjectCount describes maximum number of objects that could contain failed condition.
	MaxFailedObjectCount int
	// CallerName identifies the measurement making the calls.
	CallerName string
	// WaitInterval contains interval for which the function waits between refreshes.
	WaitInterval time.Duration
	// ConditionFieldMapping overrides the default field names used to locate
	// and interpret condition-like entries within .status. When zero-valued,
	// DefaultConditionFieldMapping() is used (status.conditions[].type/status).
	ConditionFieldMapping ConditionFieldMapping
	// MatchAll requires ALL entries in SuccessfulConditions (and separately
	// ALL in FailedConditions) to be present on an object for it to be
	// counted as successful (or failed). When false (default), a single
	// matching condition is enough.
	MatchAll bool
}

// NamespacesRange represents namespace range which will be queried.
type NamespacesRange struct {
	Prefix string
	Min    int
	Max    int
}

// Summary returns summary which should be included in all logs.
func (o *WaitForGenericK8sObjectsOptions) Summary() string {
	return fmt.Sprintf("%s: objects: %q, namespaces: %q", o.CallerName, o.GroupVersionResource.String(), o.Namespaces.String())
}

// String returns printable representation of the namespaces range.
func (nr *NamespacesRange) String() string {
	if nr.Prefix == "" {
		return ""
	}
	return fmt.Sprintf("%s-(%d-%d)", nr.Prefix, nr.Min, nr.Max)
}

// getMap returns a map with namespaces which should be queried.
func (nr *NamespacesRange) getMap() map[string]bool {
	result := map[string]bool{}
	if nr.Prefix == "" {
		result[""] = true // Cluster-scoped objects.
		return result
	}
	for i := nr.Min; i <= nr.Max; i++ {
		result[fmt.Sprintf("%s-%d", nr.Prefix, i)] = true
	}
	return result
}

// conditionSpec represents a parsed condition specification.
// Condition specs support two formats:
//   - "Type=Value"        — checks the default status field (from ConditionFieldMapping)
//   - "Type@field=Value"  — checks the named field within the condition entry
type conditionSpec struct {
	condType string
	field    string
	value    string
}

// parseConditionSpec parses a condition specification string into its components.
func parseConditionSpec(spec string, defaultField string) conditionSpec {
	eqIdx := strings.Index(spec, "=")
	if eqIdx < 0 {
		return conditionSpec{condType: spec, field: defaultField}
	}
	lhs := spec[:eqIdx]
	value := spec[eqIdx+1:]
	if atIdx := strings.Index(lhs, "@"); atIdx >= 0 {
		return conditionSpec{
			condType: lhs[:atIdx],
			field:    lhs[atIdx+1:],
			value:    value,
		}
	}
	return conditionSpec{
		condType: lhs,
		field:    defaultField,
		value:    value,
	}
}

func (cs conditionSpec) matches(c GenericCondition) bool {
	return c.Type == cs.condType && c.Fields[cs.field] == cs.value
}

// WaitForGenericK8sObjects waits till the desired number of k8s objects
// fulfills given conditions requirements, ctx.Done() channel is used to
// wait for timeout.
func WaitForGenericK8sObjects(ctx context.Context, dynamicClient dynamic.Interface, options *WaitForGenericK8sObjectsOptions) error {
	mapping := options.ConditionFieldMapping
	if mapping == (ConditionFieldMapping{}) {
		mapping = DefaultConditionFieldMapping()
	}
	defaultField := mapping.StatusField
	store, err := NewDynamicObjectStore(ctx, dynamicClient, options.GroupVersionResource, options.Namespaces.getMap(), mapping)
	if err != nil {
		return err
	}

	objects, err := store.ListObjectSimplifications()
	if err != nil {
		return err
	}
	successful, failed, count := countObjectsMatchingConditions(objects, options.SuccessfulConditions, options.OptionalSuccessfulConditions, options.FailedConditions, options.MatchAll, defaultField)
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s: timeout while waiting for %d objects to be successful or failed - currently there are: successful=%d failed=%d count=%d",
				options.Summary(), options.MinDesiredObjectCount, len(successful), len(failed), count)
		case <-time.After(options.WaitInterval):
			objects, err := store.ListObjectSimplifications()
			if err != nil {
				return err
			}
			successful, failed, count = countObjectsMatchingConditions(objects, options.SuccessfulConditions, options.OptionalSuccessfulConditions, options.FailedConditions, options.MatchAll, defaultField)

			klog.V(2).Infof("%s: successful=%d failed=%d count=%d", options.Summary(), len(successful), len(failed), count)
			if klog.V(4).Enabled() {
				for _, detail := range objectConditionDetails(objects, options.SuccessfulConditions, options.OptionalSuccessfulConditions, options.FailedConditions, defaultField) {
					klog.V(4).Infof("%s: %s", options.Summary(), detail)
				}
			}
			if options.MinDesiredObjectCount <= len(successful)+len(failed) {
				if options.MaxFailedObjectCount < len(failed) {
					return fmt.Errorf("%s: too many failed objects, expected at most %d - currently there are: successful=%d failed=%d count=%d failed-objects=[%s]",
						options.Summary(), options.MaxFailedObjectCount, len(successful), len(failed), count, strings.Join(failed, ","))
				}
				return nil
			}
		}
	}
}

// countObjectsMatchingConditions counts objects that have a successful or failed condition.
// When matchAll is false (default), an object is successful/failed if ANY of its
// conditions matches ANY entry in the respective list.
// When matchAll is true, an object is successful only when ALL entries in
// successfulConditions are found among its conditions. Failed conditions are
// checked first and use ANY-match semantics regardless of matchAll.
// optionalSuccessfulConditions are checked only after the main successful
// check passes; conditions whose Type is absent from the object are ignored.
//
// Condition specs support per-field overrides: "Type@field=Value" checks the
// named field instead of the default status field.
func countObjectsMatchingConditions(objects []ObjectSimplification, successfulConditions []string, optionalSuccessfulConditions []string, failedConditions []string, matchAll bool, defaultField string) (successful []string, failed []string, count int) {
	parsedSuccessful := make([]conditionSpec, len(successfulConditions))
	for i, sc := range successfulConditions {
		parsedSuccessful[i] = parseConditionSpec(sc, defaultField)
	}
	parsedFailed := make([]conditionSpec, len(failedConditions))
	for i, fc := range failedConditions {
		parsedFailed[i] = parseConditionSpec(fc, defaultField)
	}

	count = len(objects)
	for _, object := range objects {
		if matchAll {
			isFailed := false
			for _, c := range object.Conditions {
				for _, spec := range parsedFailed {
					if spec.matches(c) {
						failed = append(failed, object.String())
						isFailed = true
						break
					}
				}
				if isFailed {
					break
				}
			}
			if isFailed {
				continue
			}
			allMatched := len(parsedSuccessful) > 0
			for _, spec := range parsedSuccessful {
				found := false
				for _, c := range object.Conditions {
					if spec.matches(c) {
						found = true
						break
					}
				}
				if !found {
					allMatched = false
					break
				}
			}
			if allMatched {
				allMatched = checkOptionalConditions(object.Conditions, optionalSuccessfulConditions, true, defaultField)
			}
			if allMatched {
				successful = append(successful, object.String())
			}
		} else {
			isSuccessful := false
			for _, c := range object.Conditions {
				matchedAny := false
				for _, spec := range parsedSuccessful {
					if spec.matches(c) {
						isSuccessful = true
						matchedAny = true
						break
					}
				}
				if matchedAny {
					break
				}
				for _, spec := range parsedFailed {
					if spec.matches(c) {
						failed = append(failed, object.String())
						matchedAny = true
						break
					}
				}
				if matchedAny {
					break
				}
			}
			if isSuccessful {
				isSuccessful = checkOptionalConditions(object.Conditions, optionalSuccessfulConditions, false, defaultField)
			}
			if isSuccessful {
				successful = append(successful, object.String())
			}
		}
	}
	return
}

// objectConditionDetails builds a per-object diagnostic showing every condition
// found on the object and which of the wanted conditions matched or are missing.
func objectConditionDetails(objects []ObjectSimplification, successfulConditions []string, optionalSuccessfulConditions []string, failedConditions []string, defaultField string) []string {
	var details []string
	for _, object := range objects {
		var allKeys []string
		for _, c := range object.Conditions {
			allKeys = append(allKeys, fmt.Sprintf("%s=%s", c.Type, c.Fields[defaultField]))
		}

		var matchedSuccessful []string
		var missingSuccessful []string
		for _, sc := range successfulConditions {
			spec := parseConditionSpec(sc, defaultField)
			found := false
			for _, c := range object.Conditions {
				if spec.matches(c) {
					found = true
					break
				}
			}
			if found {
				matchedSuccessful = append(matchedSuccessful, sc)
			} else {
				missingSuccessful = append(missingSuccessful, sc)
			}
		}

		var matchedFailed []string
		for _, fc := range failedConditions {
			spec := parseConditionSpec(fc, defaultField)
			for _, c := range object.Conditions {
				if spec.matches(c) {
					matchedFailed = append(matchedFailed, fc)
					break
				}
			}
		}

		var matchedOptional, mismatchedOptional []string
		for _, oc := range optionalSuccessfulConditions {
			spec := parseConditionSpec(oc, defaultField)
			for _, c := range object.Conditions {
				if c.Type == spec.condType {
					if c.Fields[spec.field] == spec.value {
						matchedOptional = append(matchedOptional, oc)
					} else {
						mismatchedOptional = append(mismatchedOptional,
							fmt.Sprintf("%s(actual=%s=%s)", oc, spec.field, c.Fields[spec.field]))
					}
					break
				}
			}
		}

		details = append(details, fmt.Sprintf("object %s: conditions=[%s] matched-successful=[%s] matched-failed=[%s] missing-successful=[%s] matched-optional=[%s] mismatched-optional=[%s]",
			object.String(),
			strings.Join(allKeys, ", "),
			strings.Join(matchedSuccessful, ", "),
			strings.Join(matchedFailed, ", "),
			strings.Join(missingSuccessful, ", "),
			strings.Join(matchedOptional, ", "),
			strings.Join(mismatchedOptional, ", ")))
	}
	return details
}

// checkOptionalConditions validates optional conditions against the object's
// actual conditions. Only conditions whose Type is present on the object are
// considered. When matchAll is true every present optional condition must
// match; when false at least one present must match (or none are present).
func checkOptionalConditions(conditions []GenericCondition, optionalConditions []string, matchAll bool, defaultField string) bool {
	if len(optionalConditions) == 0 {
		return true
	}
	if matchAll {
		for _, oc := range optionalConditions {
			spec := parseConditionSpec(oc, defaultField)
			for _, c := range conditions {
				if c.Type == spec.condType {
					if c.Fields[spec.field] != spec.value {
						return false
					}
					break
				}
			}
		}
		return true
	}
	// matchAll == false: any present optional condition that matches is enough.
	// If none are present on the object the check passes vacuously.
	anyPresent := false
	for _, oc := range optionalConditions {
		spec := parseConditionSpec(oc, defaultField)
		for _, c := range conditions {
			if c.Type == spec.condType {
				anyPresent = true
				if c.Fields[spec.field] == spec.value {
					return true
				}
				break
			}
		}
	}
	return !anyPresent
}
