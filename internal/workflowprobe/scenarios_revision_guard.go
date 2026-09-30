package workflowprobe

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

// runRevisionGuard verifies the optional plan version guard on revision
// append: a guarded append succeeds and returns the new plan version, a stale
// guard is rejected with version_conflict, a non-increasing serial with
// sequence_conflict, and an exact content replay with duplicate_revision.
// Appends without a guard keep the legacy behavior.
func runRevisionGuard() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	var plan domain.RolloutPlan
	if err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                "guard.example",
		MinimumSuccessRatio: 0.5,
		MaximumLatencyMS:    100,
		ExpectedNodes:       1,
	}, &plan); err != nil {
		return err
	}
	if plan.Version != 1 {
		return ensure(false, "new plan must start at version 1")
	}

	// A guarded append with a matching guard succeeds and reports the new
	// plan version so the caller can chain the next guard.
	var first domain.RevisionAppendResult
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", map[string]any{
		"serial":           5001,
		"authority":        "ns1.guard.example",
		"entries":          []domain.ZoneEntry{{Owner: "guard.example", Type: domain.EntryA, Value: "192.0.2.1"}},
		"expected_version": plan.Version,
	}, &first); err != nil {
		return err
	}
	if first.Plan.Version != 2 {
		return ensure(false, "guarded append must return the new plan version")
	}
	if first.Plan.ID != plan.ID || first.Plan.State != domain.PlanDraft {
		return ensure(false, "guarded append must return the plan summary")
	}
	if first.Revision.Serial != 5001 || first.Revision.EntryCount != 1 {
		return ensure(false, "guarded append must return the created revision")
	}

	// A stale guard (version 1 while the plan is at version 2) is rejected
	// with version_conflict and does not append.
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", map[string]any{
		"serial":           5002,
		"authority":        "ns1.guard.example",
		"entries":          []domain.ZoneEntry{{Owner: "guard.example", Type: domain.EntryA, Value: "192.0.2.2"}},
		"expected_version": 1,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeVersionConflict)); err != nil {
		return err
	}

	// A valid guard but a non-increasing serial is rejected with
	// sequence_conflict, distinct from a guard or content failure.
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", map[string]any{
		"serial":           5001,
		"authority":        "ns1.guard.example",
		"entries":          []domain.ZoneEntry{{Owner: "guard.example", Type: domain.EntryA, Value: "192.0.2.99"}},
		"expected_version": 2,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeSequenceConflict)); err != nil {
		return err
	}

	// A valid guard replaying the exact same revision content is rejected with
	// duplicate_revision, distinct from a sequence or guard failure.
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", map[string]any{
		"serial":           5001,
		"authority":        "ns1.guard.example",
		"entries":          []domain.ZoneEntry{{Owner: "guard.example", Type: domain.EntryA, Value: "192.0.2.1"}},
		"expected_version": 2,
	}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeDuplicateRevision)); err != nil {
		return err
	}

	// A second guarded append with the now-current version succeeds and
	// advances the version again.
	var second domain.RevisionAppendResult
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", map[string]any{
		"serial":           5002,
		"authority":        "ns1.guard.example",
		"entries":          []domain.ZoneEntry{{Owner: "guard.example", Type: domain.EntryA, Value: "192.0.2.2"}},
		"expected_version": first.Plan.Version,
	}, &second); err != nil {
		return err
	}
	if second.Plan.Version != 3 {
		return ensure(false, "second guarded append must advance the plan version")
	}

	// An append without a guard keeps the legacy behavior: no version check,
	// and the response still carries the new plan version summary.
	var unguarded domain.RevisionAppendResult
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions", domain.RevisionCreateInput{
		Serial:    5003,
		Authority: "ns1.guard.example",
		Entries:   []domain.ZoneEntry{{Owner: "guard.example", Type: domain.EntryA, Value: "192.0.2.3"}},
	}, &unguarded); err != nil {
		return err
	}
	if unguarded.Plan.Version != 4 {
		return ensure(false, "unguarded append must still return the new plan version")
	}

	return nil
}
