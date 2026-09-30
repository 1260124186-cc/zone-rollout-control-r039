package workflowprobe

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

// revisionRequest is the guarded append shape used by revision version
// guard probes.
type revisionRequest struct {
	domain.RevisionCreateInput
	ExpectedVersion *uint64 `json:"expected_version,omitempty"`
}

func revisionInput(serial uint32, value string) domain.RevisionCreateInput {
	return domain.RevisionCreateInput{
		Serial:    serial,
		Authority: "ns1.edge.example",
		Entries: []domain.ZoneEntry{
			{Owner: "edge.example", Type: domain.EntryA, Value: value},
		},
	}
}

func runRevisionVersionGuard() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	var plan domain.RolloutPlan
	if err := harness.call(http.MethodPut, "/v1/plans", domain.PlanCreateInput{
		Zone:                "edge.example",
		MinimumSuccessRatio: 1,
		MaximumLatencyMS:    80,
		ExpectedNodes:       2,
	}, &plan); err != nil {
		return err
	}

	// A legacy request without expected_version keeps the unguarded behavior
	// and still returns the new plan version summary.
	var first domain.RevisionAppendResult
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions",
		revisionInput(2026093001, "192.0.2.10"), &first); err != nil {
		return err
	}
	if first.Serial != 2026093001 || first.Plan.Version != 2 ||
		first.Plan.LatestRevisionID != first.ID {
		return ensure(false, "unguarded append did not return the plan version summary")
	}

	// A guarded append on the current version succeeds and bumps the version.
	v2 := uint64(2)
	var second domain.RevisionAppendResult
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions",
		revisionRequest{
			RevisionCreateInput: revisionInput(2026093002, "192.0.2.11"),
			ExpectedVersion:     &v2,
		}, &second); err != nil {
		return err
	}
	if second.Serial != 2026093002 || second.Plan.Version != 3 ||
		second.Plan.LatestRevisionID != second.ID || second.Plan.State != domain.PlanDraft {
		return ensure(false, "guarded append did not return the updated plan version summary")
	}

	// A stale guard is reported as a version conflict distinct from serial
	// and content failures, and nothing is appended.
	stale := uint64(1)
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions",
		revisionRequest{
			RevisionCreateInput: revisionInput(2026093003, "192.0.2.12"),
			ExpectedVersion:     &stale,
		}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeVersionConflict)); err != nil {
		return err
	}

	// The same stale guard wins even when the serial would also collide:
	// guard verification happens in the same transaction before appending.
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions",
		revisionRequest{
			RevisionCreateInput: revisionInput(2026093002, "192.0.2.11"),
			ExpectedVersion:     &stale,
		}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeVersionConflict)); err != nil {
		return err
	}

	// Current guard but a non-increasing serial is a serial conflict.
	v3 := uint64(3)
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions",
		revisionRequest{
			RevisionCreateInput: revisionInput(2026093002, "192.0.2.12"),
			ExpectedVersion:     &v3,
		}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeSerialConflict)); err != nil {
		return err
	}

	// Current guard with identical content is a content duplicate, distinct
	// from the serial conflict above.
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions",
		revisionRequest{
			RevisionCreateInput: revisionInput(2026093002, "192.0.2.11"),
			ExpectedVersion:     &v3,
		}, nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeDuplicateRevision)); err != nil {
		return err
	}

	// Rejected appends did not move the plan version; the next guarded
	// append with a fresh serial succeeds and returns the new summary.
	var current domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &current); err != nil {
		return err
	}
	if current.Version != 3 || current.LatestRevisionID != second.ID {
		return ensure(false, "failed guarded appends changed the plan version")
	}

	var third domain.RevisionAppendResult
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions",
		revisionRequest{
			RevisionCreateInput: revisionInput(2026093003, "192.0.2.12"),
			ExpectedVersion:     &v3,
		}, &third); err != nil {
		return err
	}
	if third.Plan.Version != 4 || third.Plan.LatestRevisionID != third.ID {
		return ensure(false, "final guarded append did not return the new plan version")
	}

	// Legacy clients without a guard keep the original serial collision
	// behavior: duplicate serial stays duplicate_revision, a regression
	// stays a validation error.
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions",
		revisionInput(2026093003, "192.0.2.99"), nil)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeDuplicateRevision)); err != nil {
		return err
	}
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions",
		revisionInput(2026092999, "192.0.2.99"), nil)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	// An explicit zero guard is a request validation error.
	zero := uint64(0)
	err = harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/revisions",
		revisionRequest{
			RevisionCreateInput: revisionInput(2026093004, "192.0.2.13"),
			ExpectedVersion:     &zero,
		}, nil)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	return nil
}
