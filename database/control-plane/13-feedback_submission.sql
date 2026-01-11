-- Feedback Table
CREATE TABLE feedback (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    interaction_id UUID NOT NULL REFERENCES query_interactions(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    thumb TEXT CHECK (thumb IN ('UP', 'DOWN')),
    comment TEXT,
    feedback_source TEXT DEFAULT 'end_user'
);

CREATE INDEX idx_feedback_interaction ON feedback(interaction_id);

-- Review Queue Table
CREATE TABLE review_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    interaction_id UUID NOT NULL REFERENCES query_interactions(id),
    status TEXT DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'APPROVED', 'REJECTED', 'NEEDS_FIX')),
    resolution_type TEXT CHECK (resolution_type IN ('APPROVED_AS_IS', 'APPROVED_WITH_SQL_FIX', 'DUPLICATE_OF_EXISTING', 'CANNOT_FIX')),
    corrected_sql TEXT,
    canonical_group_id UUID, -- Will be FK later when table exists
    reviewer_notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Ensure unique active review item per interaction?
-- Or allow multiple reviews if interaction is re-opened.
-- For idempotency on downvote, we might want unique constraint on (interaction_id) where status=PENDING?
CREATE UNIQUE INDEX idx_review_queue_pending ON review_queue(interaction_id) WHERE status = 'PENDING';

ALTER TABLE feedback ENABLE ROW LEVEL SECURITY;
ALTER TABLE review_queue ENABLE ROW LEVEL SECURITY;

CREATE POLICY service_role_full_access_fb ON feedback USING (true) WITH CHECK (true);
CREATE POLICY service_role_full_access_rq ON review_queue USING (true) WITH CHECK (true);
