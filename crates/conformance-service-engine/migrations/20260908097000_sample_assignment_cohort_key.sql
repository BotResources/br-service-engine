-- A materialised cohort-key column: the CohortIndex seam matches a caller's
-- membership keys with one indexed `WHERE cohort_key = ANY(...)`, so a cohort
-- view reads only the caller's rows. Nullable: rows seeded by the plain
-- `assignment()` helper leave it null and are simply not returned by the seam.
ALTER TABLE sample_assignment ADD COLUMN cohort_key bytea;

CREATE INDEX sample_assignment_cohort_key_idx ON sample_assignment (cohort_key);
