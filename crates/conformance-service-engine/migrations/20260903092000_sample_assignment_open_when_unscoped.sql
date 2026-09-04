DROP POLICY sample_assignment_tenant ON sample_assignment;

CREATE POLICY sample_assignment_tenant ON sample_assignment
    USING (
        nullif(current_setting('app.current_tenant_id', true), '') IS NULL
        OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid
    );
