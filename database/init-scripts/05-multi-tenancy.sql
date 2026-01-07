-- Multi-Tenancy & Row-Level Security (RLS) Setup
-- This script implements database-enforced tenant isolation using PostgreSQL RLS
-- In the Pagila dataset, store_id is used as the tenant identifier

-- 1. Create a secure function to retrieve the current tenant
-- This avoids repeating the casting logic and provides a central point for logic
CREATE OR REPLACE FUNCTION current_tenant_id() RETURNS integer AS $$
DECLARE
    tenant_id_text text;
BEGIN
    -- 'app.current_tenant' is the session variable we will set via asyncpg
    tenant_id_text := current_setting('app.current_tenant', true);

    -- Strict Mode: If no tenant is set, return NULL.
    -- RLS policies equating to NULL usually fail safe (return no rows).
    IF tenant_id_text IS NULL THEN
        RETURN NULL;
    END IF;

    RETURN tenant_id_text::integer;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- 2. Enable RLS on core tables
-- We treat 'store_id' as the tenant identifier for the Pagila dataset.
ALTER TABLE customer ENABLE ROW LEVEL SECURITY;
ALTER TABLE rental ENABLE ROW LEVEL SECURITY;
ALTER TABLE payment ENABLE ROW LEVEL SECURITY;
ALTER TABLE staff ENABLE ROW LEVEL SECURITY;
ALTER TABLE inventory ENABLE ROW LEVEL SECURITY;

-- 3. Force RLS to prevent table owners (like 'postgres') from bypassing checks
-- This ensures that even during maintenance, policies are active unless explicitly bypassed.
ALTER TABLE customer FORCE ROW LEVEL SECURITY;
ALTER TABLE rental FORCE ROW LEVEL SECURITY;
ALTER TABLE payment FORCE ROW LEVEL SECURITY;
ALTER TABLE staff FORCE ROW LEVEL SECURITY;
ALTER TABLE inventory FORCE ROW LEVEL SECURITY;

-- 4. Create Isolation Policies
-- Strategy: Users can only see rows where store_id matches their session tenant.

-- Customer Policy
CREATE POLICY tenant_isolation_customer ON customer
    FOR ALL
    USING (store_id = current_tenant_id());

-- Staff Policy
CREATE POLICY tenant_isolation_staff ON staff
    FOR ALL
    USING (store_id = current_tenant_id());

-- Inventory Policy
CREATE POLICY tenant_isolation_inventory ON inventory
    FOR ALL
    USING (store_id = current_tenant_id());

-- Rental Policy
CREATE POLICY tenant_isolation_rental ON rental
    FOR ALL
    USING (store_id = current_tenant_id());

-- Payment Policy (temporary - will be updated after denormalization)
-- Note: Payment table will be denormalized below to include store_id
-- For now, we link via staff.store_id
CREATE POLICY tenant_isolation_payment ON payment
    FOR ALL
    USING (
        EXISTS (
            SELECT 1 FROM staff s
            WHERE s.staff_id = payment.staff_id
            AND s.store_id = current_tenant_id()
        )
    );

-- 5. Indexing for Performance
-- RLS adds a WHERE clause. If this column isn't indexed, every query becomes a Seq Scan.
CREATE INDEX IF NOT EXISTS idx_customer_store_id ON customer(store_id);
CREATE INDEX IF NOT EXISTS idx_payment_store_id ON payment(store_id);
CREATE INDEX IF NOT EXISTS idx_rental_store_id ON rental(store_id);
CREATE INDEX IF NOT EXISTS idx_staff_store_id ON staff(store_id);
CREATE INDEX IF NOT EXISTS idx_inventory_store_id ON inventory(store_id);

-- 6. Denormalize payment table for RLS performance
-- Add store_id column to payment table
ALTER TABLE payment ADD COLUMN IF NOT EXISTS store_id INTEGER;

-- Populate store_id from staff table (one-time migration)
UPDATE payment p
SET store_id = s.store_id
FROM staff s
WHERE p.staff_id = s.staff_id
AND p.store_id IS NULL;

-- Make store_id NOT NULL after population
ALTER TABLE payment ALTER COLUMN store_id SET NOT NULL;

-- Add foreign key constraint
ALTER TABLE payment
ADD CONSTRAINT fk_payment_store
FOREIGN KEY (store_id) REFERENCES store(store_id);

-- 7. Update RLS policy to use direct store_id (replaces temporary policy above)
DROP POLICY IF EXISTS tenant_isolation_payment ON payment;
CREATE POLICY tenant_isolation_payment ON payment
    FOR ALL
    USING (store_id = current_tenant_id());

-- Ensure index exists (already created above, but verify)
CREATE INDEX IF NOT EXISTS idx_payment_store_id ON payment(store_id);

-- Grant execute permission on the function to the read-only user
GRANT EXECUTE ON FUNCTION current_tenant_id() TO bi_agent_ro;
