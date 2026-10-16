-- Explicitly retires the Orders-derived toast.service_charges table.
-- Run together with toast_configuration.sql using psql --single-transaction.
-- Deploy the corresponding Orders writer change before resuming scheduled syncs.
-- Application amounts/flags/refunds in the old table are removed by request.
-- No CASCADE: unhandled dependencies abort the transaction.
DO $$
BEGIN
    IF to_regclass('toast.applied_taxes') IS NOT NULL THEN
        ALTER TABLE toast.applied_taxes
            ADD COLUMN IF NOT EXISTS configured_service_charge_id UUID;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'toast' AND table_name = 'service_charges'
          AND column_name = 'order_id'
    ) THEN
        IF to_regclass('toast.applied_taxes') IS NOT NULL THEN
            UPDATE toast.applied_taxes AS tax
            SET configured_service_charge_id = charge.service_charge_id
            FROM toast.service_charges AS charge
            WHERE tax.service_charge_id = charge.id
              AND tax.restaurant_id = charge.restaurant_id
              AND tax.configured_service_charge_id IS NULL;
        END IF;
        DROP TABLE toast.service_charges RESTRICT;
    END IF;
END
$$;
