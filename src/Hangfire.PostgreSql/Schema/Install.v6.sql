DO $$
BEGIN
    BEGIN
        CREATE INDEX "ix_hangfire_counter_expireat" ON "counter" ("expireat");
    EXCEPTION
        WHEN duplicate_table THEN RAISE NOTICE 'INDEX ix_hangfire_counter_expireat already exists.';
    END;
END;
$$;

DO $$
BEGIN
    BEGIN
        CREATE INDEX "ix_hangfire_jobqueue_jobidandqueue" ON "jobqueue" ("jobid","queue");
    EXCEPTION
        WHEN duplicate_table THEN RAISE NOTICE 'INDEX "ix_hangfire_jobqueue_jobidandqueue" already exists.';
    END;
END;
$$;

