CREATE USER streaming_replica WITH REPLICATION;

-- Connect to the maintenance database (e.g., 'postgres')
GRANT pg_read_all_settings TO streaming_replica;

GRANT EXECUTE ON FUNCTION pg_backup_start(text, boolean) TO streaming_replica;

GRANT EXECUTE ON FUNCTION pg_backup_stop(boolean) TO streaming_replica;

GRANT EXECUTE ON FUNCTION pg_switch_wal() TO streaming_replica;

GRANT EXECUTE ON FUNCTION delete_snapshot(text) TO streaming_replica;

GRANT EXECUTE ON FUNCTION validate_backup_label(text) TO streaming_replica;

-- For faster backups (Postgres 15+):
GRANT pg_checkpoint TO streaming_replica;

