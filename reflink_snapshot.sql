-- reflink_snapshot.sql
-- Security definer function to validate backup_label
CREATE OR REPLACE FUNCTION validate_backup_label(backup_label text)
    RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
    AS $$
BEGIN
    IF backup_label IS NULL OR length(trim(backup_label)) = 0 THEN
        RAISE EXCEPTION 'backup_label cannot be null or empty';
    END IF;
    IF backup_label !~ '[a-zA-Z]' THEN
        RAISE EXCEPTION 'backup_label must contain at least one alphabetic character';
    END IF;
    IF backup_label ~ '/' THEN
        RAISE EXCEPTION 'backup_label cannot contain slashes';
    END IF;
END;
$$;

-- Security definer function to create a reflink snapshot
CREATE OR REPLACE FUNCTION reflink_snapshot(backup_label text)
    RETURNS text
    LANGUAGE plpgsql
    SECURITY DEFINER
    AS $$
DECLARE
    backup_start_result text;
    dest_prefix text := current_setting('reflink.dest_prefix');
    snapshot_path text;
BEGIN
    PERFORM
        validate_backup_label(backup_label);
    snapshot_path := dest_prefix || '/' || backup_label;
    -- Start backup
    PERFORM
        pg_backup_start(backup_label, TRUE);
    -- Use a subdirectory under $REFLINK_DEST named after the backup_label
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'cp -a --reflink=always "$PGDATA" "' || snapshot_path || '"');
    -- Stop backup and write label into the snapshot directory
    EXECUTE format('COPY (SELECT labelfile FROM pg_backup_stop(FALSE)) TO PROGRAM %L WITH csv', 'head -c -2 | tail -c +2 | tee "' || snapshot_path || '/backup_label"');
    -- Return the full filesystem path to the new snapshot
    RETURN backup_label;
END;
$$;

-- delete_snapshot.sql
-- Security definer function to delete a reflink snapshot
CREATE OR REPLACE FUNCTION delete_snapshot(backup_label text)
    RETURNS void
    LANGUAGE plpgsql
    SECURITY DEFINER
    AS $$
DECLARE
    dest_prefix text := current_setting('reflink.dest_prefix');
    snapshot_path text;
BEGIN
    PERFORM
        validate_backup_label(backup_label);
    snapshot_path := dest_prefix || '/' || backup_label;
    -- Delete the snapshot directory
    EXECUTE format('COPY (SELECT 1) TO PROGRAM %L', 'rm -rf "' || snapshot_path || '"');
END;
$$;

