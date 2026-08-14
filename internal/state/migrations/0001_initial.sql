-- Начальная схема долговременного состояния агента ноды.

CREATE TABLE agent_meta (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    usage_spool_id TEXT NOT NULL CHECK (length(usage_spool_id) > 0),
    usage_sequence INTEGER NOT NULL DEFAULT 0 CHECK (usage_sequence >= 0),
    highest_emitted_usage_sequence INTEGER NOT NULL DEFAULT 0
        CHECK (highest_emitted_usage_sequence >= 0),
    initialized INTEGER NOT NULL DEFAULT 0 CHECK (initialized IN (0, 1)),
    last_xray_audit_at_unix_ms INTEGER
) STRICT;

CREATE TABLE managed_users (
    accounting_id TEXT PRIMARY KEY CHECK (length(accounting_id) > 0),
    credential_uuid TEXT NOT NULL,
    flow TEXT NOT NULL,
    egress_key TEXT NOT NULL,
    desired_present INTEGER NOT NULL CHECK (desired_present IN (0, 1)),
    applied INTEGER NOT NULL CHECK (applied IN (0, 1)),
    updated_at_unix_ms INTEGER NOT NULL
) STRICT;

CREATE TABLE operations (
    operation_id TEXT PRIMARY KEY CHECK (length(operation_id) > 0),
    operation_type TEXT NOT NULL
        CHECK (operation_type IN ('ENSURE_PRESENT', 'ENSURE_ABSENT', 'RECONCILE')),
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'COMPLETED')),
    result BLOB,
    created_at_unix_ms INTEGER NOT NULL,
    updated_at_unix_ms INTEGER NOT NULL,
    CHECK ((status = 'PENDING' AND result IS NULL) OR
           (status = 'COMPLETED' AND result IS NOT NULL))
) STRICT;

CREATE TABLE usage_batches (
    spool_id TEXT NOT NULL CHECK (length(spool_id) > 0),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    collected_at_unix_ms INTEGER NOT NULL,
    payload BLOB NOT NULL CHECK (length(payload) > 0),
    acknowledged INTEGER NOT NULL DEFAULT 0 CHECK (acknowledged IN (0, 1)),
    PRIMARY KEY (spool_id, sequence)
) STRICT, WITHOUT ROWID;

CREATE INDEX usage_batches_pending_sequence_idx
    ON usage_batches (sequence)
    WHERE acknowledged = 0;
