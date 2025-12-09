# Changelog

## [Schema 2024.09.0] - 2024-09-30
- Added `task_cascadence.ume.schema_public` exporting versioned dataclass schemas for UME events such as `StageUpdate`, `AuditEvent`, and `TaskRun`.
- Transports now coerce emitted UME events to the public schema so external dashboards and agents can lock to the published schema version.
