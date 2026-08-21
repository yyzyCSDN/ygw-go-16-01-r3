# RelayDock

RelayDock is a durable webhook relay core. It accepts idempotent events, journals admission, schedules retries, fences concurrent delivery leases, tracks endpoint circuit state, and can replay journaled work after a restart. The demo uses in-memory adapters so its transaction and lifecycle behavior is deterministic in tests.
