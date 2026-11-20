# PostgreSQL
On VM1, actual DB persists in a Docker named volume. `postgres-init/001-schema.sql` only applies at database initialization; editing it does not migrate an existing DB. Use versioned DB migrations for new schemas.
