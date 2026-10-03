-- name: Binding :one
SELECT binding FROM bindings WHERE id = $1;

-- name: InsertBinding :exec
INSERT INTO bindings (id, digest, binding) VALUES ($1, $2, $3);

-- name: Claim :one
SELECT claim FROM claims WHERE id = $1;

-- name: InsertClaim :exec
INSERT INTO claims (id, owner_kind, owner_authority, owner_identity, state, claim) VALUES ($1, $2, $3, $4, $5, $6);

-- name: UpdateClaim :exec
UPDATE claims SET state = $1, claim = $2 WHERE id = $3;

-- name: ClaimsByOwnerAfter :many
SELECT id, owner_identity, claim FROM claims WHERE owner_kind = $1 AND owner_authority = $2 AND id > $3 ORDER BY id LIMIT $4;

-- name: ClaimIdentitiesByOwnerDesc :many
SELECT id, owner_identity FROM claims WHERE owner_kind = $1 AND owner_authority = $2 ORDER BY id DESC LIMIT $3;

-- name: ClaimIdentitiesByOwnerBefore :many
SELECT id, owner_identity FROM claims WHERE owner_kind = $1 AND owner_authority = $2 AND id < $3 ORDER BY id DESC LIMIT $4;
