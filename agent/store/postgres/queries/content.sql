-- name: Content :one
SELECT data, media_type, durability FROM content WHERE authority = $1 AND key = $2;

-- name: InsertContent :exec
INSERT INTO content (authority, key, data, media_type, durability) VALUES ($1, $2, $3, $4, $5);

-- name: UpdateContentDurability :exec
UPDATE content SET durability = $1 WHERE authority = $2 AND key = $3;
