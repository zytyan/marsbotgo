-- name: GetMarsInfo :one
SELECT *
FROM mars_info
WHERE group_id = ?
  AND pic_dhash = ?;

-- name: UpsertMarsInfo :exec
INSERT INTO mars_info (group_id, pic_dhash, count, last_msg_id, in_whitelist)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(group_id, pic_dhash) DO UPDATE SET count=excluded.count,
                                               last_msg_id=excluded.last_msg_id,
                                               in_whitelist=excluded.in_whitelist;

-- name: GetDhashFromFileUid :one
SELECT dhash
FROM fuid_to_dhash
WHERE fuid = ?;

-- name: IsUserInWhitelist :one
SELECT EXISTS (SELECT 1 FROM group_user_in_whitelist WHERE group_id = ? AND user_id = ?);

-- name: UpsertDhash :exec
INSERT INTO fuid_to_dhash (fuid, dhash)
VALUES (?, ?)
ON CONFLICT DO UPDATE SET dhash=excluded.dhash;

-- name: AddUserToWhitelist :exec
INSERT INTO group_user_in_whitelist(group_id, user_id)
VALUES (?, ?);

-- name: DeleteUserFromWhitelist :exec
DELETE
FROM group_user_in_whitelist
WHERE group_id = ?
  AND user_id = ?