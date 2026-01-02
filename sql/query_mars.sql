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

-- name: IncrementMarsInfo :one
INSERT INTO mars_info (group_id, pic_dhash, count, last_msg_id, in_whitelist)
VALUES (?, ?, 1, ?, 0)
ON CONFLICT(group_id, pic_dhash) DO UPDATE SET count       = count + 1,
                                               last_msg_id = excluded.last_msg_id
RETURNING group_id,
    pic_dhash,
    count,
    last_msg_id,
    in_whitelist;

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
  AND user_id = ?;

-- name: SetMarsWhitelist :exec
INSERT INTO mars_info (group_id, pic_dhash, count, last_msg_id, in_whitelist)
VALUES (?, ?, 0, 0, ?)
ON CONFLICT(group_id, pic_dhash) DO UPDATE SET in_whitelist = excluded.in_whitelist;

-- name: IncrementGroupStat :exec
INSERT INTO mars_group_stat (group_id, image_count)
VALUES (?, 1)
ON CONFLICT(group_id) DO UPDATE SET image_count = image_count + 1;

-- name: CountGroups :one
SELECT COUNT(*)
FROM mars_group_stat
WHERE group_id < 0;

-- name: GetGroupMarsCount :one
SELECT image_count
FROM mars_group_stat
WHERE group_id = ?;

-- name: ListMarsInfoByGroup :many
SELECT group_id, pic_dhash, count, last_msg_id, in_whitelist
FROM mars_info
WHERE group_id = ?;

-- name: ListSimilarPhotos :many
SELECT sqlc.embed(mars_info),
       CAST(hamming_distance(pic_dhash, CAST(@src_dhash AS BLOB)) AS INTEGER) AS hd
FROM mars_info
WHERE group_id = ?
  AND hd < CAST(@min_distance AS INTEGER)
ORDER BY hd
LIMIT 10;