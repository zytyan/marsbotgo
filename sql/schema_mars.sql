CREATE TABLE group_user_in_whitelist
(
    group_id INTEGER NOT NULL,
    user_id  INTEGER NOT NULL,
    PRIMARY KEY (group_id, user_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS "fuid_to_dhash"
(
    fuid  TEXT not null
        primary key,
    dhash BLOB not null
) without rowid;

CREATE TABLE IF NOT EXISTS "mars_info"
(
    group_id     INTEGER           not null,
    pic_dhash    BLOB              not null,
    count        INTEGER default 0 not null,
    last_msg_id  INTEGER default 0 not null,
    in_whitelist INTEGER default 0 not null,
    primary key (group_id, pic_dhash),
    check (count >= 0),
    check (in_whitelist IN (0, 1)),
    check (last_msg_id >= 0)
) without rowid;

CREATE TABLE IF NOT EXISTS mars_group_stat
(
    group_id    INTEGER PRIMARY KEY NOT NULL,
    image_count INTEGER             NOT NULL DEFAULT 0 CHECK (image_count >= 0)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS mars_stat_meta
(
    key   TEXT PRIMARY KEY NOT NULL,
    value INTEGER          NOT NULL
) WITHOUT ROWID;