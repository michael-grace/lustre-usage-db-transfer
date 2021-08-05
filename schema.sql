CREATE SCHEMA hgi_lustre_usage;
COMMENT ON SCHEMA hgi_lustre_usage IS "Data of lustre usage over time";

CREATE TABLE hgi_lustre_usage.pi(
    pi_id serial,
    pi_name varchar,
    primary key (pi_id)
);

CREATE TABLE hgi_lustre_usage.unix_group(
    group_id serial,
    group_name varchar,
    is_humgen boolean,
    primary key (group_id)
);

CREATE TABLE hgi_lustre_usage.volume(
    volume_id serial,
    scratch_disk varchar,
    primary key(volume_id)
)

CREATE TABLE hgi_lustre_usage.lustre_usage(
    record_id serial,
    used bigint,
    quota bigint,
    record_date date,

    archived boolean, -- check this makes sense
    last_modified int, -- keeping this here for fun historical processing

    pi_id int,
    unix_id int,
    volume_id int,
);

