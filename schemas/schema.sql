CREATE SCHEMA hgi_lustre_usage_new;
COMMENT ON SCHEMA hgi_lustre_usage_new IS "Data of lustre usage over time";

CREATE TABLE hgi_lustre_usage_new.pi(
    pi_id int AUTO_INCREMENT,
    pi_name text,
    primary key (pi_id)
);

CREATE TABLE hgi_lustre_usage_new.unix_group(
    group_id int AUTO_INCREMENT,
    group_name text,
    is_humgen boolean,
    primary key (group_id)
);

CREATE TABLE hgi_lustre_usage_new.volume(
    volume_id int AUTO_INCREMENT,
    scratch_disk text,
    primary key(volume_id)
)

CREATE TABLE hgi_lustre_usage_new.lustre_usage(
    record_id int AUTO_INCREMENT,
    used bigint(8),
    quota bigint(8),
    record_date date,

    archived boolean, -- check this makes sense
    last_modified int, -- keeping this here for fun historical processing

    pi_id int,
    unix_id int,
    volume_id int,

    primary key(record_id),

    foreign key (pi_id) references hgi_lustre_usage_new.pi(pi_id),
    foreign key (unix_id) references hgi_lustre_usage_new.unix_group(group_id),
    foreign key (volume_id) references hgi_lustre_usage_new.volume(volume_id)
);

