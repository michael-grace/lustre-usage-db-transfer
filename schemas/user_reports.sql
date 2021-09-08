CREATE TABLE hgi_lustre_usage_new.user(
    user_id int AUTO_INCREMENT,
    user_name text,

    primary key (user_id)
);

CREATE TABLE hgi_lustre_usage_new.user_usage(
    record_id int AUTO_INCREMENT,
    record_date date,
    user_id int,
    group_id int,
    volume_id int,
    size bigint,
    last_modified date,

    primary key(record_id),
    foreign key (user_id) references hgi_lustre_usage_new.user(user_id),
    foreign key (group_id) references hgi_lustre_usage_new.unix_group(group_id),
    foreign key (volume_id) references hgi_lustre_usage_new.volume(volume_id)
);