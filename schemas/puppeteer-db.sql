CREATE TABLE hgi_lustre_usage_new.vault_actions(
    vault_action_id int AUTO_INCREMENT,
    action_name text,

    primary key(vault_action_id)
);

INSERT INTO hgi_lustre_usage_new.vault_actions (action_name)
    VALUES ('Keep'), ('Archive');

CREATE TABLE hgi_lustre_usage_new.vault(
    record_id int AUTO_INCREMENT,
    record_date date,
    filepath text,
    group_id int,
    vault_action_id int,
    size int,
    file_owner text,
    last_modified date,
    volume_id int,

    primary key(record_id),
    foreign key (vault_action_id) references hgi_lustre_usage_new.vault_actions(vault_action_id),
    foreign key (group_id) references hgi_lustre_usage_new.unix_group(group_id),
    foreign key (volume_id) references hgi_lustre_usage_new.volume(volume_id)
);