CREATE TABLE hgi_lustre_usage_new.vault_actions(
    vault_action_id int AUTO_INCREMENT,
    action_name text,

    primary key(vault_action_id)
);

INSERT INTO hgi_lustre_usage_new.vault_actions (action_name)
    VALUES ("keep"), ("archive");

CREATE TABLE hgi_lustre_usage_new.vault(
    record_id int AUTO_INCREMENT,
    record_date date,
    filepath text,
    vault_action_id int,
    ttl int,

    volume_id int,

    primary key(record_id),
    foreign key (vault_action_id) references hgi_lustre_usage_new.vault_actions(vault_action_id)
);