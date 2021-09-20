CREATE TABLE hgi_lustre_usage_new.warning(
    warning_id int AUTO_INCREMENT,
    warning text,

    primary key(warning_id)
);

INSERT INTO hgi_lustre_usage_new.warning (warning) VALUES ('OK'), ('Kinda OK'), ('Not OK');

ALTER TABLE hgi_lustre_usage_new.lustre_usage
    ADD warning_id int;

ALTER TABLE hgi_lustre_usage_new.lustre_usage
    add foreign key(warning_id) references warning(warning_id);

ALTER TABLE hgi_lustre_usage_new.vault
    DROP COLUMN file_owner;

ALTER TABLE hgi_lustre_usage_new.vault
    ADD user_id int;

ALTER TABLE hgi_lustre_usage_new.vault
    add foreign key(user_id) references user(user_id);