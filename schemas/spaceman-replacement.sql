CREATE TABLE hgi_lustre_usage_new.directory(
    directory_id int AUTO_INCREMENT,
    project_name text,
    directory_path text,
    num_files bigint(8),
    size bigint(8),
    last_modified int,
    
    pi_id int,
    volume_id int,
    group_id int,

    primary key(directory_id),

    foreign key (pi_id) references hgi_lustre_usage_new.pi(pi_id),
    foreign key (group_id) references hgi_lustre_usage_new.unix_group(group_id),
    foreign key (volume_id) references hgi_lustre_usage_new.volume(volume_id)
);

CREATE TABLE hgi_lustre_usage_new.filetype(
    filetype_id int AUTO_INCREMENT,
    filetype_name text,

    primary key(filetype_id)
);

CREATE TABLE hgi_lustre_usage_new.file_size(
    file_size_id int AUTO_INCREMENT,
    directory_id int,
    filetype_id int,
    size float,

    primary key(file_size_id),

    foreign key (directory_id) references hgi_lustre_usage_new.directory(directory_id),
    foreign key (filetype_id) references hgi_lustre_usage_new.filetype(filetype_id)
);

INSERT INTO hgi_lustre_usage_new.filetype (filetype_name)
    VALUES ("BAM"), ("CRAM"), ("VCF"), ("PEDBED");