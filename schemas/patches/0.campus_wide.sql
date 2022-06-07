create table base_directory
(
	base_directory_id int,
	directory_path text not null,
	volume_id int not null,
	constraint base_directory_volume_volume_id_fk
		foreign key (volume_id) references volume (volume_id)
);

create unique index base_directory_base_directory_id_uindex
	on base_directory (base_directory_id);

alter table base_directory
	add constraint base_directory_pk
		primary key (base_directory_id);

alter table base_directory modify base_directory_id int auto_increment;

-- note: below may be a bit temperemaental

alter table lustre_usage
	add base_directory_id int null;

drop index volume_id on lustre_usage;

create index volume_id
	on lustre_usage (volume_id);

alter table lustre_usage
	add constraint lustre_usage_base_directory_base_directory_id_fk
		foreign key (base_directory_id) references base_directory (base_directory_id);

alter table lustre_usage drop foreign key lustre_usage_ibfk_3;

alter table lustre_usage drop column volume_id;

alter table lustre_usage
	add constraint lustre_usage_ibfk_3
		foreign key (volume_id) references volume (volume_id);

alter table unix_group drop column is_humgen;

alter table lustre_usage drop column archived;

