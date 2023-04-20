create schema stats;

-- set db owner to dnb
alter database dnb owner to dnb;

-- set schema owner to dnb
alter schema stats owner to dnb;

select version();

-- create schema hourly_stats


create schema hourly_stats;

alter schema hourly_stats owner to dnb;
