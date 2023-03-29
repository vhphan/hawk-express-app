create schema stats;

-- set db owner to dnb
alter database dnb owner to dnb;

-- set schema owner to dnb
alter schema stats owner to dnb;

select version();