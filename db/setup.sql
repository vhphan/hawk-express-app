select version();

-- create schema hourly_stats


create schema hourly_stats;

alter schema hourly_stats owner to dnb;






-- alter column dnb_index in table daiky_stats.df_dpm to varcahr(100)
alter table daily_stats.df_dpm alter column dnb_index type varchar(100);

alter table daily_stats.df_dpm alter column "on_board_date" type varchar(100);
alter table daily_stats.df_dpm alter column "api_call_date" type varchar(100);
alter table daily_stats.df_dpm alter column "added" type varchar(100);
alter table daily_stats.df_dpm alter column "Acceptance_Cluster" type varchar(100);
alter table daily_stats.df_dpm alter column "Sub_Cluster" type varchar(100);
alter table daily_stats.df_dpm alter column "CBOClusterName" type varchar(100);

create table daily_stats.meta (
    "table_name" varchar(100),
    "last_updated" timestamp(0)
);


-- create a unique contraint on column table_name in table daily_stats.meta
alter table daily_stats.meta add constraint meta_table_name_key unique ("table_name");

-- create a trigger to update the last_updated column in table daily_stats.meta
-- for each insert, update, or delete in table daily_stats.df_dpm
create or replace function update_meta() returns trigger as $$
begin
    insert into daily_stats.meta ("table_name", "last_updated")
    values (TG_TABLE_NAME, now())
    on conflict ("table_name") do update
    set "last_updated" = now();
    return null;
end
$$ language plpgsql;

-- create trigger on table daily_stats.df_dpm
-- for all rows instead of each row

create trigger update_meta_df_dpm
    after insert or update or delete on daily_stats.df_dpm
    for each statement execute procedure update_meta();




-- create a test table with columns id, and name
create table daily_stats.test (
    id int,
    name varchar(100)
);


create trigger update_meta_test
    after insert or update or delete on daily_stats.test
    for each statement execute procedure update_meta();


create trigger update_meta_cell_mapping
    after insert or update or delete on daily_stats.cell_mapping
    for each statement execute procedure update_meta();

-- show all extensions installed

select * from pg_extension;

CREATE EXTENSION postgis;


-- insert test rows into table daily_stats.test
insert into daily_stats.test values (1, 'test1');

select * from dnb.daily_stats.meta;

-- show all triggers in database dnb
select * from pg_catalog.pg_trigger;

SELECT * FROM pg_available_extensions;



alter table daily_stats.cell_mapping alter column "Cellname" type varchar(100);
alter table daily_stats.cell_mapping alter column "Region" type varchar(100);
alter table daily_stats.cell_mapping alter column "Cluster_ID" type varchar(100);
alter table daily_stats.cell_mapping alter column "District" type varchar(100);
alter table daily_stats.cell_mapping alter column "MCMC_State" type varchar(100);
alter table daily_stats.cell_mapping alter column "SITEID" type varchar(100);
alter table daily_stats.cell_mapping alter column "SystemID" type varchar(100);
alter table daily_stats.cell_mapping alter column "Sitename" type varchar(100);



alter table daily_stats.dc_e_nr_nrcelldu_day alter column "nrcelldu" type varchar(100);
alter table daily_stats.dc_e_nr_nrcellcu_day alter column "nrcellcu" type varchar(100);



alter table daily_stats.dc_e_nr_nrcelldu_day alter column "nr_name" type varchar(100);
alter table daily_stats.dc_e_nr_nrcellcu_day alter column "nr_name" type varchar(100);



-- create index on daily_stats.dc_e_nr_nrcelldu_day using column nrcelldu

create index dc_e_nr_nrcelldu_day_nrcelldu_idx on daily_stats.dc_e_nr_nrcelldu_day (nrcelldu);



create index on daily_stats.dc_e_nr_nrcelldu_day (nr_name);
create index on daily_stats.dc_e_nr_nrcellcu_day (nr_name);



create index dc_e_nr_nrcellcu_day_nrcellcu_idx on daily_stats.dc_e_nr_nrcellcu_day (nrcellcu);
