select * from pg_indexes where schemaname = 'daily_stats' and tablename ilike 'dc_e%' and indexdef ilike '%unique%';

select * from pg_indexes where schemaname = 'hourly_stats' and tablename ilike 'dc_e%' and indexdef ilike '%unique%';


CREATE UNIQUE INDEX pk_dc_e_nr_nrcellcu_day ON daily_stats.dc_e_nr_nrcellcu_day USING btree (nrcellcu, date_id)

CREATE UNIQUE INDEX pk_dc_e_nr_events_nrcellcu_flex_daily ON daily_stats.dc_e_nr_events_nrcellcu_flex_day USING btree (nrcellcu, date_id, flex_filtername)




CREATE UNIQUE INDEX pk_dc_e_nr_nrcelldu_day ON daily_stats.dc_e_nr_nrcelldu_day USING btree (nrcelldu, date_id)



CREATE UNIQUE INDEX pk_dc_e_nr_events_nrcelldu_flex_day ON daily_stats.dc_e_nr_events_nrcelldu_flex_day USING btree (nrcelldu, date_id, flex_filtername)



CREATE UNIQUE INDEX pk_dc_e_vpp_rpuserplanelink_v_day ON daily_stats.dc_e_vpp_rpuserplanelink_v_day USING btree (ne_name, date_id)



CREATE UNIQUE INDEX pk_dc_e_erbsg2_mpprocessingresource_v_day ON daily_stats.dc_e_erbsg2_mpprocessingresource_v_day USING btree (date_id, erbs)



CREATE UNIQUE INDEX pk_dc_e_nr_nrcelldu_v_day ON daily_stats.dc_e_nr_nrcelldu_v_day USING btree (nrcelldu, date_id)