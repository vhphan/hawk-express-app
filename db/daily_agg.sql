
select
date_id,
ul_qpsk_nom  as  ul_qpsk ,
dl_mac_vol_as_scell_ext_nom/dl_mac_vol_as_scell_ext_den  as  dl_mac_vol_as_scell_ext ,
cell_availability_nom/cell_availability_den  as  cell_availability ,
"e-rab_block_rate_nom"/"e-rab_block_rate_den"  as  "e-rab_block_rate" ,
resource_block_utilizing_rate_dl_nom/resource_block_utilizing_rate_dl_den  as  resource_block_utilizing_rate_dl ,
resource_block_utilizing_rate_ul_nom/resource_block_utilizing_rate_ul_den  as  resource_block_utilizing_rate_ul ,
ul_bler_nom/ul_bler_den  as  ul_bler ,
dl_user_throughput_nom/dl_user_throughput_den  as  dl_user_throughput ,
ul_user_throughput_nom/ul_user_throughput_den  as  ul_user_throughput ,
dl_cell_throughput_nom/dl_cell_throughput_den  as  dl_cell_throughput ,
ul_cell_throughput_nom/ul_cell_throughput_den  as  ul_cell_throughput ,
dl_data_volume_gb_nom/dl_data_volume_gb_den  as  dl_data_volume_gb ,
ul_data_volume_gb_nom/ul_data_volume_gb_den  as  ul_data_volume_gb ,
total_traffic_gb_nom/total_traffic_gb_den  as  total_traffic_gb ,
dl_qpsk_nom  as  dl_qpsk ,
dl_16qam_nom  as  dl_16qam ,
dl_64qam_nom  as  dl_64qam ,
dl_256qam_nom  as  dl_256qam ,
ul_16qam_nom  as  ul_16qam ,
ul_64qam_nom  as  ul_64qam ,
ul_256qam_nom  as  ul_256qam ,
dl_mac_vol_to_scell_nom/dl_mac_vol_to_scell_den  as  dl_mac_vol_to_scell ,
dl_mac_vol_as_scell_nom/dl_mac_vol_as_scell_den  as  dl_mac_vol_as_scell ,
dl_mac_vol_to_scell_ext_nom/dl_mac_vol_to_scell_ext_den  as  dl_mac_vol_to_scell_ext 
from
dnb.daily_stats.dc_e_nr_nrcelldu_day
group by date_id;