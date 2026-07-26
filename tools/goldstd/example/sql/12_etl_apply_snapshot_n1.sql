INSERT OVERWRITE TABLE dwd.dwd_apply_snapshot
SELECT * FROM ods.ods_t_apply WHERE dt = '2026-07-25'
