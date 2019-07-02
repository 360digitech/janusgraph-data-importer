
# 2425 重复值在 wifi中全部有
select count(*) from migrate_id_repe_check_result_tmp a 
join migrate_use_wifi_tmp b 
on a.name = b.end_name;


insert overwrite table migrate_wifi_tmp 
select  a.* from migrate_wifi_tmp a
left join migrate_id_repe_check_result_tmp b 
on a.name = b.name
where b.name is null;



select * from migrate_use_wifi_tmp where end_name = 'FP2368037167772741632';


select row_number() over () as rowid, name from migrate_wifi_tmp limit 100;


# 合并生成唯一 id
create table migrate_id_mat_tmp as 
select row_number() over () as id, name from (
    select name from migrate_device_tmp
    union
    select name from migrate_wifi_tmp
    union
    select name from migrate_mobile_tmp 
) as abc;


insert overwrite table migrate_id_repe_check_tmp  
select name, type from (
    select name, 'd' as type from migrate_device_tmp
    union
    select name, 'w' as type from migrate_wifi_tmp
    union
    select name, 'm' as type from migrate_mobile_tmp 
) as abc;


create table migrate_id_repe_check_result_tmp as
select name, count(*) as count
from migrate_id_repe_check_tmp
group by name
having count(*) > 1;



create table migrate_mobile_id_tmp as 
select b.id, a.* from migrate_mobile_tmp a
join migrate_id_mat_tmp b 
on a.name = b.name;


create table migrate_device_id_tmp as 
select b.id, a.* from migrate_device_tmp a
join migrate_id_mat_tmp b 
on a.name = b.name;

create table migrate_wifi_id_tmp as 
select b.id, a.* from migrate_wifi_tmp a
join migrate_id_mat_tmp b 
on a.name = b.name;



DROP TABLE IF EXISTS migrate_call_id_tmp;
create table migrate_call_id_tmp as 
select b.id as start_id, c.id as end_id, a.mgm from migrate_call_tmp a
join migrate_mobile_id_tmp b 
on a.start_name = b.name
join migrate_mobile_id_tmp c
on a.end_name = c.name;


DROP TABLE IF EXISTS migrate_use_id_tmp;
create table migrate_use_id_tmp as 
select b.id as start_id, c.id as end_id from migrate_use_tmp a
join migrate_mobile_id_tmp b 
on a.start_name = b.name
join migrate_device_id_tmp c
on a.end_name = c.name;



DROP TABLE IF EXISTS migrate_has_id_tmp;
create table migrate_has_id_tmp as 
select b.id as start_id, c.id as end_id from migrate_has_tmp a
join migrate_device_id_tmp b 
on a.start_name = b.name
join migrate_mobile_id_tmp c
on a.end_name = c.name;


DROP TABLE IF EXISTS migrate_use_wifi_id_tmp;
create table migrate_use_wifi_id_tmp as 
select b.id as start_id, c.id as end_id from migrate_use_wifi_tmp a
join migrate_mobile_id_tmp b 
on a.start_name = b.name
join migrate_wifi_id_tmp c
on a.end_name = c.name;