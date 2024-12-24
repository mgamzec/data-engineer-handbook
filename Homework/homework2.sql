-- 1. A query to deduplicate game_details from Day 1 so there's no duplicates

with deduped_game_details as (
    select *,
           row_number() over (partition by game_id, team_id, player_id) as row_num
    from game_details
)
select
    *
from deduped_game_details;
-- 2. A DDL for a user_devices_cumulated table

create table user_devices_cumulated (
    user_id numeric,
    device_activity date[],
    browser_type text,
    date date,
    primary key (user_id, browser_type, date)
);
-- 3. A cumulative query to generate device_activity_datelist from events

insert into user_devices_cumulated
with yesterday as (
    select *
    from user_devices_cumulated
    where date = date('2023-01-30')
), today as (
    select
        user_id,
        browser_type,
        date(cast(event_time as timestamp)) as date_active
    from events left join devices
        on events.device_id = devices.device_id
    where
        date(cast(event_time as timestamp)) = date('2023-01-31')
        and user_id is not null
        and devices.browser_type is not null
    group by user_id, date(cast(event_time as timestamp)), browser_type
)
select
    coalesce(t.user_id, y.user_id) as user_id,
    case
        when y.device_activity is null then array[t.date_active]
        when t.date_active is null then y.device_activity
        else array[t.date_active] || y.device_activity
    end as device_activity,
    coalesce(y.browser_type, t.browser_type) as browser_type,
    coalesce(t.date_active, y.date + interval '1 day') as date
from today t full outer join yesterday y
on t.user_id = y.user_id and t.browser_type = y.browser_type;

select count(1)
from user_devices_cumulated
-- 4. A datelist_int generation query.
-- Convert the device_activity_datelist column into a datelist_int column

with users as (
    select * from user_devices_cumulated
    where date = date('2023-01-31')
),
    series as (
        select * from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day') as series_date
    ),
    place_holder_ints as (
        select
            case
                when device_activity @> array[date(series_date)]
                    then cast(pow(2, 31 - (date - date(series_date))) as bigint)
                    else 0
                end as placeholder_int_value,
            *
        from users cross join series
    )
SELECT user_id,
       browser_type,
       cast(cast(sum(placeholder_int_value) as bigint) as bit(32)) as datelist_int_binary,
       bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) as cnt_active_dates,
       cast(sum(placeholder_int_value) as bigint) as datelist_int,
       date
FROM place_holder_ints
where user_id = '444502572952128450'
GROUP BY user_id, browser_type, date

-- 31,30,29,28,27,25,24,19,18,17,16,15,14,13,12,11,7,6,5,4,3,2
-- select * from user_devices_cumulated
-- where user_id = '444502572952128450'
-- and date = date('2023-01-31')
-- 5. A DDL for hosts_cumulated table
-- a host_activity_datelist which logs to see which dates each host is experiencing any activity

create table hosts_cumulated (
    host text,
    host_activity_datelist date[],
    today_date date,
    PRIMARY KEY (host, today_date)
);
-- 6. The incremental query to generate host_activity_datelist

insert into hosts_cumulated
with yesterday as (
    select *
    from hosts_cumulated
    where today_date = date('2023-01-30')
),
    today as (
        select
            host,
            cast(cast(event_time as timestamp) as date) as today_date
        from events
        where cast(cast(event_time as timestamp) as date) = date('2023-01-31')
        and host is not null
        group by host, cast(cast(event_time as timestamp) as date)
    )
select
    coalesce(y.host, t.host) as host,
    case
        when y.host_activity_datelist is null then array[t.today_date]
        when t.today_date is not null then array[t.today_date] || y.host_activity_datelist
        else y.host_activity_datelist
    end as host_activity_datelist,
    coalesce(t.today_date, y.today_date + interval '1 day') as today_date
from yesterday y full outer join today t
on y.host = t.host;
-- 7. A monthly, reduced fact table DDL host_activity_reduced

create table host_activity_reduced (
    month date,
    host text,
    hit_metric text,
    hit_array integer[],
    unique_visitors_metric text,
    unique_visitors_array numeric[],
    primary key (month, host, hit_metric, unique_visitors_metric)
);
-- 8. An incremental query that loads host_activity_reduced

insert into host_activity_reduced
with daily_agg as (
    select
        host,
        date(event_time) as date,
        count(1) as num_site_hits,
        COUNT(DISTINCT user_id) as unique_visitors
    from events
    where date(event_time) = date('2023-01-31')
        and user_id is not null
    group by host, date(event_time)
),
    yesterday_array as (
        select *
        from host_activity_reduced
        where month = date('2023-01-01') -- hardcoded
    )
select
    coalesce(ya.month, date_trunc('month', da.date)) as month,
    coalesce(da.host, ya.host) as host,
    'site_hits' as hit_metric,
    case
        when ya.hit_array is not null then ya.hit_array || array[coalesce(da.num_site_hits, 0)]
        when ya.hit_array is null then array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)]) || array[coalesce(da.num_site_hits, 0)]
    end as hit_array,
    'unique_visitors' as unique_visitors_metric,
    case
        when ya.unique_visitors_array is not null then ya.unique_visitors_array || array[coalesce(da.unique_visitors, 0)]
        when ya.unique_visitors_array is null then array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)]) || array[coalesce(da.unique_visitors, 0)]
    end as unique_visitors_array
from daily_agg da full outer join yesterday_array ya
on da.host = ya.host
on conflict (month, host, hit_metric, unique_visitors_metric)
do
    update set hit_array = excluded.hit_array,
               unique_visitors_array = excluded.unique_visitors_array;

-- not part of the exercise just for fun
with agg as (
    select
        host,
        month,
        array [
            sum(hit_array[1]),
            sum(hit_array[2]),
            sum(hit_array[3])
        ] as avg_hit_array,
        array [
            avg(unique_visitors_array[1]),
            avg(unique_visitors_array[2]),
            avg(unique_visitors_array[3])
        ] as avg_unique_visitors_array
    from host_activity_reduced
    group by host, month
),
daily_records as (
    select
        host,
        month,
        generate_series(1, array_length(avg_hit_array, 1)) as day_offset,
        avg_hit_array,
        avg_unique_visitors_array
    from agg
),
unnested as (
    select
        dr.host,
        dr.month + (dr.day_offset - 1) * interval '1 day' as date,
        dr.avg_hit_array[dr.day_offset] as hit_value,
        dr.avg_unique_visitors_array[dr.day_offset] as unique_visitor_value
    from daily_records dr
)
select *
from unnested
order by host, date;


