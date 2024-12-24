-- week 2 lab 3

create table array_metrics (
    user_id numeric,
    month_start date,
    metric_name text,
    metric_array real[],
    primary key (user_id, month_start, metric_name)
);

insert into array_metrics
with daily_aggregate as (
    select
        user_id,
        date(event_time) as date,
        count(1) as num_site_hits
    from events
    where date(event_time) = date('2023-01-31')
    and user_id is not null
    group by user_id, date(event_time)
),
    yesterday_array as (
        select *
        from array_metrics
        where month_start = date('2023-01-01')
    )

select
    coalesce(da.user_id, ya.user_id) as user_id,
    coalesce(ya.month_start, date_trunc('month', da.date)) as month_start,
    'site_hits' as metric_name,
    case
        -- this is reverse order from day 2's lab
        -- we use coalesce and put a 0 if we are not fine with having null
        when ya.metric_array is not null then ya.metric_array || array[coalesce(da.num_site_hits, 0)]
        -- deal with cases where a new user shows up after the 1st of the month, so we add 0s at the start and then append the array
        -- we add the coalesce because that array cannot accept null values, but also if either of them is null then we just don't fill
        -- because we don't need to fill because that means it's the 1st day of the month
        when ya.metric_array is null then array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)]) || array[coalesce(da.num_site_hits, 0)]
    end as metric_array
from daily_aggregate da full outer join yesterday_array ya
on da.user_id = ya.user_id
on conflict (user_id, month_start, metric_name)
do
    update set metric_array = excluded.metric_array;


-- we can do N day analysis
-- going from monthly array metrics to daily aggregates
-- but it's very fast as it's the minimal set of data we need
with agg as (
    select metric_name,
        month_start,
        array [
            sum(metric_array[1]),
            sum(metric_array[2]),
            sum(metric_array[3]),
            sum(metric_array[4])
        ] as summed_array
     from array_metrics
     group by metric_name, month_start
)
select metric_name,
       month_start + cast(cast(index-1 as text) || 'day' as interval),
       elem as value
from agg
    cross join unnest(agg.summed_array)
        with ordinality as a(elem, index)
