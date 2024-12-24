-- week 2 lab 2

create table users_cumulated (
    user_id text,
    -- lists of dates in the past where the user was active
    dates_active date[],
    -- current date for the user
    date date,
    primary key (user_id, date)
);

insert into users_cumulated
with yesterday as (
    select *
    from users_cumulated
    where date = date('2023-01-30')
), today as (
    select
        cast(user_id as text) as user_id,
        date(cast(event_time as timestamp)) as date_active
    from events
    where
        date(cast(event_time as timestamp)) = date('2023-01-31')
        and user_id is not null -- deal with null user_ids in this data
    group by user_id, date(cast(event_time as timestamp))
)
select
    coalesce(t.user_id, y.user_id) as user_id,
    case
        when y.dates_active is null then array[t.date_active]
        when t.date_active is null then y.dates_active -- we don't want to keep adding a big array of nulls
        else array[t.date_active] || y.dates_active
    end as dates_active,
    -- today's date_active might not be date if the user doesn't exist yet
    -- so we add 1 to yesterday's
    coalesce(t.date_active, y.date + interval '1 day') as date
from today t full outer join yesterday y
on t.user_id = y.user_id;


-- generate a datelist for 30 days
with users as (
    select * from users_cumulated
    where date = date('2023-01-31')
),
    series as (
        select * from generate_series(date('2023-01-01'), date('2023-01-31'), interval '1 day') as series_date
    ),
    place_holder_ints as (
        select
            case
                when dates_active @> array[date(series_date)]
                    -- date - series_date is # of days b/e current date and series date
-- if we cast a power of 2 number as bits and turn it into binary
-- then we can get a history of 1s and 0s active/inactive
                    then cast(pow(2, 32 - (date - date(series_date))) as bigint)
                    else 0
                end as placeholder_int_value,
            *
        from users cross join series -- we got the 31 days for each user
    )
select
    user_id,
    -- these are extremely efficient operations
    -- bit_count() can give us how many times the user is active
    bit_count(cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_is_monthly_active,
    -- let's check a user is active in the last 7 days
    bit_count(cast('11111110000000000000000000000000' as bit(32)) & --bit-wise and
        cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_is_weekly_active,
    -- daily is the same but with the 1st one only 1
    bit_count(cast('10000000000000000000000000000000' as bit(32)) & --bit-wise and
        cast(cast(sum(placeholder_int_value) as bigint) as bit(32))) > 0 as dim_is_daily_active
from place_holder_ints
group by user_id;
