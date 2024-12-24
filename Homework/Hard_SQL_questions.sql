'''Who are the top 10 NBA players by consecutive 20+ point seasons?
 
Using the table bootcamp.nba_player_seasons, find the most consecutive seasons a player has scored 20+ points and find the top 10 players! Make sure to handle ties correctly (you might have more than 10 records!). Sort the output data set by consecutive_seasons descending and player_name ascending!

These are the tables to query for this question:
bootcamp.nba_player_seasons
player_name string
age int
height string
weight int
college string
country string
draft_year string
draft_round string
draft_number string
gp double
pts double
reb double
ast double
netrtg double
oreb_pct double
dreb_pct double
usg_pct double
ts_pct double
ast_pct double
season int
Your answer should include these columns:
player_name varchar
consecutive_seasons integer
'''

with player_20pt_seasons as (
    select
        player_name,
        season,
        case when pts >= 20 then 1 else 0 end as has_20pts
    from bootcamp.nba_player_seasons
),
prev_season as (
    select
        player_name,
        season,
        has_20pts,
        lag(has_20pts, 1) over (partition by player_name order by season) as previous_season_has_20pts
    from player_20pt_seasons
),
indicators as (
    select *,
        case
            when has_20pts <> previous_season_has_20pts then 1
            else 0
        end as change_ind
    from prev_season
),
streaks as (
    select *,
        sum(change_ind) over (partition by player_name order by season) as streak_identifier
    from indicators
),
almost_final as (
    select 
        player_name,
        has_20pts,
        min(season) as start_season,
        max(season) as end_season
    from streaks
    group by player_name, streak_identifier, has_20pts
    order by player_name, streak_identifier
),
ranked_results as (
    select
        player_name,
        end_season + 1 - start_season as consecutive_seasons,
        rank() over (order by end_season + 1 - start_season desc) as rank
    from almost_final
    where has_20pts = 1
)
select
    player_name,
    consecutive_seasons
from ranked_results
where rank <= 10
order by rank, player_name
