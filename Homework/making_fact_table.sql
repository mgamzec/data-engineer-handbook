-- fact data modelling lab 1

-- check if game_id, team_id, player_id are unique
-- we find there is (almost) 2 of every record
select
    game_id, team_id, player_id, count(1)
from game_details
group by game_id, team_id, player_id
having count(1) > 1;

-- Thing to remember when doing fact data modelling
-- are the cols we are giving even useful?
-- don't put any derived values in the fact table, let the analyst do w/e they want
-- when logging we might get double data
-- so it's common to dedupe as the 1st step
insert into fct_game_details
with deduped as (
    select
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        row_number() over (partition by gd.game_id, team_id, player_id order by g.game_date_est) as row_num
    from game_details gd join games g on gd.game_id = g.game_id
)
select
    -- all these cols are a fundamental nature of the fact
    -- we might want to put all IDs first then dim them m
    game_date_est as dim_game_date,
    season as dim_season,
    team_id as dim_team_id,
    player_id as dim_player_id,
    player_name as dim_player_name,
    start_position as dim_start_position,
    team_id = home_team_id as dim_is_playing_at_home,
    coalesce(position('DNP' in comment), 0) > 0 as dim_did_not_play,
    coalesce(position('DND' in comment), 0) > 0 as dim_did_not_dress,
    coalesce(position('NWT' in comment), 0) > 0 as dim_not_with_team,
--     comment -- we can remove it if we are confident that we have parsed all options
    cast(split_part(min, ':', 1) as real)
        + cast(split_part(min, ':', 2) as real)/60 as m_minutes,
    fgm as m_fgm,
    fga as m_fga,
    fg3m as m_fg3m,
    fg3a as m_fg3a,
    ftm as m_ftm,
    fta as m_fta,
    oreb as m_oreb,
    dreb as m_dreb,
    reb as m_reb,
    ast as m_ast,
    stl as m_stl,
    blk as m_blk,
    "TO" as m_turnovers,
    pf as m_pf,
    pts as m_pts,
    plus_minus as m_plus_minus
from deduped
where row_num = 1;

create table fct_game_details (
    -- dim_ are cols that you should group by and filter on
    dim_game_date date,
    dim_season integer,
    dim_team_id integer,
    dim_player_id integer,
    dim_player_name text,
    dim_start_position text,
    dim_is_playing_at_home boolean,
    dim_did_not_play boolean,
    dim_did_not_dress boolean,
    dim_not_with_team boolean,
    -- measures -> m_
    -- m_ are cols that you should agg and to math
    m_minutes real,
    m_fgm integer,
    m_fga integer,
    m_fg3m integer,
    m_fg3a integer,
    m_ftm integer,
    m_fta integer,
    m_oreb integer,
    m_dreb integer,
    m_reb integer,
    m_ast integer,
    m_stl integer,
    m_blk integer,
    m_turnovers integer,
    m_pf integer,
    m_pts integer,
    m_plus_minus integer,
    -- the PK helps create indexes which is good if we do filtering on cols in the PK
    primary key (dim_game_date, dim_team_id, dim_player_id)
);

-- we may have lost the team info but we can easily bring them in
select t.*, gd.*
from fct_game_details gd join teams t
on t.team_id = gd.dim_team_id;

-- lets find the players that bailed out on the most games
-- fact data modelling is about making tables that are easy to query to create cool things
select dim_player_name,
       count(1) as num_games,
       count(case when dim_not_with_team then 1 end) as bailed_num,
       cast(count(case when dim_not_with_team then 1 end) as real)/count(1) as bail_pct
from fct_game_details
group by 1
order by 4 desc;
