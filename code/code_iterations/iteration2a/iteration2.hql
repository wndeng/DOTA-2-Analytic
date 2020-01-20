// login hive
> beeline
> !connect jdbc:hive2://babar.es.its.nyu.edu:10000/
// enter username and password
> use ys3603;


// create table and load hero_names from HDFS
create external table hero_names (id int, name string) row format delimited fields terminated by ',' location 'hdfs://dumbo/user/ys3603/hero/';

create external table hero_win (match_id string, hero_id int, win boolean)  row format delimited fields terminated by ',' location 'hdfs://dumbo/user/ys3603/d2/';

// create hero win table with hero name
create external table hero_name_win (match_id string, hero_name string, win boolean);
// insert data 
insert into hero_name_win select hero_win.match_id, hero_names.name, hero_win.win from hero_win join hero_names where hero_win.hero_id = hero_names.id;

// Get win rate for each player
> create external table hero_win_count (hero string, wins int, games int);
> insert into hero_win_count select hero, count(case win when true then 1 else null end), count(*) from hero_name_win group by hero_name;
> create external table hero_win_rate (hero string, win_rate double);
> insert into hero_win_rate select hero, wins/CAST(games as double) from hero_win_count;
> select * from hero_win_rate sort by win_rate DESC limit 10;

result: 
+---------------------+-------------------------+--+
| hero_win_rate.hero  | hero_win_rate.win_rate  |
+---------------------+-------------------------+--+
| Omniknight          | 0.625563293732077       |
| Spectre             | 0.5922298400261182      |
| Abaddon             | 0.5910931174089069      |
| Necrophos           | 0.5821640162145307      |
| Ursa                | 0.5784451305079988      |
| Skeleton King       | 0.5768398268398268      |
| Warlock             | 0.5661881977671451      |
| Crystal Maiden      | 0.5607718894009217      |
| Zeus                | 0.554111631870473       |
| Lich                | 0.5503826530612245      |
+---------------------+-------------------------+--+
10 rows selected (46.988 seconds)

// tip: external table, use insert overwrite table to reinit

> create external table hero_win_1 (match_id string, hero_id int, win boolean, team int,)  row format delimited fields terminated by ',' location 'hdfs://dumbo/user/ys3603/d2/';
> create external table hero_name_win_1 (match_id string, hero_name string, team int, win boolean);
> insert into hero_name_win_1 select hero_win_1.match_id, hero_names.name, hero_win_1.team, hero_win_1.win from hero_win_1 join hero_names where hero_win_1.hero_id = hero_names.id;

// buddy
> create external table hero_buddy_win (hero1 string, hero2 string, win boolean);
> insert into hero_buddy_win select a.hero_name, b.hero_name, a.win from hero_name_win_1 a join hero_name_win_1 b on a.match_id = b.match_id and a.team = b.team;
> insert overwrite table hero_buddy_win select * from hero_buddy_win where hero1 != hero2; # remove self buddy
> create external table hero_buddy_count (hero1 string, hero2 string, wins int, games int);
> insert into hero_buddy_count select hero1, hero2, count(case win when true then 1 else null end), count(*) from hero_buddy_win group by hero1, hero2;

> create external table hero_buddy_win_rate (hero1 string, hero2 string, win_rate double);
> insert overwrite table hero_buddy_win_rate select hero1, hero2, wins/CAST(games as double) from hero_buddy_count;
> select * from hero_buddy_win_rate sort by win_rate DESC limit 10;


+----------------------------+----------------------------+-------------------------------+--+
| hero_buddy_win_rate.hero1  | hero_buddy_win_rate.hero2  | hero_buddy_win_rate.win_rate  |
+----------------------------+----------------------------+-------------------------------+--+
| Batrider                   | Chen                       | 1.0                           |
| Lone Druid                 | Elder Titan                | 1.0                           |
| Elder Titan                | Lone Druid                 | 1.0                           |
| Chen                       | Batrider                   | 1.0                           |
| Beastmaster                | Terrorblade                | 1.0                           |
| Chen                       | Brewmaster                 | 1.0                           |
| Chen                       | Shadow Demon               | 1.0                           |
| Visage                     | Morphling                  | 1.0                           |
| Chen                       | Morphling                  | 1.0                           |
| Brewmaster                 | Chen                       | 1.0                           |
+----------------------------+----------------------------+-------------------------------+--+
10 rows selected (54.356 seconds)
total pair: 134710

select * from hero_buddy_win_count sort by win limit 50;
+-------------------------+-------------------------+------------------------+-------------------------+--+
| hero_buddy_count.hero1  | hero_buddy_count.hero2  | hero_buddy_count.wins  | hero_buddy_count.games  |
+-------------------------+-------------------------+------------------------+-------------------------+--+
| Chen                    | Oracle                  | 0                      | 1                       |
| Chen                    | Shadow Demon            | 1                      | 1                       |
| Oracle                  | Chen                    | 0                      | 1                       |
| Shadow Demon            | Chen                    | 1                      | 1                       |
| Treant Protector        | Chen                    | 1                      | 2                       |
| Winter Wyvern           | Visage                  | 1                      | 2                       |
| Morphling               | Chen                    | 2                      | 2                       |
| Wisp                    | Brewmaster              | 1                      | 2                       |
| Chen                    | Treant Protector        | 1                      | 2                       |
| Shadow Demon            | Wisp                    | 0                      | 2                       |
| Chen                    | Morphling               | 2                      | 2                       |
| Visage                  | Chen                    | 1                      | 2                       |
| Chen                    | Visage                  | 1                      | 2                       |
| Visage                  | Winter Wyvern           | 1                      | 2                       |
| Chen                    | Centaur Warrunner       | 1                      | 2                       |
| Meepo                   | Brewmaster              | 1                      | 2                       |
| Dark Seer               | Chen                    | 1                      | 2                       |
| Chen                    | Elder Titan             | 1                      | 2                       |
| Chen                    | Dark Seer               | 1                      | 2                       |
| Centaur Warrunner       | Chen                    | 1                      | 2                       |
| Elder Titan             | Chen                    | 1                      | 2                       |
| Brewmaster              | Meepo                   | 1                      | 2                       |
| Brewmaster              | Wisp                    | 1                      | 2                       |
| Wisp                    | Shadow Demon            | 0                      | 2                       |
| Wisp                    | Naga Siren              | 1                      | 3                       |
| Treant Protector        | Shadow Demon            | 1                      | 3                       |
| Disruptor               | Chen                    | 1                      | 3                       |
| Chen                    | Skywrath Mage           | 2                      | 3                       |
| Chen                    | Winter Wyvern           | 2                      | 3                       |
| Visage                  | Morphling               | 3                      | 3                       |
| Visage                  | Shadow Demon            | 2                      | 3                       |
| Brewmaster              | Shadow Demon            | 1                      | 3                       |
| Chen                    | Disruptor               | 1                      | 3                       |
| Visage                  | Earth Spirit            | 0                      | 3                       |
| Visage                  | Elder Titan             | 1                      | 3                       |
| Lone Druid              | Elder Titan             | 3                      | 3                       |
| Shadow Demon            | Brewmaster              | 1                      | 3                       |
| Earth Spirit            | Visage                  | 0                      | 3                       |
| Shadow Demon            | Naga Siren              | 1                      | 3                       |
| Chen                    | Techies                 | 3                      | 3                       |
| Shadow Demon            | Treant Protector        | 1                      | 3                       |
| Techies                 | Chen                    | 3                      | 3                       |
| Skywrath Mage           | Chen                    | 2                      | 3                       |
| Chen                    | Brewmaster              | 3                      | 3                       |
| Chen                    | Bane                    | 1                      | 3                       |
| Bane                    | Chen                    | 1                      | 3                       |
| Brewmaster              | Chen                    | 3                      | 3                       |
| Dark Seer               | Wisp                    | 1                      | 3                       |
| Elder Titan             | Lone Druid              | 3                      | 3                       |
| Shadow Demon            | Visage                  | 2                      | 3                       |
+-------------------------+-------------------------+------------------------+-------------------------+--+
50 rows selected (59.248 seconds)

reason: too less pairs

// enemy
> create external table hero_enemy_win (hero1 string, hero2 string, win boolean);
> insert into hero_enemy_win select a.hero_name, b.hero_name, a.win from hero_name_win_1 a join hero_name_win_1 b on a.match_id = b.match_id and a.team != b.team;
> insert overwrite table hero_buddy_win select * from hero_buddy_win where hero1 != hero2; # remove self buddy
> create external table hero_buddy_count (hero1 string, hero2 string, wins int, games int);
> insert into hero_buddy_count select hero1, hero2, count(case win when true then 1 else null end), count(*) from hero_buddy_win group by hero1, hero2;

> create external table hero_buddy_win_rate (hero1 string, hero2 string, win_rate double);
> insert overwrite table hero_buddy_win_rate select hero1, hero2, wins/CAST(games as double) from hero_buddy_count;


// popularity
total match = 269192
> insert overwrite table hero_popularity select hero_name, count(hero_name)/26919.2 from hero_name_win_1 group by hero_name;
> select * from hero_popularity sort by popularity desc limit 10;

+----------------------------+-----------------------------+--+
| hero_popularity.hero_name  | hero_popularity.popularity  |
+----------------------------+-----------------------------+--+
| Pudge                      | 0.3536880739397902          |
| Invoker                    | 0.3082186692026509          |
| Windranger                 | 0.249635947576451           |
| Juggernaut                 | 0.23340218134268476         |
| Phantom Assassin           | 0.2327706618324467          |
| Legion Commander           | 0.2311361407471247          |
| Sniper                     | 0.2026063181669589          |
| Earthshaker                | 0.18971589051680585         |
| Shadow Fiend               | 0.1767511664536836          |
| Zeus                       | 0.17437368123866978         |
+----------------------------+-----------------------------+--+
10 rows selected (67.345 seconds)



