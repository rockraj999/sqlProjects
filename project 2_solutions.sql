

 -- In this Project i have used complex SQL queries and analysis of data. which will answer how 
 -- the Olympics have evolved over time, including participation and performance of women, different nations , and 
 -- different sports and events. The objective was to showcase my proficiency in SQL.


--   Data Collection and Storage:-

--  i have used  historical dataset of the Olympic Games, including all the Games from 1896 to 2016.
--  I have taken this data from Kaggle to do analysis using SQL. Note that the Winter and 
--  Summer Games were held in the same year up until 1992. After that, they staggered them such that Winter Games occur on a
--  four year cycle starting with 1994, then Summer in 1996, then Winter in 1998 and so on.


-- Data exploration:

--  This athlete_events dataset contains details of athlete_id, win or not, year, season and events.
--  athletes dataset contain data about each athletes their id, name,gender,  nationality, height, team.
--  in athletes dataset id is the primary key and athlete_id in athlete_events table is foreign key.


-- Data Analysis:






--1 which team has won the maximum gold medals over the years.

select top 1  team,count(distinct event) as cnt from athlete_events ae
inner join athletes a on ae.athlete_id=a.id
where medal='Gold'
group by team
order by cnt desc;



--2 for each team print total silver medals and year in which they won maximum silver medal..output 3 columns
-- team,total_silver_medals, year_of_max_silver

with cte as (
select a.team,ae.year , count(distinct event) as silver_medals
,rank() over(partition by team order by count(distinct event) desc) as rn
from athlete_events ae
inner join athletes a on ae.athlete_id=a.id
where medal='Silver'
group by a.team,ae.year)
select team,sum(silver_medals) as total_silver_medals, max(case when rn=1 then year end) as  year_of_max_silver
from cte
group by team;



--3 which player has won maximum gold medals  amongst the players 
--which have won only gold medal (never won silver or bronze) over the years

with cte as (
select name,medal
from athlete_events ae
inner join athletes a on ae.athlete_id=a.id)
select top 1 name , count(1) as no_of_gold_medals
from cte 
where name not in (select distinct name from cte where medal in ('Silver','Bronze'))
and medal='Gold'
group by name
order by no_of_gold_medals desc;



--4 in each year which player has won maximum gold medal . Write a query to print year,player name 
--and no of golds won in that year . In case of a tie print comma separated player names.

with cte as (
select ae.year,a.name,count(1) as no_of_gold
from athlete_events ae
inner join athletes a on ae.athlete_id=a.id
where medal='Gold'
group by ae.year,a.name)
select year,no_of_gold,STRING_AGG(name,',') as players from (
select *,
rank() over(partition by year order by no_of_gold desc) as rn
from cte) a where rn=1
group by year,no_of_gold;



--5 in which event and year India has won its first gold medal,first silver medal and first bronze medal
--print 3 columns medal,year,sport

select distinct * from (
select medal,year,event,rank() over(partition by medal order by year) rn
from athlete_events ae
inner join athletes a on ae.athlete_id=a.id
where team='India' and medal != 'NA'
) A
where rn=1;


--6 find players who won gold medal in summer and winter olympics both.

select name,year,count(distinct season) as times_year
from athlete_events ae join athletes a on a.id=ae.athlete_id
where medal='Gold'
group by name,year
having count(distinct season)>1;



--7 find players who won gold, silver and bronze medal in a single olympics. print player name along with year.

select year,name
from athlete_events ae
inner join athletes a on ae.athlete_id=a.id
where medal != 'NA'
group by year,name having count(distinct medal)=3;



--8 find players who have won gold medals in consecutive 3 summer olympics in the same event . Consider only olympics 2000 onwards. 
--Assume summer olympics happens every 4 year starting 2000. print player name and event name.

with cte_1 as (select name,event,year
from athlete_events ae join athletes a on a.id=ae.athlete_id
where year>=2000 and season='Summer' and medal='Gold')
,cte_con as (select name,year,1 as medals
from cte_1 group by name,year )
select a.name from (select *,sum(medals) over(partition by name order by year) as tot_medal
from cte_con) a 
where tot_medal=3;





