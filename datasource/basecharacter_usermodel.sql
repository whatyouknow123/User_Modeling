/* there are some query about the usermodel*/
/* use the usermodel database*/
use usermodel;

/*select the top data from the basic table*/
select * from basic_table limit 2;

# the start time and end time of the data
select FROM_UNIXTIME(submitTime,'%Y年%m月%d %H时:%i分:%s秒') from basic_table order by submitTime desc limit 1;
select FROM_UNIXTIME(submmiTime,'%Y年%m月%d %H时:%i分:%s秒') from basic_table order by submitTime asc limit 1;

# create view which record the basic info of user
drop view user_jobs;
create view user_jobs as select userId, count(*) as jobs_num from basic_table group by userId;
select * from user_jobs;
select jobs_num, count(*) as num from user_jobs group by jobs_num desc;

select * from user_jobs order by jobs_num  asc limit 234;

# create view which record the basic info of status
drop view user_jsatus;
create view user_jstatus as select userId, jStatus, count(*) as num from basic_table group by userId, jStatus;
select * from user_jstatus;
select a.userId, a.num , b.num from user_jstatus as a join user_jstatus as b on a.userId = b.userId and a.jStatus = 32 and b.jStatus= 64;
 
select * from basic_table limit 1;

alter table basic_table add exetime int;

# calculate the exetime
SET SQL_SAFE_UPDATES = 0;
update basic_table set exetime = event_time - starttime;
update basic_table set exetime = event_time - submittime where starttime < submittime;
select exetime from basic_table;
select count(*) from basic_table where exetime < 3600;

# create view about the short, middle and long task 
create view user_task as select userid, sum(if(exetime<=3600, 1, 0)) as short_num , sum(if(exetime>86400, 1, 0)) as long_number, sum(if(exetime<=86400&&exetime>3600, 1, 0)) as middle from basic_table group by userid;
select * from user_task;

# create view about the morning, noon and night task number
drop view user_daily;
create view user_daily as select userid, sum(if(from_unixtime(submittime, "%k")<=14&&from_unixtime(submittime, "%k")>=6, 1, 0)) as morning, sum(if(from_unixtime(submittime, "%k")<=22&&from_unixtime(submittime, "%k")>14, 1, 0))
as noon, sum(if(from_unixtime(submittime, "%k")<6&&from_unixtime(submittime, "%k")>=0 or from_unixtime(submittime, "%k")<=24&&from_unixtime(submittime, "%k")>22, 1, 0)) as night from basic_table group by userid;
select * from user_daily;

select sum(if(from_unixtime(submittime, "%k")<=14&&from_unixtime(submittime, "%k")>=6, 1, 0)) as morning, sum(if(from_unixtime(submittime, "%k")<=22&&from_unixtime(submittime, "%k")>14, 1, 0))
as noon, sum(if(from_unixtime(submittime, "%k")<6&&from_unixtime(submittime, "%k")>=0 or from_unixtime(submittime, "%k")<=24&&from_unixtime(submittime, "%k")>22, 1, 0)) as night from basic_table where exetime >86400;

select userid, sum(if(from_unixtime(submittime, "%k")<=14&&from_unixtime(submittime, "%k")>=6, 1, 0)) as morning, sum(if(from_unixtime(submittime, "%k")<=22&&from_unixtime(submittime, "%k")>14, 1, 0))
as noon, sum(if(from_unixtime(submittime, "%k")<6&&from_unixtime(submittime, "%k")>=0 or from_unixtime(submittime, "%k")<=24&&from_unixtime(submittime, "%k")>22, 1, 0)) as night from basic_table where exetime < 3600 group by userid;

#create view about the weekend and wrokday
create view user_week as select userid, sum(if(from_unixtime(submittime, "%w")=0 or from_unixtime(submittime, "%w")=6,1,0)) as weekend, sum(if(from_unixtime(submittime, "%w")>0 and from_unixtime(submittime, "%w")<6, 1,0)) as workday from basic_table group by userId;
select * from user_week;

select userid, sum(if(from_unixtime(submittime, "%w")=0 or from_unixtime(submittime, "%w")=6,1,0)) as weekend, sum(if(from_unixtime(submittime, "%w")>0 and from_unixtime(submittime, "%w")<6, 1,0)) as workday from basic_table where exetime > 86400 group by userId;


# find the new user arrive time
drop view user_arrive;
create view user_arrive as select userid, from_unixtime(submittime,"%Y-%m-%d") as arrive_time from basic_table group by userid order by submittime;
select * from user_arrive;

# create view record the number of task in 24 hour in a day for every user
select userid, from_unixtime(submittime, "%k") as date_hour, count(*) from basic_table group by userid, from_unixtime(submittime, "%k");
create table user_day_hour(userid int primary key,
h0 int,
h1 int,
h2 int,
h3 int,
h4 int,
h5 int,
h6 int,
h7 int,
h8 int,
h9 int,
h10 int,
h11 int,
h12 int,
h13 int,
h14 int,
h15 int,
h16 int,
h17 int,
h18 int,
h19 int,
h20 int,
h21 int,
h22 int,
h23 int);
select * from user_day_hour;

# create table user_week_num record which record the num of work  in every day of week for each user
create view user_week_view as select userid, from_unixtime(submittime, "%w") as date_week, count(*) as week_num from basic_table group by userid, from_unixtime(submittime, "%w");
select * from user_week_view;
create table user_week_day(userid int primary key,
mon int,
tue int,
wen int,
thr int,
fri int,
sta int,
sun int);
select * from user_week_day;

select from_unixtime(submittime, "%Y-%M-%d %h"), count(*) from basic_table where userid= 1228  and from_unixtime(submittime, "%w")=4 group by from_unixtime(submittime, "%Y-%M-%d %h");
select from_unixtime(submittime, "%w"), count(*) from basic_table where userid= 1228  group by from_unixtime(submittime, "%w");

create view user_each_hour as select userid, from_unixtime(submittime, "%Y-%m-%d %h"), count(*) as batch_number from basic_table group by userid, from_unixtime(submittime, "%Y-%m-%d %h");
select sum(batch_number) from user_each_hour where batch_number <= 100;
select count(*) from user_each_hour where batch_number >100;