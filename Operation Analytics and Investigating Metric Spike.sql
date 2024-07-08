create database job_data;
use job_data;

create table job_data(
job_id int,
actor_id int,
event varchar(50),
language varchar(50),
time_spent int,
org char(1),
ds date);
	
select * from job_data; 
	
insert into job_data (ds, job_id, actor_id, event, language, time_spent, org) values
('2020-11-30', 21, 1001, 'skip', 'English', 15, 'A'),
('2020-11-30', 22, 1006, 'transfer', 'Arabic', 25, 'B'),
('2020-11-29', 23, 1003, 'decision', 'Persian', 20, 'C'),
('2020-11-28', 23, 1005,'transfer', 'Persian', 22, 'D'),
('2020-11-28', 25, 1002, 'decision', 'Hindi', 11, 'B'),
('2020-11-27', 11, 1007, 'decision', 'French', 104, 'D'),
('2020-11-26', 23, 1004, 'skip', 'Persian', 56, 'A'),
('2020-11-25', 20, 1003, 'transfer', 'Italian', 45, 'C');

#A
select ds, count(job_id),sum(time_spent),
(count(job_id)*60*60/sum(time_spent)) as "Jobs Reviewed per Hour" 
from job_data
where ds between '2020-11-01' and '2020-11-30' 
group by ds
order by ds;

#B
select count(event),sum(time_spent),
round((count(event)/sum(time_spent)),3) as "7 day rolling throughput "
from job_data;

select ds, round(count(event)/sum(time_spent), 3) as "Daily matric throughput"
from job_data
group by ds
order by ds;

#C
select language, round(count(*)*100/8,3) as percentage
from job_data
group by language;

#D 
select job_id, count(job_id) as repeated
from job_data
group by job_id
having count(job_id)>1;

select actor_id, count(actor_id) as repeated
from job_data
group by actor_id
having count(actor_id)>1;



create table users(
user_id int,
created_at timestamp,
company_id int,
language varchar(50),
activated_at timestamp,
state varchar(50));

drop table users;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Table-1 users.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Table-1 users.csv'
INTO TABLE users
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(user_id, created_at, company_id, language, @activated_at, state)
SET activated_at = NULLIF(@activated_at, '');

select *from users;

create table events(
user_id int,
occurred_at timestamp,
event_type varchar(50),
event_name varchar(50),
location varchar(50),
device varchar(150),
user_type varchar(50));

select *from events;

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Table-2 events.csv'
INTO TABLE events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

create table email_events(
user_id int,
occurred_at timestamp,
action varchar(50),
user_type int);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Table-3 email_events.csv'
INTO TABLE email_events
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

select *from email_events;

#A User Engagement
select count(distinct user_id) as no_of_user, extract(week from occurred_at) as weeks from events
where event_type = 'engagement'
group by weeks;

#B User Growth
select Years,Weeks, Users,
Users-LAG(Users, 1) OVER (ORDER BY Years) 
AS "Growth by comparative weekly",
sum(Users) over(order by Years,Weeks rows between unbounded preceding and current row)
as "Cumulative Growth"
from(
select extract(year from created_at) as Years,
extract(week from created_at) as Weeks,count(activated_at) AS Users
from users
where state='active' 
group by Years,Weeks
order by Years, Weeks)a;


#C Weekly Retention
select count(user_id), first_retention_week from(
select user_id,count(user_id) as no_of_times_retained, 
MIN(retention_week) AS first_retention_week, max(retention_week) AS last_retention_week
FROM (
select a.user_id,
       a.sign_up_week,
       b.engagement_week,
       b.engagement_week - a.sign_up_week as retention_week
from(
(select distinct user_id, extract(week from occurred_at) as sign_up_week
from events
where event_type = 'signup_flow'
and event_name = 'complete_signup')a 
left join
(select distinct user_id, extract(week from occurred_at) as engagement_week
from events
where event_type = 'engagement')b
on a.user_id = b.user_id)
where b.engagement_week - a.sign_up_week >= 1) c
GROUP BY user_id)m
group by first_retention_week;

#C Weekly Retention
select retention_week, count(user_id) as no_of_users_retained from(
select a.user_id,
       a.sign_up_week,
       b.engagement_week,
       b.engagement_week - a.sign_up_week as retention_week
from(
(select distinct user_id, extract(week from occurred_at) as sign_up_week
from events
where event_type = 'signup_flow'
and event_name = 'complete_signup')a 
left join
(select distinct user_id, extract(week from occurred_at) as engagement_week
from events
where event_type = 'engagement')b
on a.user_id = b.user_id)
where b.engagement_week - a.sign_up_week >= 1)c
group by retention_week;

#D Weekly Engagement
select 
extract(year from occurred_at) as years,
extract(week from occurred_at) as weeks,
device,
count(distinct user_id) as no_of_users
from events
where event_type = 'engagement'
group by years,weeks,device
order by years,weeks;

#E Email Engagement
select weeks, sent_weekly_digest*100/total as 'Weekly_digest',
email_open*100/total as 'Weekly_email_open',
email_clickthrough*100/total as'Weekly_clickthrough',
sent_reengagement_email*100/total as'Weekly_reengagement'
from(
select extract(week from occurred_at)as weeks,count(user_id)as total,
sum(case when action ='sent_weekly_digest' then 1 else 0 end) as sent_weekly_digest,
sum(case when action ='email_open' then 1 else 0 end) as email_open,
sum(case when action ='email_clickthrough' then 1 else 0 end) as email_clickthrough,
sum(case when action ='sent_reengagement_email' then 1 else 0 end) as sent_reengagement_email
from email_events
group by weeks)a
group by weeks
order by weeks;


select *from events;
select distinct device from events;
select extract(week from occurred_at)as weeks,
count(distinct case when device ='acer aspire desktop' then user_id else null end) as acer_aspire_desktop,
count(distinct case when device ='acer aspire notebook' then user_id else null end) as acer_aspire_notebook,
count(distinct case when device ='amazon fire phone' then user_id else null end) as amazon_fire_phone,
count(distinct case when device ='asus chromebook' then user_id else null end) as asus_chromebook,
count(distinct case when device ='dell inspiron desktop' then user_id else null end) as dell_inspiron_desktop,
count(distinct case when device ='dell inspiron notebook' then user_id else null end) as dell_inspiron_notebook,
count(distinct case when device ='hp pavilion desktop' then user_id else null end) as hp_pavilion_desktop,
count(distinct case when device ='htc one' then user_id else null end) as htc_one,
count(distinct case when device ='ipad air' then user_id else null end) as ipad_air,
count(distinct case when device ='ipad mini' then user_id else null end) as ipad_mini,
count(distinct case when device ='iphone 4s' then user_id else null end) as iphone_4s,
count(distinct case when device ='iphone 5' then user_id else null end) as iphone_5,
count(distinct case when device ='iphone 5s' then user_id else null end) as iphone_5s,
count(distinct case when device ='kindle fire' then user_id else null end) as kindle_fire,
count(distinct case when device ='lenovo thinkpad' then user_id else null end) as lenovo_thinkpad,
count(distinct case when device ='mac mini' then user_id else null end) as mac_mini,
count(distinct case when device ='macbook air' then user_id else null end) as macbook_air,
count(distinct case when device ='macbook pro' then user_id else null end) as macbook_pro,
count(distinct case when device ='nexus 10' then user_id else null end) as nexus_10,
count(distinct case when device ='nexus 5' then user_id else null end) as nexus_5,
count(distinct case when device ='nexus 7' then user_id else null end) as nexus_7,
count(distinct case when device ='nokia lumia 635' then user_id else null end) as nokia_lumia_635,
count(distinct case when device ='samsumg galaxy tablet' then user_id else null end) as samsumg_galaxy_tablet,
count(distinct case when device ='samsung galaxy note' then user_id else null end) as samsung_galaxy_note,
count(distinct case when device ='samsung galaxy s4' then user_id else null end) as samsung_galaxy_s4,
count(distinct case when device ='windows surface' then user_id else null end) as windows_surface
from events
group by weeks;

xc