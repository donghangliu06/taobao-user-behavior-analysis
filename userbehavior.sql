show databases;
use taobao;
create table userbehavior(
user_id VARCHAR(255) not null,
item_id VARCHAR(255) not null,
category_id VARCHAR(255) not null,
behavior varchar(255) not null,
timestamps VARCHAR(255) not null,
PRIMARY KEY(user_id, item_id, timestamps)); 
select COUNT(user_id)
from userbehavior;

##此处用navicat导入了一百万行数据

#将timestamps字段转为日期和时间两个字段
alter table userbehavior add dates date;
alter table userbehavior add times time;
update userbehavior
set dates = FROM_UNIXTIME(timestamps,'%Y-%m-%d');
update userbehavior 
set times = FROM_UNIXTIME(timestamps,'%T');
-- 查看结果
select * from userbehavior
limit 10;

-- 查询时间范围
select min(dates), max(dates)
from userbehavior;

/* 数据获取的描述中表明数据在2017-11-25到2017-12-03之间
我们删去范围外的行 */
delete from userbehavior
where dates < '2017-11-25' or dates > '2017-12-03';
-- 查看总行数
select count(user_id)
from userbehavior;

-- 用户行为转化情况
create view behavior_percent as
select concat(round(sum(case when behavior = 'pv' then 1 else 0 end)/count(behavior)*100,2), '%') as 'pv_ratio',
			 concat(round(sum(case when behavior ='cart' then 1 else 0 end)/count(behavior)*100,2), '%') as 'cart_ratio',
			 concat(round(sum(CASE WHEN behavior = 'fav' then 1 else 0 end)/count(behavior)*100,2), '%') as 'fav_ratio',
			 concat(round(sum(case when behavior ='buy' then 1 else 0 end)/count(behavior)*100,2),'%') as 'buy_ratio'
from userbehavior;

/*用户浏览到购买的转化率低
假设一: 淘宝平台的商品无法满足客户需求
假设二: 不同购买流程的转化率影响了购买量
*/

-- 假设一, 先比较点击量排名前十的商品类型和购买排名前十的商品类型
create view category_pv as
select category_id, count(category_id) as gross_pv
from userbehavior
where behavior = 'pv'
group by category_id
order by gross_pv desc
limit 10;

create view category_buy as
select category_id, count(category_id) as gross_buy
from userbehavior
where behavior = 'buy'
group by category_id
order by gross_buy desc
limit 10;

select a.category_id
from category_pv a
inner join category_buy b
on a.category_id = b.category_id;

-- 考虑点击量前三的商品类型中各前三的商品的购买量
create view 3in3_buy as
select category_id, item_id, gross_buy, row_number() over (partition by category_id order by gross_buy desc) as rank_buy
from
(select category_id, item_id, count(item_id) as gross_buy
from userbehavior
where category_id in (4756105, 4145813, 2355072)
and behavior = 'buy'
group by category_id,item_id) a
order by rank_buy
limit 9;

-- 对比所有商品中排名前十的购买量
create view f10buy as
select category_id, item_id, count(item_id) as gross_buy
from userbehavior
where behavior = 'buy'
group by category_id, item_id
order by gross_buy desc
limit 10;

-- 点击量前十的商品的购买量
create view pvf10_buy  as
select a.category_id, a.item_id, sum(case when a.behavior='buy' then 1 else 0 end) as gross_buy
from userbehavior a
inner join (select category_id, item_id , count(item_id) as gross_pv from userbehavior
						where behavior = 'pv'
						group by category_id, item_id
						order by gross_pv desc
						limit 10) b
on a.category_id = b.category_id
and a.item_id = b.item_id
group by a.category_id, a.item_id
order by gross_buy desc;

show columns from pvf10_buy;
/* 业务流程分析
假设二: 不同购买流程得转化率影响了购买量
*/
create view process as
select user_id, item_id,
sum(case when behavior = 'pv' then 1 else 0 end) as 'pv',
sum(case when behavior = 'fav' then 1 else 0 end) as 'fav',
sum(case when behavior = 'cart' then 1 else 0 end) as 'cart',
sum(case when behavior = 'buy' then 1 else 0 end) as 'buy'
from userbehavior
group by user_id, item_id;
select * from process;

-- 1.各商品点击的用户数之和
select count(user_id)
from process
where pv>0;
-- 1-1.点击后直接购买的用户总数
select count(user_id)
from process
where pv>0 and fav=0 and cart=0 and buy>0;
-- 1-2.点击后直接流失的用户总数
select count(user_id)
from process
where pv>0 and fav=0 and cart=0 and buy=0;
-- 1-3.点击后不收藏加入购物车的用户总数
select count(user_id)
from process
where pv>0 and fav=0 and cart>0;
		-- 1-3-1.加入购物车后购买
		select count(user_id)
		from process
		where pv>0 and fav=0 and cart>0 and buy>0;
		-- 1-3-2.加入购物车后流失
		select count(user_id)
		from process
		where pv>0 and fav=0 and cart>0 and buy=0;
-- 1-4.点击后收藏
select count(user_id)
from process
where pv>0 and fav>0;
		-- 1-4-1.收藏后直接购买
		select count(user_id)
		from process
		where pv>0 and fav>0 and cart=0 and buy>0;
		-- 1-4-2.收藏后流失
		select count(user_id)
		from process
		where pv>0 and fav>0 and cart=0 and buy=0;
		-- 1-4-3.收藏后加入购物车
		select count(user_id)
		from process
		where pv>0 and fav>0 and cart>0;
				-- 1-4-3-1.加入购物车后购买
				select count(user_id)
				from process
			  where pv>0 and fav>0 and cart>0 and buy>0;
				-- 1-4-3-2.加入购物车后流失
				select count(user_id)
				from process
				where pv>0 and fav>0 and cart>0 and buy=0;

/*用户行为与时间分析
	假设三:点击量,用户量在工作日与周末具有差异
假设四:点击量,用户量在不同时段上有差异
*/
create view dau as
select *, pv/uv as 'pv/uv'
from
(select dates, sum(case when behavior='pv' then 1 else 0 end) as pv,
count(distinct user_id) as uv
from userbehavior
group by dates) a
order by dates;

alter table userbehavior
add column hours integer not null;
update userbehavior
set hours = hour(times);

create view hau as
select *, pv/uv as 'pv/uv'
from
(select hours, sum(case when behavior = 'pv' then 1 else 0 end) as pv,
count(distinct user_id) as uv
from userbehavior
group by hours) a
order by hours;

#商品购买分析
-- 分日期分布
create view buy_perday as
select dates, sum(case when behavior='buy' then 1 else 0 end) as buy
from userbehavior
group by dates
order by dates;
-- 分时段分布
create view buy_perhour as
select hours, sum(case when behavior='buy' then 1 else 0 end) as buy
from userbehavior
group by hours
order by hours;
-- 分工作日和周末分析时间段的分布
create view buy_perhour_week as
select hours, sum(case when dates between '2017-11-27' and '2017-12-01' then 1 else 0 end)/5 as weekday_buy,
sum(case when dates between '2017-12-02' and '2017-12-03' then 1 else 0 end)/2 as weekend_buy
from userbehavior
where behavior='buy'
group by hours
order by hours;
-- 用户复购率
select sum(case when buy >=2 then 1 else 0 end)/count(user_id)
from
(select user_id, count(user_id) as buy
from userbehavior
where behavior='buy'
group by user_id) a; 

#留存分析
select concat(round(count(b.user_id)/count(a.user_id)*100, 2),'%') as "次日留存",
concat(round(count(c.user_id)/count(a.user_id)*100, 2),'%') as "三日留存",
concat(round(count(d.user_id)/count(a.user_id)*100, 2),'%') as "七日留存"
from (select distinct user_id from userbehavior where dates='2017-11-25') a
left join (select distinct user_id from userbehavior where dates='2017-11-26') b
on a.user_id=b.user_id
left join (select distinct user_id from userbehavior where dates='2017-11-28') c
on a.user_id=c.user_id
left join (select distinct user_id from userbehavior where dates='2017-12-2') d
on a.user_id=d.user_id;



#客户价值的RFM分析
create view  RF as 
select user_id, datediff('2017-12-03', max(dates)) as R,
count(behavior) as F
from userbehavior
where behavior='buy'
group by user_id;

select min(R), max(R), min(F), max(F)
from RF;

create view RF_score as
select user_id,
(case when R between 0 and 2 then 4
			when R between 3 and 4 then 3
			when R between 5 and 6 then 2
			else 1 end) as R_score,
(case when F between 1 and 6 then 1
			when F between 7 and 12 then 2
			when F between 13 and 18 then 3
			else 4 end) as F_score
from RF;	
select avg(R_score), avg(F_score)
from RF_score;

create view user_category as 
select user_id,
(case when R_score >=(select avg(R_score) from RF_score) and F_score >= (select avg(F_score) from RF_score) then "重要价值客户" 
when R_score >=(select avg(R_score) from RF_score) and F_score < (select avg(F_score) from RF_score) then "重要发展客户"
when R_score <(select avg(R_score) from RF_score) and F_score >= (select avg(F_score) from RF_score) then "重要保持客户"
else "重要挽留客户" end) as user_category
from RF_score;

select user_category, concat(round(count(user_category)/(select count(user_category) from user_category) * 100, 2), '%') as percent
from user_category
group by user_category;

show VARIABLES like 'port';

