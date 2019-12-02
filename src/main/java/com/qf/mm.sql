
create table if not exists qfuser2(
id int,
name string
)
row format delimited fields terminated by ','
lines terminated by '\n'
stored as textfile
;

show databases;

use  default
;
load data local inpath '/hivedata/user.csv' into table qfuser

create table qfusernew(
id int,
name string
) comment 'this is table'
row format delimited fields terminated by ','
;

insert into qfusernew
select * from qfuser where  id <3
;

-- 带表数据克隆
create table t7 as  select  * from qfusernew;

create table if not exists t5 like qfusernew location
'/user/hive/warehouse/zoo.db/qfusernew';

-- 不带表数据克隆
create table if not exists qfuserold like  qfusernew;

-- 查看表信息
show create table t7;
describe extended t7;

Desc formatted part5

show create table part5


create table if not exists part1(id int ,name string)
partitioned by (dt string)
row format delimited fields terminated by ",";

load data local inpath '/hivedata/user.csv' into table part1 partition(dt='2019-11-11')
load data local inpath '/hivedata/user.csv' into table part1 partition(dt='2019-6-18')

SELECT * FROM part1 WHERE dt='2019-11-11';

create table if not exists part3(id int ,name string)
partitioned by (year string,mouth string,day string)
row format delimited fields terminated by ",";

load data local inpath '/hivedata/user.csv' into table part3 partition(year='2019',mouth='11',day='11')

select * from part3
;

show partitions part3

-- 新增分区part5
create table if not exists part5(
   id  int ,
   name string
) partitioned by (dt string) row format delimited fields terminated by ",";


alter table  part5 add partition(dt='2010-1-2') partition (dt='2011-1-3')
;

-- 修改分区的位置
alter table part5 partition(dt='2010-1-2') set location 'hdfs://hadoop1:8020//user/hive/warehouse/part1/dt=2019-11-11'
select * from part5

alter table part5 drop partition(dt='2010-1-2');

alter table part5 drop partition(dt='2010-1-1'),partition(dt='2011-1-3')

alter table  part5 add partition(dt='2010-1-1')
alter table part1 drop partition(dt='2019-6-18');
load data local inpath '/hivedata/user.csv' into table part5 partition(dt='2010-1-2')

set hive.exec.dynamic.partition=true
set hive.exec.dynamic.partition.mode=nonstrict
set hive.exec.max.dynamic.partitions=1000
set hive.exec.max.dynamic.partitions.pernode=100


create table  dy_part1(
id int,
name string
)partitioned by (dt string)
row format delimited fields terminated by ','

create table tmp_part1(
id int,
name string,
dt string
)
row format delimited fields terminated by ',';

load data local inpath '/hivedata/user.csv' into table tmp_part1

insert into  dy_part1 partition(dt) select id,name,dt from tmp_part1

-- 创建混合分区表
create table  dy_part2(
id int ,
name string
)
partitioned by (year string,month string,day string)
row format delimited fields terminated by ',';

-- 创建混合分区 临时表
create table  tmp_part2(
id int,
name string,
year string,
month string,
day string
)
row format delimited fields terminated by ',';

load data local inpath  '/hivedata/user.csv' into table tmp_part2

insert into  dy_part2 partition(year='2019',month ,day) select id,name,month,day from tmp_part2

create table if not exists  buck3(
id int,
name string,
age int
)clustered by (id) into 4 buckets
row format delimited fields terminated by ','
;

create table if not exists  tmp_buck3(
id int,
name string,
age int
)row format delimited fields terminated by ',';


load data local inpath '/hivedata/user2.csv' into table tmp_buck3

set mapreduce.job.reduces=4
set hive.enforce.bucketing=true
insert overwrite table buck3 select  id,name,age from  tmp_buck3 cluster by (id)




;
use default;

create external table if not exists hbase2hive(id int,name string) stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' with serdeproperties("hbase.columns.mapping"=":key,f1:name") tblproperties("hbase.table.name"="nsq:alj");

use mydb;

create table student(sid string,sname string,sage int,ssex string) row format delimited fields terminated by ' ';

create table course(cid string,cname string,tid string) row format delimited fields terminated by ' ';

create table teacher(tid string,tname string) row format delimited fields terminated by ' ';

create table sc(sid string,cid string,score int) row format delimited fields terminated by ' ';

select * from  student;

drop table  student;
drop table  course;
drop table teacher;
drop table sc;



load data local inpath '/hivedata/student' into table sc

select * from student;

select sname,cname,degree
from mydb.sc
inner join my.sc on sc.cid=course.cid
inner join my.sc on sc.sid=student.sid
where ;


select * from student;

//开启本地模式
set hive.exec.mode.local.auto=true

FROM
<left_table>
ON
<join_condition>
<join_type>
JOIN
<right_table>
WHERE
<where_condition>
GROUP BY
<group_by_list>
HAVING
<having_condition>
SELECT
DISTINCT
<select_list>
ORDER BY
<order_by_condition>
LIMIT
<limit_number>
-- 1.	查询“某1”课程比“某2”课程成绩高的所有学生的学号
select * from sc
SELECT * FROM sc ts1, sc ts2
WHERE ts1.sid = ts2.sid AND ts1.cid = 01 AND ts2.cid = 02 AND ts1.score > ts2.score;


select * from sc;


select *
from sc as  sc1,
inner join sc as sc2 on sc1.sid=sc2.sid
where sc1.cid=01 and sc2.cid =02 and  sc1.score>sc2.score;

-- 2.	查询平均成绩大于60分的同学的学号和平均成绩；
--
--select sid,avg (score)
from mydb.sc
inner join mydb.student on sc.sid=student.sid
group by  student.sid
having avg (score)>60

-- 3.	查询所有同学的学号、姓名、选课数、总成绩
--

select * from student

select student.sid,student.sname,sctmp.con,sctmp.ag
from
(
SELECT sid,count (sid) as con,avg (score) as ag
from sc
group by sid) as sctmp
inner join student on student.sid=sctmp.sid





-- 4.	查询姓“李”的老师的个数
select count (tid)
from teacher
where tname like '李%'

-- 5.	查询没学过“张三”老师课的同学的学号、姓名







SELECT student.sid,sname
 FROM student
  WHERE student.Sid NOT IN
   (
    SELECT sc.sid  FROM  sc
      WHERE sc.cid=01
   );

-- 6.	查询学过数学并且也学过编号语文课程的同学的学号、姓名




select sid,sname
from student
where sid in(
select sc1.sid
from (
select sc.sid
from mydb.sc
where sc.cid=01
)as sc1,(
select sc.sid
from mydb.sc
where sc.cid=02
)as  sc2
where sc1.sid=sc2.sid
);


-- inner join  on  后面加and相当于where
-- in 就是判断在结果表里面吗？
-- not in 刚好相反
--但是inner join on 也可以代替in 因为它是连接 所以在表里的当然会连接  但是记得要把表 as其别名
select
a.*,
b.cid,
b.score,
c.cid,
c.score
from student a
join sc b on a.sid=b.sid and b.cid=01
join sc c on a.sid=c.sid and c.cid=02





-- 7.	查询学过“张三”老师所教的所有课的同学的学号、姓名
select student.sid,student.sname
from mydb.sc
inner join mydb.student on sc.sid=student.sid
where sc.cid=02;


--
-- 8.	查询课程编号“01”的成绩比课程编号“02”课程低的所有同学的学号、姓名
--


SELECT * FROM sc ts1, sc ts2
WHERE ts1.sid = ts2.sid AND ts1.cid = 01 AND ts2.cid = 02 AND ts1.score <ts2.score;
-- 9.	查询所有课程成绩小于60分的同学的学号、姓名
select student.sid,student.sname,sctmp.max
from
(
select sc.sid,max (score) as max
from mydb.sc
group by sc.sid
having max (sc.score)<60) as sctmp
inner join student on student.sid=sctmp.sid;


-- 10.	查询没有学全所有课的同学的学号、姓名

select student.sid,sname
from student
where student.sid not in (

select a.sid
from sc
inner join sc a on  a.sid=sc.sid and a.cid=01
inner join sc b on  b.sid=sc.sid and b.cid=02
inner join sc c on  c.sid=sc.sid and c.cid=03)

--
--
-- 11.	查询至少有一门课与学号为“01”的同学所学相同的同学的学号和姓名

select  student.sid,student.sname
from
(
select distinct sc.sid
from
(select cid as tt
from sc
where sc.sid=01) as tmp
inner join sc on sc.cid=tmp.tt) as tt2
inner join student on student.sid=tt2.sid;




-- 12.	查询和"01"号的同学学习的课程完全相同的其他同学的学号和姓名

from (
select sc.cid
from mydb.sc
where sc.sid=01) as tmp
inner join sc a on  a.sid= sc.sid and a.cid=sc.cid
inner join sc b on  b.sid=sc.sid and b.cid=sc.cid

-- 13.	查询两门及其以上不及格课程的同学的学号，姓名及其平均成绩
select  * from sc;
select student.sid,student.sname,mm.sco
from (
select sid,count(*) as con,avg (score) as sco
from sc
where score<60
group by sid) as mm
left join student on student.sid=mm.sid
-- 14.	检索"01"课程分数小于60，按分数降序排列的学生信息
select *
from mydb.sc
inner join mydb.student on sc.sid=student.sid
where sc.cid=01 and sc.score<60
order by sc.score desc;
-- 15.	查询各科成绩最高分、最低分和平均分：以如下形式显示：课程ID，课程name，最高分，最低分，平均分，及格率，中等率，优良率，优秀率

a.cid,b.cname,MAX(score),MIN(score),ROUND(AVG(score),2),
    ROUND(100*(SUM(case when a.score>=60 then 1 else 0 end)/SUM(case when a.score then 1 else 0 end)),2) as 及格率,
    ROUND(100*(SUM(case when a.score>=70 and a.score<=80 then 1 else 0 end)/SUM(case when a.score then 1 else 0 end)),2) as 中等率,
    ROUND(100*(SUM(case when a.score>=80 and a.score<=90 then 1 else 0 end)/SUM(case when a.score then 1 else 0 end)),2) as 优良率,
    ROUND(100*(SUM(case when a.score>=90 then 1 else 0 end)/SUM(case when a.score then 1 else 0 end)),2) as 优秀率
    from score a left join course b on a.c_id = b.c_id GROUP BY a.cid,b.cname



-- 16.	查询学生的总成绩并进行排名
select sid,sum(score)as mm
from sc
group by sid
order by mm


-- 17.	按平均成绩从高到低显示所有学生的所有课程的成绩以及平均成绩
select  sc.sid,sc.score,tmp.mm
from sc
inner join (
select   sid,avg(score) mm
from sc
group by sid) as tmp on tmp.sid=sc.sid


-- 18.	查询不同老师所教不同课程平均分从高到低显示
select course.tid,course.cname,mm.yy
from course
inner join (
select course.tid,avg (score) as yy
from course
inner join  sc on sc.cid=course.cid
inner  join teacher on teacher.tid=course.cid
group by course.tid) as mm on mm.tid=course.tid


select * from course
select * from  teacher

-- 19.	查询每门课程被选修的学生数

select cid,count(*)
from  sc
group by cid

--
-- 20.	查询出只选修了一门课程的全部学生的学号和姓名
select student.sid,student.sname
from student
INNER JOIN (
select sid,count (*) as mm
from sc
group by sid) as mm on mm.sid= student.sid
where mm=1
-- 21.	查询男生、女生人数
select * from student
select ssex,count (1)
from  student
group by ssex
--
-- 22.	查询名字中含有"风"字的学生信息
select student.sname
from student
where student.sname like '%风%'
-- 23.	查询同名同性学生名单，并统计同名人数

select tmpA.ssex,tmpA.sname,tmpC
from(
select student.ssex,sname,count(1) as tmpC
from student
group by ssex,sname
) as tmpA
where  tmpC>1
-- 24.	查询1990年出生的学生名单
select *
from student
where sage like  '1990%'
-- 25.	查询每门课程的平均成绩，结果按平均成绩升序排列，平均成绩相同时，按课程号降序排列

select  cid,avg (score) as tmpa
from sc
group by cid
order by  tmpa,cid desc
-- 26.	查询不及格的课程，并按课程号从大到小排列
select cid,score
from sc
where score<60
order by cid desc
-- 27.	查询每门功课成绩最好的前两名

select *
from (select cid,score,row_number() over (distribute by cid sort by score desc) rn
from sc
) a
where a.rn < 3;

-- 28.	统计每门课程的学生选修人数（超过5人的课程才统计）。要求输出课程号和选修人数，查询结果按人数降序排列，若人数相同，按课程号升序排列
select course.cid,cname,tmpT.tmpC
from (select cid,count (1) tmpC
from sc
group by sc.cid) as tmpT
inner join course on course.cid=tmpT.cid
where tmpT.tmpC>5
-- 29.	检索至少选修两门课程的学生学号

select DISTINCT sid
from (select sid,score,row_number() over (distribute by sid sort by cid desc) rn
from sc
) a
where a.rn >1;
-- 30.	查询选修了全部课程的学生信息
select DISTINCT sid
from (select sid,score,row_number() over (distribute by sid sort by cid desc) rn
from sc
) a
where a.rn >2;










