
 create table if not exists student(
 sno int ,
 sname string,
 ssex string,
 sbirthday string,
 class string
 )
 ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';



 create table if not exists course(
 cno string,
 cname string,
 tno int
 )
 row format delimited fields terminated by ',';


  load data local inpath '/homework/h3.csv' into table  course

 create table if not  exists score(
 sno int,
 cno string,
 degree int
 )
 row format delimited fields terminated by ',';

 drop table zoo.score;

  load data local inpath '/homework/h2.csv' into table  score

 create table if not exists teacher(
 tno int,
 tname string,
 tsex string,
 tbirthday string,
 prof string,
 depart string
 )
 row format delimited fields terminated by ',';

   load data local inpath '/homework/h4.csv' into table  teacher;

-- 1、 查询Student表中的所有记录的Sname、Ssex和Class列。
select sname,ssex,class
from student;

-- 2、 查询教师所有的单位即不重复的Depart列。
select distinct depart
from zoo.teacher;
-- 3、 查询Student表的所有记录。
select *
from zoo.student
-- 4、 查询Score表中成绩在60到80之间的所有记录。
select score.degree
from zoo.score
where degree>60 and degree<80;
-- 5、 查询Score表中成绩为85，86或88的记录。
select score.degree
from zoo.score
where degree=85 or degree=86 or degree=88;
-- 6、 查询Student表中“95031”班或性别为“女”的同学记录。
select *
from zoo.student
where ssex='女' or class='95031';
-- 7、 以Class降序查询Student表的所有记录。
select *
from zoo.student
order by class desc ;
-- 8、 以Cno升序、Degree降序查询Score表的所有记录。
select *
from  zoo.score
order by cno asc ,degree desc ;
-- 9、 查询“95031”班的学生人数。
select sname, count(*) over()
from zoo.student
where  class='95031'

-- 10、查询Score表中的最高分的学生学号和课程号。
select *
from zoo.score as s2
where s2.degree  in (
select  max(score.degree)
from  zoo.score );
-- 11、查询‘3-105’号课程的平均分。
select  score.cno,avg(score.degree)
from zoo.score
inner join zoo.course on score.cno=course.cno
where course.cno='3-105'
group by score.cno;
-- 12、查询Score表中至少有5名学生选修的并以3开头的课程的平均分数。
select  score.cno,avg(score.degree)
from zoo.score
inner join zoo.course on score.cno=course.cno
where course.cno like '3%'
group by score.cno;
-- 13、查询最低分大于70，最高分小于90的Sno列。
select score.degree
from zoo.score
where degree>70 and degree<90
-- 14、查询所有学生的Sname、Cno和Degree列。
select sname,cno,degree
from zoo.score
inner join zoo.student on score.sno=student.sno
-- 15、查询所有学生的Sno、Cname和Degree列。
select sno,cname,degree
from zoo.score
inner join zoo.course on score.cno=course.cno

-- 16、查询所有学生的Sname、Cname和Degree列。
select sname,cname,degree
from zoo.score
inner join zoo.course on score.cno=course.cno
inner join zoo.student on score.sno=student.sno

-- 17、查询“95033”班所选课程的平均分。
select  avg(degree)
from zoo.score
inner join zoo.student on score.sno=student.sno
where class='95033'
-- 18、假设使用如下命令建立了一个grade表：

create table grade(low   number(3,0),upp  number(3),rank   char(1));
insert into grade values(90,100,'A');
insert into grade values(80,89,'B');
insert into grade values(70,79,'C');
insert into grade values(60,69,'D');
insert into grade values(0,59,'E');

SHOW FUNCTIONS


create table if not exists mingxing_favor(
id int,
name string,
age int,
favor string
)
row format delimited fields terminated by ' ';

load data local inpath '/homework/test1.csv' into table mingxing_favor

select name,max (age)
from mingxing_favor
group by name;

-- select collect_set(tt2.name)
-- from (
select name,age, hobby,
max(age) over(partition by tt.hobby) as max,
dense_rank()  over(partition by tt.hobby order by age desc ) as pp
from (select id ,name,age,hobby from mingxing_favor lateral view explode(split(favor,'[,]') ) e as hobby)as tt)as tt2
 group by tt2.hobby,tt2.pp;


 create table if not exists score2 (
 id int,
 username string,
 math int,
 computer int,
 english int
 )row format delimited fields terminated by ',';

load data local inpath '/homework/test2.csv' into table score2;

insert overwrite local directory '/mm' select * from score2



nam
e,m_class,m_score from  view explode(mymap) mymap as course,score

select  username,course,score
from (select username,map('math',math,'computer',computer,'english',english) as mymap from score2)as tmp lateral view explode(mymap) mymap as course,score;



add jar /hivedata/Maven2.jar

show functions

create temporary function toUP as 'com.qf.hive.FirstUDF'