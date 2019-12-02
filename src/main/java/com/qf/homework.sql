create table if not exists Student(
s_id string,
s_name string,
s_birth string,
s_sex string
)row format delimited fields terminated by ',';

drop table  student;

load data local inpath '/hivedata/data1.csv' into table Student
create table if not exists Course(
c_id string,
c_name string,
t_id string
)row format delimited fields terminated by ',';

load data local inpath '/hivedata/data2.csv' into table Course

create table if not exists Teacher(
t_id string,
t_name string
)row format delimited fields terminated by ',';

load data  local inpath '/hivedata/data3.csv' into table Teacher
create table if not exists Score(
s_id string,
c_id string,
s_score string
) row format delimited fields terminated by ',';

load data  local inpath '/hivedata/data4.csv' into table Score;

select student.s_id,student.s_name,sum(score.s_score)
from  student
INNER JOIN score on student.s_id=score.s_id
group by student.s_id;

select s_name,s_score
from student
inner join  score on  student.s_id=score.s_id
where score.c_id='03';

select sum(s_score)+10
from score;

select s_name,sum(s_score)
from student
inner join score on student.s_id=score.s_id
where s_name='liyun'
group by  s_name;

select s_name,s_score
from student
inner join  score on student.s_id=score.s_id
where score.c_id='03' and score.s_score >90

select s_name,sum(s_score)
from student
inner join score on student.s_id=score.s_id
group by  s_name
having sum(s_score)>270;

select student.s_name,score.s_score
from student
inner join  score on student.s_id=score.s_id
where score.c_id='03' and score.s_score <90 and score.s_score>80;

select
from student
inner join score on student.s_id=score.s_id
inner join course on score.c_id=course.c_id
where course.c_name='math' and s_score>=89 and s_score<=90


select s_name,avg(s_score)
from student
inner join score on student.s_id=score.s_id
where s_name like 'li%'
group by s_name
having avg(s_score);

10
select s_name as name,s_score as score
from score
inner join course on score.c_id=course.c_id
inner join student on  score.s_id=student.s_id
where score.s_score>70 and  course.c_name='chinese'
union
select s_name as name,s_score as score
from score
inner join course on score.c_id=course.c_id
inner join student on  score.s_id=student.s_id
where s_score>80 and c_name='math';


SELECT student.s_name
FROM (
   select s_name as name
from score
inner join course on score.c_id=course.c_id
inner join student on  score.s_id=student.s_id
where score.s_score>70 and  course.c_name='chinese'
union
select s_name as name
from score
inner join course on score.c_id=course.c_id
inner join student on  score.s_id=student.s_id
where s_score>80 and c_name='math'
 ) allname join student  ON (student.s_name= allname.name)



select score.s_score
from score
left outer join course on score.c_id=course.c_id
where course.c_name='math'
order by score.s_score;

select sum(s_score)
from student
inner join score on student.s_id=score.s_id
group by student.s_id;


select student.s_id,sum(score.s_score) as  li
from  student
INNER JOIN score on student.s_id=score.s_id
group by student.s_id
order by li;

select score.s_score
from score
left outer join course on score.c_id=course.c_id
left outer join student on  score.s_id=s_name
where course.c_name='math' and s_name like 'li%'
order by score.s_score;

select s_name
from student
where s_birth like  '1990%';

select student.s_name,course.c_name,score.s_score
from score
left outer join course on score.c_id=course.c_id
left outer join student on  score.s_id=student.s_id
where s_score>70;

create table if not  exists b1(
id1 tinyint,
id2 smallint ,
id3 int ,
id4 bigint,
sla float ,
sla1 double ,
isok boolean,
content binary,
dt timestamp
)row format delimited fields terminated by ',';

create table if not  exists arr1(
name string,
score array<string>
)row format delimited fields terminated by ' '
collection items terminated by ',';

load data local inpath '/hivedata/zhang3.csv' into table arr1

select  name,cj from arr1 lateral view  explode(score) score as cj

select  name,sum(cj) from arr1 lateral view  explode(score) score as cj group by name;

create table arr2_temp as select  name,cj from arr1 lateral view  explode(score) score as cj

create table if not  exists arr2(
name string,
score array<string>
)
row format delimited fields terminated by ' '
collection items terminated by ',';




create table if not  exists map2(
name string,
score map<string,int>
)row format delimited fields terminated by ' '
collection items terminated by ','
map keys terminated by ':';

load data local  inpath '/hivedata/zhang2.csv' into table map2




select to_date('2017-01-01 12:12:12');

select to_date(regexp_replace('2017/1/1','/','-'))

select concat_ws("-",'1','2','2');

select concat('2','2');

-- 显示转换
select cast('11' as int );

select ceil(42.3);
















