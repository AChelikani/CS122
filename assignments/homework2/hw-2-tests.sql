select 3 + 2 as five;

drop table if exists testing;
create table testing ( a integer );
insert into testing values ( 7 );
insert into testing values ( 1 );
insert into testing values ( 2 );
select * from testing;
select * from testing order by A;

drop table if exists testing;
create table testing ( a integer );
insert into testing values ( 7 );
insert into testing values ( 1 );
insert into testing values ( 2 );
select * from testing;
select * from testing where A=7;

drop table if exists testing;
create table testing ( a integer, b integer );
insert into testing values ( 1 , 2 );
insert into testing values ( 2 , 2 );
select * from testing;
select A from ( select * from testing ) as test1;

drop table if exists testing;
create table testing ( a integer, b integer );
insert into testing values ( 1 , 2 );
insert into testing values ( 2 , 2 );
insert into testing values ( 1 , 1 );
insert into testing values ( 2 , 1 );
select * from ( select * from testing where A=1 ) as test2 where B=1;

drop table if exists jointest1;
drop table if exists jointest2;
create table jointest1 ( a integer, b integer );
create table jointest2 ( b integer, c integer );
insert into jointest1 values ( 1, 1 );
insert into jointest1 values ( 2, 2 );
insert into jointest1 values ( 3, 3 );
insert into jointest2 values ( 1, 1 );
insert into jointest2 values ( 2, 2 );
insert into jointest2 values ( 3, 3 );
select * from jointest1 JOIN jointest2;
