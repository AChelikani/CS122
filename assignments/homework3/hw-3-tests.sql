drop table if exists table1;
create table table1 ( a integer );
insert into table1 values ( 1 );
insert into table1 values ( 2 );
insert into table1 values ( 3 );
analyze table1;
show table table1 stats;

drop table if exists table2;
create table table2 ( a varchar(20) );
insert into table2 values ( 'hello' );
insert into table2 values ( 'my' );
insert into table2 values ( 'name' );
analyze table2;
show table table2 stats;

drop table if exists table3;
create table table3 ( a integer, b integer );
insert into table3 values ( 1 , 2 );
insert into table3 values ( 1 , 2 );
insert into table3 values ( 3 , 4 );
analyze table3;
show table table3 stats;
