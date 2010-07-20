
insert into table1 (col1) values (1);
insert into table1 (col1) values (2);
rollback;
insert into table1 (col1) values (3);
