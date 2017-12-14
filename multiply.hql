drop table Mmatrix;
drop table Nmatrix;
drop table intermediate;
drop table MNoutput;

create table Mmatrix (
  i int,
  j int,
  value double)
row format delimited fields terminated by ',' stored as textfile;

create table Nmatrix (
  i int,
  j int,
  value double)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:M}' overwrite into table Mmatrix;

load data local inpath '${hiveconf:N}' overwrite into table Nmatrix;

create table intermediate (i int, j int, value double);
create table MNoutput (i int, j int, value double);

INSERT OVERWRITE TABLE intermediate
select m.i,n.j,m.value*n.value
from Mmatrix as m join Nmatrix as n on m.j = n.i
ORDER BY m.i,n.j;

INSERT OVERWRITE TABLE MNoutput
select i,j,SUM(value) from intermediate
GROUP BY i,j;

select COUNT(*),AVG(value) from MNoutput;