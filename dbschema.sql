drop table if exists streams;
create table streams (
  id varchar(32) primary key,
  credentials text not null,
  started_at timestamp
);


