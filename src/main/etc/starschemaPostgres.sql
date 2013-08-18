create sequence time_id_seq;

create table timeperiod (
       time_id int not null primary key default nextval('time_id_seq'),
       year int not null,
       month int not null,
       day int not null
);

create sequence category_id_seq;

create table category (
       category_id int not null primary key default nextval('category_id_seq'),
       name varchar(100) not null
);

create sequence district_id_seq;

create table district (
       district_id int not null primary key default nextval('district_id_seq'),
       name varchar(50) not null
);
create table fact (
       crimes int not null,
       district_id int not null,
       category_id int not null,
       time_id int not null
);
