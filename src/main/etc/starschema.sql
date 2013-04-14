create table timeperiod (
       time_id int not null primary key auto_increment,
       year int not null,
       month int not null,
       week int not null,
       day int not null
);
create table category (
       category_id int not null primary key auto_increment,
       name varchar(100) not null
);
create table district (
       district_id int not null primary key auto_increment,
       name varchar(50) not null
);
create table fact (
       crimes int not null,
       district_id int not null,
       category_id int not null,
       time_id int not null
);
