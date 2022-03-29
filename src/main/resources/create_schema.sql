create database if not exists repository;
create user 'user'@'%' identified by 'password';
grant all privileges on repository.* to 'user'@'%';
flush privileges;

create table if not exists `repository`.`usertags` (`user` varchar(20) not null, `taglist` varchar(100) default  null, primary key (`user`));

insert into `repository`.`usertags` (`user`, `taglist`) values ('davide', 'dpo,analyst'), ('bob','analyst'), ('alice', NULL);