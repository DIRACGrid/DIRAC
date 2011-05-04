-- RSS Config Database Definition

drop database if exists RSSConfigurationDB;
create database RSSConfigurationDB;
use RSSConfigurationDB;

-- ----------------------------------------------------------------
drop table if exists Users;

create table Users (
 login varchar(32),
 primary key (login)
);

-- insert into Users values
--        ('fstagni'), ('roma'), ('santinel'), ('joel'), ('rsantana'), ('vibernar'), ('ubeda');
-- ----------------------------------------------------------------
drop table if exists Status;

create table Status (
 label varchar(32),
 priority integer,
 primary key (label)
);

insert into Status values
       ('Banned', 0), ('Bad',1), ('Probing', 3), ('Active', 4);
-- -----------------------------------------------------------------

--
-- Tables for the actual Configurations
--


-- ----------------------------------------------------------------
drop table if exists PoliciesParams;
create table PoliciesParams (
 label varchar(64),
 val   integer not null,
 primary key (label)
);
-- ----------------------------------------------------------------
drop table if exists CheckFreqs;
create table CheckFreqs (
 granularity varchar(32),
 site_type   varchar(32),
 status      varchar(32),
 freq        integer not null,
 primary key (granularity, site_type, status),
 foreign key (status)      references Status(label)
);
-- ----------------------------------------------------------------
drop table if exists AssigneeGroups;
create table AssigneeGroups (
 label         varchar(64),
 login         varchar(32),
 granularity   varchar(32),
 site_type     varchar(32),
 service_type  varchar(32),
 resource_type varchar(32),
 notification  varchar(32),
 primary key (label),
 foreign key (login)         references Users(login)
);
-- ----------------------------------------------------------------
drop table if exists Policies;
create table Policies (
 label         varchar(32),
 description   varchar(255),
 status        varchar(32),
 former_status varchar(32),
 site_type     varchar(32),
 service_type  varchar(32),
 resource_type varchar(32),
 primary key (label),
 foreign key (status)        references Status(label)
);
