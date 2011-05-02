-- RSS Config Database Definition

drop database if exists RSSConfig;
create database RSSConfig;
use RSSConfig;

--
-- Table for default values
--


-- ----------------------------------------------------------------
drop table if exists Notification;

create table Notification (
 label varchar(32),
 primary key (label)
);

insert into Notification values
       ('Mail'), ('Web'), ('SMS');
-- -----------------------------------------------------------------
drop table if exists Resourse;

create table Resource (
 label varchar(32),
 primary key (label)
);

insert into Resource values
       ('Site'), ('Service'), ('Resource'), ('StorageElement');
-- ----------------------------------------------------------------
drop table if exists Status;

create table Status (
 label    varchar(32),
 priority integer not null,
 primary key (label)
);

insert into Status values
       ('Banned',0), ('Bad',1), ('Probing',2), ('Active',3);
-- ----------------------------------------------------------------
drop table if exists PolicyTypes;

create table PolicyType (
 label varchar(32),
 primary key (label)
);

insert into PolicyType values
       ('Resource_PolType'), ('Alarm_PolType'), ('Collective_PolType'), ('RealBan_PolType');
-- ----------------------------------------------------------------
drop table if exists SiteType;

create table SiteType (
 label varchar(32),
 primary key (label)
);

insert into SiteType values ('T0'), ('T1'), ('T2'), ('T3');
-- ----------------------------------------------------------------
drop table if exists ResourceType;

create table ResourceType (
 label varchar(32),
 primary key (label)
);

insert into ResourceType values
       ('CE'), ('CREAMCE'), ('SE'), ('LFC_C'), ('LFC_L'), ('FTS'), ('VOMS');
-- ----------------------------------------------------------------
drop table if exists ServiceType;

create table ServiceType (
 label varchar(32),
 primary key (label)
);

insert into ServiceType values
       ('Computing'), ('Storage'), ('VO-BOX'), ('VOMS');
-- ----------------------------------------------------------------
drop table if exists Users;

create table Users (
 login varchar(32),
 primary key (login)
);

-- insert into Users values
--        ('fstagni'), ('roma'), ('santinel'), ('joel'), ('rsantana'), ('vibernar'), ('ubeda');
-- ----------------------------------------------------------------

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
 foreign key (granularity) references Resource(label),
 foreign key (site_type)   references SiteType(label),
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
 foreign key (login)         references Users(login),
 foreign key (granularity)   references Resource(label),
 foreign key (site_type)     references SiteType(label),
 foreign key (service_type)  references ServiceType(label),
 foreign key (resource_type) references ResourceType(label),
 foreign key (notification)  references Notification(label)
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
 foreign key (status)        references Status(label),
 foreign key (former_status) references Status(label),
 foreign key (site_type)     references SiteType(label),
 foreign key (service_type)  references ServiceType(label),
 foreign key (resource_type) references ResourceType(label)
);
