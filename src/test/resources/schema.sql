DROP TABLE IF EXISTS people;
create table people (
    id   INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    nick varchar(32),
    created_at datetime
) DEFAULT CHARSET utf8;