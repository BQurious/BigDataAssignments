CREATE TABLE movies (id int, name string, year int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './movie.csv' OVERWRITE INTO TABLE movies;

CREATE TABLE movierating (movie_id int, user_id int, rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
LOAD DATA LOCAL INPATH './movieRating.csv' OVERWRITE INTO TABLE movierating;

show tables;

describe movies;
describe movierating;

select * from movies;

SELECT * FROM movies LIMIT 15;

select * from movierating;

select * from movies where year <2000;

select * from movies where year <2000 sort by name;

select * from movies where year <2000 and name not like 'a%' sort by name;