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

select * from movierating where user_id = 41;

select movie_id,rating from movierating where user_id = 41;

select * from movierating full outer join movies on movierating.movie_id=movies.id;

select avg(rating) from movierating where user_id = 41;

select user_id,count(rating),avg(rating) from movierating group by user_id;

select user_id,count(rating),avg(rating) into userrating from movierating group by user_id;

select * from userrating;
