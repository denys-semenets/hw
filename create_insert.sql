create database HW;
use hw;

create table players (
player VARchar(50), 
team Varchar(50)
);
INSERT INTO players (player, team) VALUES
('LeBron James', 'Los Angeles Lakers'),
('Stephen Curry', 'Golden State Warriors'),
('Kevin Durant', 'Phoenix Suns'),
('Giannis Antetokounmpo', 'Milwaukee Bucks'),
('Nikola Jokic', 'Denver Nuggets');



create table last_five_games (
team varchar(50),
game1 varchar(1),
game2 varchar(1),
game3 varchar(1),
game4 varchar(1),
game5 varchar(1)
);

insert into last_five_games(team,game1,game2,game3,game4,game5) Values
('Los Angeles Lakers', 'W', 'L', 'W', 'W', 'L'),
('Golden State Warriors', 'W', 'W', 'L', 'W', 'W'),
('Phoenix Suns', 'L', 'W', 'W', 'L', 'W'),
('Milwaukee Bucks', 'W', 'L', 'L', 'L', 'W'),
('Denver Nuggets', 'W', 'W', 'W', 'L', 'W');


CREATE TABLE player_stats (
    player Varchar(50),
    avg_points DECIMAL(4, 1),
    fg DECIMAL(4, 1)
);

INSERT INTO player_stats (player, avg_points, fg) VALUES
('LeBron James', 27.2, 50.5),
('Stephen Curry', 28.1, 56.6),
('Kevin Durant', 29.1, 52.4),
('Giannis Antetokounmpo', 30.4, 55.3),
('Nikola Jokic', 26.4, 58.3);


CREATE TABLE team_stats (
    team VARCHAR(50),
    ppg DECIMAL(4, 1),
    fg Decimal(4,1)
);

insert into team_stats (team, ppg, fg) values
('Los Angeles Lakers', 114.5, 48.5),
('Golden State Warriors', 118.2, 57.8),
('Phoenix Suns', 112.9, 47.5),
('Milwaukee Bucks', 115.0, 49.1),
('Denver Nuggets', 119.5, 50.2);

create table team_profit(
team varchar(50),
profit int
);

INSERT INTO team_profit (team, profit) VALUES
('Los Angeles Lakers', 125000000),
('Golden State Warriors', 135000000),
('Phoenix Suns', 75000000),
('Milwaukee Bucks', 68000000),
('Denver Nuggets', 72000000);
