with wins as(
Select team, (case when game1 = 'W' Then 1 else 0 end +
			case when game2 = 'W' Then 1 else 0 end +
			case when game3 = 'W' Then 1 else 0 end +
			case when game4 = 'W' Then 1 else 0 end +
			case when game5 = 'W' Then 1 else 0 end )/5
            as win_rate from last_five_games 
)
Select p.player, p.team, tp.profit as team_profit, ps.fg, w.win_rate from players p
	join player_stats ps on p.player = ps.player
    join team_profit tp on p.team = tp.team
    join wins w on p.team = w.team  
where tp.profit > 75000000 and p.player in(
Select player from player_stats Where fg > 50)
Group by 
p.player, p.team, tp.profit, ps.fg, w.win_rate
Having avg(ps.fg>50)
order by tp.profit desc 
limit 2;