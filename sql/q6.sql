with deciles as (
select *
	, NTILE(10) over (order by exp_03 asc) as decile
from test.experiments
)

select decile
	, AVG(CAST(exp_03 as float)) as exp_03_avg
	, ROUND(AVG(CAST(exp_03 as float)) * 100, 0) as exp_03_avg_pct
from deciles
group by 1
order by 1 asc;
