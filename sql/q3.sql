select exp_02
	, count(distinct sos_id) as voters
	, sum(case when g2020 != '' then 1 else 0 end) as g2020_voted
from test.experiments e
left join hotel.oh_cd11_clean occ
on e.sos_id = occ.sos_voterid
group by exp_02
order by exp_02; 
