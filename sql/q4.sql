select exp_01
	, state_hd
	, count(distinct sos_id) as voters
from test.experiments e
left join hotel.oh_cd11_clean occ
on e.sos_id = occ.sos_voterid
group by exp_01, state_hd
order by exp_01, state_hd; 
