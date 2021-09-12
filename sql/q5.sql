with addresses as (
select address1, address2, city, state, zip
from test.experiments e
left join hotel.oh_cd11_clean occ
on e.sos_id = occ.sos_voterid
group by 1,2,3,4,5
)

select count(*) as unique_addresses
from addresses;
