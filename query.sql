-- Question 1
-- Percentage issues. Due cast function not working as it should.

with total_txn as 

(

select
from_address,
(case when sum(gross_usd_amt >100)  then '100+' else '100-' end) as txn_value_flag
from immutable_x_transaction ixt
--inner join collection_name_address cna on ixt.collection_address=cna.collection_address 
where ixt.txn_type = 'trade'
group by 
from_address

union ALL 

select
to_address,
(case when sum(gross_usd_amt >100)  then '100+' else '100-' end) as ttxn_value_flag
from immutable_x_transaction ixt
--inner join collection_name_address cna on ixt.collection_address=cna.collection_address 
where ixt.txn_type = 'trade'
group by 
to_address

)

SELECT 
--from_address,
count ((case when txn_value_flag = '100+' then txn_value_flag  else null end)) '100+',
count((case when txn_value_flag in ( '100+','100-') then txn_value_flag else 0 end)) 'all',
cast(('100+'/'all')*100) as real
from total_txn 


---Question 2
--- During coding I Could not find a way to join the the variants on the txy_type ( I notice there is a space atthe end or the word)
--- Used functions trim, replace and case when with space (usually works on redshift)
----which implicated in use percent rank function to deliver the final results as a percentage.

select 
--trim(txn_type) as txn_type,
--replace (txn_type,' ','') as txn_type ,
date(txn_time) as date,
txn_type,
count(txn_id) as total_txn
/*case when txn_type in ('mint','mint ') then 'mint' 
	 when txn_type in ('trade','trade ') then 'trade' 	
	 when txn_type in ('transfer','transfer ') then 'transfer' else NULL end as 'txn_type_final',

cast(percent_rank() over (order by (txn_type)) as real)*/

from immutable_x_transaction
--where date(txn_time) = '2022-06-05' -- for testting purposes
group by
date,
txn_type




----------- Question 3
----- Assumed the stakeholder would be interested in the transaction as well as Sales ($)



with days as 
(
select
cna.nft_collection_name, -- collection name
date(cc.collection_creation_time),
date(txn_time),
cast((JULIANDAY(txn_time) - JULIANDAY(cc.collection_creation_time)) as integer) as days,
count(txn_id) as txn_id, -- counts the total transactions for both wallets (to and from)
sum(gross_usd_amt) as sales -- brings the total sales
from immutable_x_transaction ixt
inner join collection_name_address cna on ixt.collection_address=cna.collection_address 
inner join collection_creation cc on cc.collection_address = cna.collection_address 
where ixt.txn_type = 'trade'
group by cna.nft_collection_name,
cc.collection_creation_time,
days
)

select 
nft_collection_name,
days,
count(txn_id) as N_txn,
sum(sales) as sales
from days
where days < 3
group by 
nft_collection_name,
days


--- Question 4 
select
to_address wallets,
count(DISTINCT token_id) as n_token_id
from immutable_x_transaction ixt
inner join collection_name_address cna on ixt.collection_address=cna.collection_address 
where cna.nft_collection_name ='Gods Unchained'
and ixt.txn_type = 'transfer'
group by 
wallets
order by n_token_id desc
limit 20



--------------


----- Question 6

with rank_wallets as 
(
select
cna.nft_collection_name as nft_collection_name,
ixt.to_address as to_wallets, --- need to be to_addreess as the wallet received the NFT
count(token_id) as n_token_id,
rank () over(PARTITION by cna.nft_collection_name order by count(DISTINCT token_id) desc) as rank
from immutable_x_transaction ixt
inner join collection_name_address cna on ixt.collection_address=cna.collection_address 
and ixt.txn_type = 'transfer'
--and date(txn_time) = '2022-06-05' -- date filter for testing
group by 1,2
)
select 
nft_collection_name,
to_wallets,
n_token_id
from rank_wallets 
where rank <=20
group by 
nft_collection_name,
to_wallets
order by nft_collection_name,n_token_id desc

--- Question 7

with rank_wallets as 
(
select
cna.nft_collection_name as nft_collection_name,
ixt.to_address as to_wallets, --- need to be to_addreess as the wallet received the NFT
sum(gross_usd_amt) as gross_usd_amt,
rank () over(PARTITION by cna.nft_collection_name order by sum(gross_usd_amt) desc) as rank
from immutable_x_transaction ixt
inner join collection_name_address cna on ixt.collection_address=cna.collection_address 
and ixt.txn_type = 'trade' -- the sales are registered at the trades. Income to wallets show the NFT income in $ 
--and date(txn_time) = '2022-06-05' -- date filter for testing
group by 
nft_collection_name,
to_wallets
)
select 
nft_collection_name,
to_wallets,
gross_usd_amt
from rank_wallets 
where rank <=20
group by 
nft_collection_name,
to_wallets 
order by 
nft_collection_name,
gross_usd_amt desc

--- Question 8
----- Assumed the stakeholder would be interested in the transaction number as well as Sales ($)

select
mkt.taker_marketplace_name,
sum(gross_usd_amt) as gross_usd_amt,
count(txn_id)as txn_id
from immutable_x_transaction ixt
inner join marketplace_address mkt on ixt.marketpace_address=mkt.marketpace_address 
where ixt.txn_type = 'trade'
group by mkt.taker_marketplace_name
order by txn_id desc


--------------  Visualization

--Q2 code:

select
mkt.taker_marketplace_name,
ixt.from_address,
ixt.to_address,
ixt.txn_type,
cna.nft_collection_name,
date(txn_time) date,
count(ixt.token_id),
sum(ixt.gross_usd_amt) as gross_usd_amt,
count(ixt.txn_id)as txn_id
from immutable_x_transaction ixt
inner join marketplace_address mkt on ixt.marketpace_address=mkt.marketpace_address 
inner join collection_name_address cna on ixt.collection_address=cna.collection_address 
inner join collection_creation cc on cc.collection_address = cna.collection_address 
--where ixt.txn_type = 'trade'
--where date(txn_time) = '2022-06-05' -- date filter for testing
group by
1,2,3,4,5


----- q1


select
mkt.taker_marketplace_name,
ixt.from_address,
ixt.to_address,
ixt.txn_type,
cna.nft_collection_name,
ixt.to_address,
ixt.from_address,
ixt.txn_time,
date(txn_time) date,
count(ixt.token_id),
sum(ixt.gross_usd_amt) as gross_usd_amt,
count(ixt.txn_id)as txn_id
from immutable_x_transaction ixt
inner join marketplace_address mkt on ixt.marketpace_address=mkt.marketpace_address 
inner join collection_name_address cna on ixt.collection_address=cna.collection_address 
inner join collection_creation cc on cc.collection_address = cna.collection_address 
where ixt.txn_type = 'trade'
--where date(txn_time) = '2022-06-05' -- date filter for testing
group by
1,2,3,4,5,6,7,8








