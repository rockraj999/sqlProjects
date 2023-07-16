----==========================================Customer Transaction Analysis=========================================-----------------

--  Introduction
--  In this project, I am going to analyze credit card transactions done by customers in India. for this, I fetched the data from Kaggle.
--  The purpose of this project is to utilize my SQL skill in playing with data and finding insight that can be used to make Business 
--  decision.

--  Data Source:- https://www.kaggle.com/datasets/thedevastator/analyzing-credit-card-spending-habits-in-india


--  Data Exploration
--  In this dataset, it contains 26K+ rows and there are 7 columns: transaction_id, city, transaction_date, card_type, exp_type, gender
--  and amount. here i ensured the data quality by confirming whether Null is present or not and same fro duplicate rows. this data set
--  has transaction_id as its primary key. this dataset contains the transaction data from 2013-10 to 2015-05 done via credit card in
--  India. In the card_type column, we have considered Silver, Signature, Gold and Platinum. and expense type(exp_type) column holds 
--  values like Entertainment, Food, Bills, Fuel, Travel and Grocery.



-- total 26052 transaction
-- transaction_id(unique for each row), city, transaction_date, amount, card_type, exp_type, gender
-- this dataset contains transaction data from 2013-10 to 2015-05
-- card type belongs from Silver,Signature, Gold, Platinum
-- exp_type holds values like Entertainment, Food, Bills, Fuel, Travel, Grocery.

-- checking presence of Null values in columns .
SELECT *
FROM credit_card_transcations
WHERE transaction_id IS NULL;

-- checking duplicate rows is present or not in table.
SELECT transaction_id, city, transaction_date, amount, card_type, exp_type, gender, COUNT(*) as count
FROM credit_card_transcations
GROUP BY transaction_id, city, transaction_date, amount, card_type, exp_type, gender
HAVING COUNT(*) > 1;

-- If duplicate present then deleting them from table.

with cte_delete as 
(select *,
DENSE_RANK() over(partition by transaction_id, city, transaction_date, amount, card_type, exp_type, gender order by transaction_id) as rnk 
from credit_card_transcations)
delete from cte_delete where rnk>=2


-------================================================     QURIES   ===============================================------------------

select * from credit_card_transcations;

--  write a query to print top 5 cities with highest spends and their percentage contribution of total credit card spends.


with cte_spend as
              (select city,sum(amount) as spend from credit_card_transcations group by city)
    ,cte_tot_apend as (select sum(amount) as tot_spend from credit_card_transcations)

select top 5 city,spend*100.0/cte_tot_apend.tot_spend as percen_amt
from cte_spend cs,cte_tot_apend  order by percen_amt desc


--  write a query to print highest spend month and amount spent in that month for each card type


with cte_spend as 
		(select card_type,datepart(month,transaction_date) as mth,sum(amount) as spend from credit_card_transcations
			group by card_type,datepart(month,transaction_date))

select * from (select *,max(spend) over(partition by card_type)as max_amt from cte_spend) A where spend=max_amt



-- write a query to print the transaction details(all columns from the table) for each card type when
-- it reaches a cumulative of 1000000 total spends(We should have 4 rows in the o/p one for each card type)

with cte_run as (select * , 
sum(amount) over (partition by card_type order by transaction_date rows between unbounded preceding and current row) as running_sum
from credit_card_transcations)
select * from (select *,ROW_NUMBER() over (partition by card_type order by running_sum) as rnk from cte_run where running_sum >=1000000) a
where rnk=1;

--  OR

with cte_min_amt as (select *,min(running_sum) over(partition by card_type) as min_amt from 
(select *, sum(amount) over(partition by card_type order by amount) as running_sum from credit_card_transcations) A
where running_sum>=1000000)
select * from cte_min_amt where running_sum=min_amt


-- write a query to find city which had lowest percentage spend for gold card type

with cte_spend as (select city,card_type,sum(amount) spend from credit_card_transcations 
group by city,card_type )
,cte_per as (select *,sum(spend) over(partition by city) as run_spend,(spend*100.0/sum(spend) over(partition by city)) per from cte_spend)
select city from cte_per where per in (select min(per) from cte_per where card_type='Gold')


-- write a query to print 3 columns:  city, highest_expense_type , lowest_expense_type (example format : Delhi , bills, Fuel)


with cte_spend as (select city,exp_type,sum(amount) as spend  from credit_card_transcations group by city,exp_type)
,cte_max_min as (select *,max(spend) over (partition by city) as max_spend,min(spend) over (partition by city) as min_spend from cte_spend)
select city,string_agg(case when spend=max_spend then exp_type end,',') as highest_expense_type,
string_agg(case when spend=min_spend then exp_type end,',') as lowest_expense_type from cte_max_min group by city


--write a query to find percentage contribution of spends by females for each expense type

select exp_type,sum(case when gender='F' then amount else 0 end)*100.0/sum(amount) per from credit_card_transcations group by exp_type;



-- which card and expense type combination saw highest month over month growth in Jan-2014

with cte_spend as (select card_type,exp_type,format(transaction_date,'yyyyMM') as yyyyMM, sum(amount) as spend  
from credit_card_transcations group by card_type,exp_type,format(transaction_date,'yyyyMM'))
, cte_per as (select *,(spend-lag(spend,1) over(partition by card_type,exp_type order by yyyyMM ))*100.0/lag(spend,1) over(partition by card_type,exp_type order by yyyyMM ) as per_spen
from cte_spend) 
select top 1 card_type,exp_type,yyyyMM,per_spen from  cte_per
where yyyyMM='201401' order by per_spen desc


-- during weekends which city has highest total spend to total no of transcations ratio
 select top 5 * from credit_card_transcations;

 
with cte_ratio as (select city,sum(amount)/count(1) as tot_spend_cnt
from credit_card_transcations where DATEPART(WEEKDAY,transaction_date) in (1,7) group by city)
select * from cte_ratio where tot_spend_cnt=(select max(tot_spend_cnt) from cte_ratio);


 -- which city took least number of days to reach its 500th transaction after the first transaction in that city

 with cte_min_day as(select a.city,a.first_trans,b.transaction_date,datediff(DAY,a.first_trans,b.transaction_date) days_to from 
 (select city,min(transaction_date) first_trans from credit_card_transcations group by city) a join
 (select * from (select *,ROW_NUMBER() over( partition by city order by transaction_date) as no_of_trans from credit_card_transcations) a
 where no_of_trans=500) b on a.city=b.city)
 select city from cte_min_day where days_to in (select min(days_to) from cte_min_day)





