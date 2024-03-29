select count(1) 
from 
(
select 
           t.id,
           t.account_num,
	   sum(t.income) as income,
	   sum(t.outcome) as outcome,
	   t.client_type
from
(
select 
           acc.id,
	   acc.account_num,
	   SUM(oper.operation_sum) as income,
	   0 as outcome,
	   acc.client_type 
from
       Accounts acc
left join Operations oper on
       acc.account_num = oper.account_num_recipient
group by acc.id, acc.account_num, acc.client_type	   
union
select 
           acc.id,
	   acc.account_num,
	   0 as income,
	   SUM(oper.operation_sum) as outcome,
	   acc.client_type
from
       Accounts acc
left join Operations oper on
       acc.account_num = oper.account_num
group by acc.id, acc.account_num, acc.client_type) t
group by t.id, t.account_num, t.client_type) tt