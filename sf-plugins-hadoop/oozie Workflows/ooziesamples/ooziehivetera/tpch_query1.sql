use tpch_flat_orc_2;
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= '1998-09-16'
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
