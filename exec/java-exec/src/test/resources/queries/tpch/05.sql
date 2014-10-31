-- tpch5 using 1395599672 as a seed to the RNG
select
  n.n_name,
  sum(l.l_extendedprice * (1 - l.l_discount)) as revenue

from
  dfs.`/Users/jaltekruse/Downloads/sf-10_tpc-h_parquet/customer` c,
  dfs.`/Users/jaltekruse/Downloads/sf-10_tpc-h_parquet/orders` o,
  dfs.`/Users/jaltekruse/Downloads/sf-10_tpc-h_parquet/lineitem` l,
  dfs.`/Users/jaltekruse/Downloads/sf-10_tpc-h_parquet/supplier` s,
  dfs.`/Users/jaltekruse/Downloads/sf-10_tpc-h_parquet/nation` n,
  dfs.`/Users/jaltekruse/Downloads/sf-10_tpc-h_parquet/region` r

where
  c.c_custkey = o.o_custkey
  and l.l_orderkey = o.o_orderkey
  and l.l_suppkey = s.s_suppkey
  and c.c_nationkey = s.s_nationkey
  and s.s_nationkey = n.n_nationkey
  and n.n_regionkey = r.r_regionkey
  and r.r_name = 'EUROPE'
  and o.o_orderdate >= date '1997-01-01'
  and o.o_orderdate < date '1997-01-01' + interval '1' year
group by
  n.n_name

order by
  revenue desc;