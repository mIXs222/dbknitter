tpch_queries = {}
tpch_queries[1] = """
SELECT
    L_RETURNFLAG,
    L_LINESTATUS,
    SUM(L_QUANTITY) AS SUM_QTY,
    SUM(L_EXTENDEDPRICE) AS SUM_BASE_PRICE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS SUM_DISC_PRICE,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) AS SUM_CHARGE,
    AVG(L_QUANTITY) AS AVG_QTY,
    AVG(L_EXTENDEDPRICE) AS AVG_PRICE,
    AVG(L_DISCOUNT) AS AVG_DISC,
    COUNT(*) AS COUNT_ORDER
FROM
    lineitem
WHERE
    L_SHIPDATE <= '1998-09-02'
GROUP BY
    L_RETURNFLAG,
    L_LINESTATUS
ORDER BY
    L_RETURNFLAG,
    L_LINESTATUS
"""
tpch_queries[10] = """
SELECT
    C_CUSTKEY,
    C_NAME,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
    C_ACCTBAL,
    N_NAME,
    C_ADDRESS,
    C_PHONE,
    C_COMMENT
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE >= '1993-10-01'
    AND O_ORDERDATE < '1994-01-01'
    AND L_RETURNFLAG = 'R'
    AND C_NATIONKEY = N_NATIONKEY
GROUP BY
    C_CUSTKEY,
    C_NAME,
    C_ACCTBAL,
    C_PHONE,
    N_NAME,
    C_ADDRESS,
    C_COMMENT
ORDER BY
    REVENUE, C_CUSTKEY, C_NAME, C_ACCTBAL DESC
"""
tpch_queries[11] = """
SELECT
    PS_PARTKEY,
    SUM(PS_SUPPLYCOST * PS_AVAILQTY) AS VALUE
FROM
    partsupp,
    supplier,
    nation
WHERE
    PS_SUPPKEY = S_SUPPKEY
    AND S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'GERMANY'
GROUP BY
    PS_PARTKEY HAVING
    SUM(PS_SUPPLYCOST * PS_AVAILQTY) >
    (
    SELECT
        SUM(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001000000
    FROM
        partsupp,
        supplier,
        nation
    WHERE
        PS_SUPPKEY = S_SUPPKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND N_NAME = 'GERMANY'
    )
ORDER BY
    VALUE DESC
"""
tpch_queries[12] = """
SELECT
    L_SHIPMODE,
    SUM(CASE
            WHEN O_ORDERPRIORITY = '1-URGENT'
            OR O_ORDERPRIORITY = '2-HIGH'
            THEN 1
            ELSE 0
    END) AS HIGH_LINE_COUNT,
    SUM(CASE
            WHEN O_ORDERPRIORITY <> '1-URGENT'
            AND O_ORDERPRIORITY <> '2-HIGH'
            THEN 1
            ELSE 0
    END) AS LOW_LINE_COUNT
FROM
    orders,
    lineitem
WHERE
    O_ORDERKEY = L_ORDERKEY
    AND L_SHIPMODE IN ('MAIL', 'SHIP')
    AND L_COMMITDATE < L_RECEIPTDATE
    AND L_SHIPDATE < L_COMMITDATE
    AND L_RECEIPTDATE >= '1994-01-01'
    AND L_RECEIPTDATE < '1995-01-01'
GROUP BY
    L_SHIPMODE
ORDER BY
    L_SHIPMODE
"""
tpch_queries[13] = """
SELECT
    C_COUNT,
    COUNT(*) AS CUSTDIST
FROM
    (
    SELECT
        C_CUSTKEY,
        COUNT(O_ORDERKEY) AS C_COUNT
    FROM
        customer LEFT OUTER JOIN orders ON
        C_CUSTKEY = O_CUSTKEY
        AND O_COMMENT NOT LIKE '%pending%deposits%'
    GROUP BY
        C_CUSTKEY
    )   C_ORDERS
GROUP BY
    C_COUNT
ORDER BY
    CUSTDIST DESC,
    C_COUNT DESC
"""
tpch_queries[14] = """
SELECT
    100.00 * SUM(CASE WHEN P_TYPE LIKE 'PROMO%' THEN L_EXTENDEDPRICE * (1 - L_DISCOUNT)
    ELSE 0
    END) / SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS PROMO_REVENUE
FROM
    lineitem,
    part
WHERE
    L_PARTKEY = P_PARTKEY
    AND L_SHIPDATE >= '1995-09-01'
    AND L_SHIPDATE < '1995-10-01'
"""
tpch_queries[15] = """
with revenue0 as
(select
  L_SUPPKEY as SUPPLIER_NO,
  sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as TOTAL_REVENUE
  from
  lineitem
  where
  L_SHIPDATE >= '1996-01-01'
  AND L_SHIPDATE < date('1996-01-01', '+3 month')
  group by
  L_SUPPKEY)
select
S_SUPPKEY,
S_NAME,
S_ADDRESS,
S_PHONE,
TOTAL_REVENUE
from
supplier,
revenue0
where
S_SUPPKEY = SUPPLIER_NO
and TOTAL_REVENUE = (
  select
  max(TOTAL_REVENUE)
  from
  revenue0
)
order by
S_SUPPKEY
"""
tpch_queries[16] = """
SELECT
    P_BRAND,
    P_TYPE,
    P_SIZE,
    COUNT(DISTINCT PS_SUPPKEY) AS SUPPLIER_CNT
FROM
    partsupp,
    part
WHERE
    P_PARTKEY = PS_PARTKEY
    AND P_BRAND <> 'Brand#45'
    AND P_TYPE NOT LIKE 'MEDIUM POLISHED%'
    AND P_SIZE IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND PS_SUPPKEY NOT IN (
        SELECT
            S_SUPPKEY
        FROM
            supplier
        WHERE
            S_COMMENT LIKE '%Customer%Complaints%'
    )
GROUP BY
    P_BRAND,
    P_TYPE,
    P_SIZE
ORDER BY
    SUPPLIER_CNT DESC,
    P_BRAND,
    P_TYPE,
    P_SIZE
"""
tpch_queries[17] = """
SELECT
    SUM(L_EXTENDEDPRICE) / 7.0 AS AVG_YEARLY
FROM
    lineitem,
    part
WHERE
    P_PARTKEY = L_PARTKEY
    AND P_BRAND = 'Brand#23'
    AND P_CONTAINER = 'MED BAG'
    AND L_QUANTITY < (
        SELECT
            0.2 * AVG(L_QUANTITY)
        FROM
            lineitem
        WHERE
            L_PARTKEY = P_PARTKEY
    )
"""
tpch_queries[18] = """
SELECT
    C_NAME,
    C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE,
    SUM(L_QUANTITY)
FROM
    customer,
    orders,
    lineitem
WHERE
    O_ORDERKEY IN (
    SELECT
        L_ORDERKEY
    FROM
        lineitem
    GROUP BY
    L_ORDERKEY HAVING
        SUM(L_QUANTITY) > 300
    )
AND C_CUSTKEY = O_CUSTKEY
AND O_ORDERKEY = L_ORDERKEY
GROUP BY
    C_NAME,
    C_CUSTKEY,
    O_ORDERKEY,
    O_ORDERDATE,
    O_TOTALPRICE
ORDER BY
    O_TOTALPRICE DESC,
    O_ORDERDATE
"""
tpch_queries[19] = """
SELECT
    SUM(L_EXTENDEDPRICE* (1 - L_DISCOUNT)) AS REVENUE
FROM
    lineitem,
    part
WHERE
    (
    P_PARTKEY = L_PARTKEY
    AND P_BRAND = 'Brand#12'
    AND P_CONTAINER IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
    AND L_QUANTITY >= 1 AND L_QUANTITY <= 1 + 10
    AND P_SIZE BETWEEN 1 AND 5
    AND L_SHIPMODE IN ('AIR', 'AIR REG')
    AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )
    OR
    (
    P_PARTKEY = L_PARTKEY
    AND P_BRAND = 'Brand#23'
    AND P_CONTAINER IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
    AND L_QUANTITY >= 10 AND L_QUANTITY <= 10 + 10
    AND P_SIZE BETWEEN 1 AND 10
    AND L_SHIPMODE IN ('AIR', 'AIR REG')
    AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )
    OR
    (
    P_PARTKEY = L_PARTKEY
    AND P_BRAND = 'Brand#34'
    AND P_CONTAINER IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
    AND L_QUANTITY >= 20 AND L_QUANTITY <= 20 + 10
    AND P_SIZE BETWEEN 1 AND 15
    AND L_SHIPMODE IN ('AIR', 'AIR REG')
    AND L_SHIPINSTRUCT = 'DELIVER IN PERSON'
    )
"""
tpch_queries[2] = """
SELECT
    S_ACCTBAL,
    S_NAME,
    N_NAME,
    P_PARTKEY,
    P_MFGR,
    S_ADDRESS,
    S_PHONE,
    S_COMMENT
FROM
    part,
    supplier,
    partsupp,
    nation,
    region
WHERE
    P_PARTKEY = PS_PARTKEY
    AND S_SUPPKEY = PS_SUPPKEY
    AND P_SIZE = 15
    AND P_TYPE LIKE '%BRASS'
    AND S_NATIONKEY = N_NATIONKEY
    AND N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'EUROPE'
    AND PS_SUPPLYCOST = (
        SELECT
            MIN(PS_SUPPLYCOST)
        FROM
            partsupp,
            supplier,
            nation,
            region
        WHERE
            P_PARTKEY = PS_PARTKEY
            AND S_SUPPKEY = PS_SUPPKEY
            AND S_NATIONKEY = N_NATIONKEY
            AND N_REGIONKEY = R_REGIONKEY
            AND R_NAME = 'EUROPE'
        )
ORDER BY
    S_ACCTBAL DESC,
    N_NAME,
    S_NAME,
    P_PARTKEY
"""
tpch_queries[20] = """
SELECT
    S_NAME,
    S_ADDRESS
FROM
    supplier,
    nation
WHERE
    S_SUPPKEY IN (
    SELECT
        PS_SUPPKEY
    FROM
        partsupp
    WHERE
    PS_PARTKEY IN (
        SELECT
            P_PARTKEY
        FROM
            part
        WHERE
            P_NAME LIKE 'forest%'
    )
    AND PS_AVAILQTY > (
        SELECT
            0.5 * SUM(L_QUANTITY)
        FROM
            lineitem
        WHERE
            L_PARTKEY = PS_PARTKEY
        AND L_SUPPKEY = PS_SUPPKEY
        AND L_SHIPDATE >= '1994-01-01'
        AND L_SHIPDATE < '1995-01-01'
        )
    )
    AND S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'CANADA'
ORDER BY
    S_NAME
"""
tpch_queries[21] = """
SELECT
    S_NAME,
    COUNT(*) AS NUMWAIT
FROM
    supplier,
    lineitem AS L1,
    orders,
    nation
WHERE
    S_SUPPKEY = L1.L_SUPPKEY
    AND O_ORDERKEY = L1.L_ORDERKEY
    AND O_ORDERSTATUS = 'F'
    AND L1.L_RECEIPTDATE > L1.L_COMMITDATE
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem AS L2
        WHERE
            L2.L_ORDERKEY = L1.L_ORDERKEY
            AND L2.L_SUPPKEY <> L1.L_SUPPKEY
    )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            lineitem AS L3
        WHERE
            L3.L_ORDERKEY = L1.L_ORDERKEY
            AND L3.L_SUPPKEY <> L1.L_SUPPKEY
            AND L3.L_RECEIPTDATE > L3.L_COMMITDATE
        )
    AND S_NATIONKEY = N_NATIONKEY
    AND N_NAME = 'SAUDI ARABIA'
GROUP BY
    S_NAME
    ORDER BY
    NUMWAIT DESC,
    S_NAME
"""
tpch_queries[22] = """
SELECT
    CNTRYCODE,
    COUNT(*) AS NUMCUST,
    SUM(C_ACCTBAL) AS TOTACCTBAL
FROM
    (
    SELECT
        SUBSTR(C_PHONE, 1 , 2) AS CNTRYCODE,
        C_ACCTBAL
    FROM
        customer
    WHERE
        SUBSTR(C_PHONE , 1 , 2) IN
        ('20', '40', '22', '30', '39', '42', '21')
    AND C_ACCTBAL > (
            SELECT
                AVG(C_ACCTBAL)
            FROM
                customer
            WHERE
                C_ACCTBAL > 0.00
            AND SUBSTR(C_PHONE , 1 , 2) IN
            ('20', '40', '22', '30', '39', '42', '21')
    )
    AND NOT EXISTS (
        SELECT
            *
        FROM
            orders
        WHERE
            O_CUSTKEY = C_CUSTKEY
        )
    ) AS CUSTSALE
GROUP BY
    CNTRYCODE
ORDER BY
    CNTRYCODE
"""
tpch_queries[3] = """
SELECT
    L_ORDERKEY,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE,
    O_ORDERDATE,
    O_SHIPPRIORITY
FROM
    customer,
    orders,
    lineitem
WHERE
    C_MKTSEGMENT = 'BUILDING'
    AND C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND O_ORDERDATE < '1995-03-15'
    AND L_SHIPDATE > '1995-03-15'
GROUP BY
    L_ORDERKEY,
    O_ORDERDATE,
    O_SHIPPRIORITY
ORDER BY
    REVENUE DESC,
    O_ORDERDATE
"""
tpch_queries[4] = """
SELECT
    O_ORDERPRIORITY,
    COUNT(*) AS ORDER_COUNT
FROM
    orders
WHERE
    O_ORDERDATE >= '1993-07-01'
    AND O_ORDERDATE < '1993-10-01'
    AND EXISTS (
        SELECT
            *
        FROM
            lineitem
        WHERE
            L_ORDERKEY = O_ORDERKEY
            AND L_COMMITDATE < L_RECEIPTDATE
        )
GROUP BY
    O_ORDERPRIORITY
ORDER BY
    O_ORDERPRIORITY
"""
tpch_queries[5] = """
SELECT
    N_NAME,
    SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) AS REVENUE
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    C_CUSTKEY = O_CUSTKEY
    AND L_ORDERKEY = O_ORDERKEY
    AND L_SUPPKEY = S_SUPPKEY
    AND C_NATIONKEY = S_NATIONKEY
    AND S_NATIONKEY = N_NATIONKEY
    AND N_REGIONKEY = R_REGIONKEY
    AND R_NAME = 'ASIA'
    AND O_ORDERDATE >= '1990-01-01'
    AND O_ORDERDATE < '1995-01-01'
GROUP BY
    N_NAME
ORDER BY
    REVENUE DESC
"""
tpch_queries[6] = """
SELECT
    SUM(L_EXTENDEDPRICE * L_DISCOUNT) AS REVENUE
FROM
    lineitem
WHERE
    L_SHIPDATE >= '1994-01-01'
    AND L_SHIPDATE < '1995-01-01'
    AND L_DISCOUNT BETWEEN .06 - 0.01 AND .06 + 0.01
    AND L_QUANTITY < 24
"""
tpch_queries[7] = """
SELECT
    SUPP_NATION,
    CUST_NATION,
    L_YEAR,
    SUM(VOLUME) AS REVENUE
FROM
    (
    SELECT
        N1.N_NAME AS SUPP_NATION,
        N2.N_NAME AS CUST_NATION,
        strftime('%Y', L_SHIPDATE) AS L_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME
    FROM
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
    WHERE
        S_SUPPKEY = L_SUPPKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND C_CUSTKEY = O_CUSTKEY
        AND S_NATIONKEY = N1.N_NATIONKEY
        AND C_NATIONKEY = N2.N_NATIONKEY
        AND (
            (N1.N_NAME = 'JAPAN' AND N2.N_NAME = 'INDIA')
            OR (N1.N_NAME = 'INDIA' AND N2.N_NAME = 'JAPAN')
            )
        AND L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31'
    ) AS SHIPPING
GROUP BY
    SUPP_NATION,
    CUST_NATION,
    L_YEAR
ORDER BY
    SUPP_NATION,
    CUST_NATION,
    L_YEAR
"""
tpch_queries[8] = """
SELECT
    O_YEAR,
    SUM(CASE WHEN NATION = 'INDIA' THEN VOLUME ELSE 0 END) / SUM(VOLUME) AS MKT_SHARE
FROM
    (
    SELECT
        strftime('%Y', O_ORDERDATE) AS O_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME,
        N2.N_NAME AS NATION
    FROM
        part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
    WHERE
        P_PARTKEY = L_PARTKEY
        AND S_SUPPKEY = L_SUPPKEY
        AND L_ORDERKEY = O_ORDERKEY
        AND O_CUSTKEY = C_CUSTKEY
        AND C_NATIONKEY = N1.N_NATIONKEY
        AND N1.N_REGIONKEY = R_REGIONKEY
        AND R_NAME = 'ASIA'
        AND S_NATIONKEY = N2.N_NATIONKEY
        AND O_ORDERDATE BETWEEN '1995-01-01' AND '1996-12-31'
        AND P_TYPE = 'SMALL PLATED COPPER'
    ) AS ALL_NATIONS
GROUP BY
    O_YEAR
    ORDER BY
    O_YEAR
"""
tpch_queries[9] = """
SELECT
    NATION,
    O_YEAR,
    SUM(AMOUNT) AS SUM_PROFIT
FROM
    (
    SELECT
        N_NAME AS NATION,
        strftime('%Y', O_ORDERDATE) AS O_YEAR,
        L_EXTENDEDPRICE * (1 - L_DISCOUNT) - PS_SUPPLYCOST * L_QUANTITY AS AMOUNT
    FROM
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    WHERE
        S_SUPPKEY = L_SUPPKEY
        AND PS_SUPPKEY = L_SUPPKEY
        AND PS_PARTKEY = L_PARTKEY
        AND P_PARTKEY = L_PARTKEY
        AND O_ORDERKEY = L_ORDERKEY
        AND S_NATIONKEY = N_NATIONKEY
        AND P_NAME LIKE '%dim%'
    ) AS PROFIT
GROUP BY
    NATION,
    O_YEAR
ORDER BY
    NATION,
    O_YEAR DESC
"""
