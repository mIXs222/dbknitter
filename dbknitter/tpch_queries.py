""" TPC-H SQL Queries """

tpch_queries_sql = {}
tpch_queries_sql[1] = """
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
tpch_queries_sql[10] = """
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
tpch_queries_sql[11] = """
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
tpch_queries_sql[12] = """
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
tpch_queries_sql[13] = """
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
tpch_queries_sql[14] = """
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
tpch_queries_sql[15] = """
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
tpch_queries_sql[16] = """
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
tpch_queries_sql[17] = """
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
tpch_queries_sql[18] = """
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
tpch_queries_sql[19] = """
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
tpch_queries_sql[2] = """
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
tpch_queries_sql[20] = """
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
tpch_queries_sql[21] = """
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
tpch_queries_sql[22] = """
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
tpch_queries_sql[3] = """
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
tpch_queries_sql[4] = """
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
tpch_queries_sql[5] = """
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
tpch_queries_sql[6] = """
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
tpch_queries_sql[7] = """
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
tpch_queries_sql[8] = """
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
tpch_queries_sql[9] = """
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


""" Official English Descriptions from TPC-H Specification """

tpch_queries_eng_official = {}

tpch_queries_eng_official[1] = r"""The Pricing Summary Report Query provides a summary pricing report for all lineitems shipped before 1998-09-02. The query lists totals for quantity, extended price, discounted extended price, discounted extended price plus tax, average quantity, average extended price, and average discount. These aggregates are grouped by RETURNFLAG and LINESTATUS, and listed in ascending order of RETURNFLAG and LINESTATUS. A count of the number of lineitems in each group is included."""
tpch_queries_eng_official[2] = r"""The Minimum Cost Supplier Query finds, in EUROPE region, for each part of a BRASS type and size of 15, the supplier who can supply it at minimum cost. If several suppliers in that EUROPE offers the BRASS part and size of 15 at the same (minimum) cost, the query lists the suppliers with the order by account balance in descending order, and nation name, supplier name, part key in ascending order. For each supplier, the query lists the supplier's account balance, name and nation; the part's number and manufacturer; the supplier's address, phone number and comment information."""
tpch_queries_eng_official[3] = r"""The Shipping Priority Query retrieves the shipping priority and potential revenue, defined as the sum of l_extendedprice * (1-l_discount), of the orders having the largest revenue among those that had not been shipped as of a given date of 1995-03-15 and where market segment is BUILDING. Orders are listed in decreasing order of revenue."""
tpch_queries_eng_official[4] = r"""The Order Priority Checking Query counts the number of orders ordered between 1993-07-01 and 1993-10-01 in which at least one lineitem was received by the customer later than its committed date. The query lists the count of such orders for each order priority sorted in ascending priority order."""
tpch_queries_eng_official[5] = r"""The Local Supplier Volume Query lists for each nation in ASIA region the revenue volume that resulted from lineitem transactions in which the customer ordering parts and the supplier filling them were both within ASIA. The query is run in order to determine whether to institute local distribution centers in ASIA. The query considers only parts ordered between 1990-01-01 and 1995-01-01. The query displays the nations and revenue volume in descending order by revenue. Revenue volume for all qualifying lineitems in a particular nation is defined as sum(l_extendedprice * (1 - l_discount))."""
tpch_queries_eng_official[6] = r"""The Forecasting Revenue Change Query considers all the lineitems shipped between 1994-01-01 and 1995-01-01 with discounts between .06 - 0.01 and .06 + 0.01. The query lists the amount by which the total revenue would have increased if these discounts had been eliminated for lineitems with l_quantity less than 24. Note that the potential revenue increase is equal to the sum of [l_extendedprice * l_discount] for all lineitems with discounts and quantities in the qualifying range."""
tpch_queries_eng_official[7] = r"""The Volume Shipping Query finds, for two given nations which are INDIA and JAPAN, the gross discounted revenues derived from lineitems in which parts were shipped from a supplier in either nation to a customer in the other nation during 1995 and 1996. The query lists the supplier nation, the customer nation, the year, and the revenue from shipments that took place in that year. The query orders the answer by Supplier nation, Customer nation, and year (all ascending)."""
tpch_queries_eng_official[8] = r"""This query determines how the market share of INDA within ASIA has changed over two years for SMALL PLATED COPPER., The market share for INDA within ASIA is defined as the fraction of the revenue, the sum of [l_extendedprice * (1-l_discount)], from the products of SMALL PLATED COPPER in ASIA that was supplied by suppliers from INDIA. The query determines this for the years 1995 and 1996 presented in this order."""
tpch_queries_eng_official[9] = r"""This query determines how much profit is made on a given line of parts, broken out by supplier nation and year., The Product Type Profit Measure Query finds, for each nation and each year, the profit for all parts ordered in that year that contain a specified dim in their names and that were filled by a supplier in that nation. The profit is defined as the sum of [(l_extendedprice*(1-l_discount)) - (ps_supplycost * l_quantity)] for all lineitems describing parts in the specified line. The query lists the nations in ascending alphabetical order and, for each nation, the year and profit in descending order by year (most recent first)."""
tpch_queries_eng_official[10] = r"""The query identifies customers who might be having problems with the parts that are shipped to them., The Returned Item Reporting Query finds the customers, in terms of their effect on lost revenue betwen  1993-10-01 and 1994-01-01, who have returned parts. The query considers only parts that were ordered in the specified quarter. The query lists the customer's name, address, nation, phone number, account balance, comment information and revenue lost. The customers are listed in descending order of lost revenue, customer key, customer name and customer account balance. Revenue lost is defined as sum(l_extendedprice*(1-l_discount)) for all qualifying lineitems., """
tpch_queries_eng_official[11] = r"""This query finds the most important subset of suppliers' stock in GERMANY., The Important Stock Identification Query finds, from scanning the available stock of suppliers in GERMANY, all the parts that represent a significant percentage of the total value of all available parts. The query displays the part number and the value of those parts in descending order of value."""
tpch_queries_eng_official[12] = r"""This query determines whether selecting less expensive modes of shipping is negatively affecting the critical-priority orders by causing more parts to be received by customers after the committed date. The Shipping Modes and Order Priority Query counts, by ship mode which is MAIL and SHIP, for lineitems actually received by customers between 1994-01-01 and 1995-01-01 which is the receipt date, the number of lineitems belonging to orders for which the l_receiptdate exceeds the l_commitdate for two different specified ship modes. Only lineitems that were actually shipped before the l_commitdate are considered. The late lineitems are partitioned into two groups, those with priority URGENT or HIGH, and those with a priority other than URGENT or HIGH."""
tpch_queries_eng_official[13] = r"""This query seeks relationships between customers and the size of their orders. This query determines the distribution of customers by the number of orders they have made, including customers who have no record of orders, past or present. It counts and reports how many customers have no orders, how many have 1, 2, 3, etc. A check is made to ensure that the orders counted do not fall into pending or do not fall into deposits. Special categories are identified in the order comment column whose pattern is neither "pending" nor "depoists"."""
tpch_queries_eng_official[14] = r"""This query monitors the market response to a promotion such as TV advertisements or a special campaign. The Promotion Effect Query determines what percentage of the revenue between 1995-09-01 and 1995-10-01 was derived from promotional parts. The query considers only parts actually shipped and gives the percentage. Revenue is defined as (l_extendedprice * (1-l_discount))."""
tpch_queries_eng_official[15] = r"""This query determines the top supplier so it can be rewarded, given more business, or identified for special recognition. The Top Supplier Query finds the supplier who contributed the most to the overall revenue for parts shipped between 1996-01-01 and 1996-04-01. In case of a tie, the query lists all suppliers whose contribution was equal to the maximum, presented in supplier number order."""
tpch_queries_eng_official[16] = r"""This query finds out how many suppliers can supply parts with given attributes. It might be used, for example, to determine whether there is a sufficient number of suppliers for heavily ordered parts., The Parts/Supplier Relationship Query counts the number of suppliers who can supply parts that satisfy a particular customer's requirements which does not have a brand id of 45. The customer is interested in parts of eight different sizes which are one of 49, 14, 23, 45, 19, 3, 36, or 9, as long as they are not of MEDIUM POLISHED, not of a brand 45, and not from a supplier who has had complaints registered at the Better Business Bureau. Results must be presented in descending count and ascending brand, type, and size."""
tpch_queries_eng_official[17] = r""" This query determines how much average yearly revenue would be lost if orders were no longer filled for small quantities of certain parts. This may reduce overhead expenses by concentrating sales on larger shipments., The Small-Quantity-Order Revenue Query considers parts of a brand 23 and with MED BAG and determines the average lineitem quantity of such parts ordered for all orders (past and pending) in the 7-year database. What would be the average yearly gross (undiscounted) loss in revenue if orders for these parts with a quantity of less than 20% of this average were no longer taken?"""
tpch_queries_eng_official[18] = r"""The Large Volume Customer Query ranks customers based on their having placed a large quantity order. Large quantity orders are defined as those orders whose total quantity is above a certain level., The Large Volume Customer Query finds a list of the customers who have ever placed quantity orders larger than 300. The query lists the customer name, customer key, the order key, date and total price and the quantity for the order."""
tpch_queries_eng_official[19] = r"""The Discounted Revenue query finds the gross discounted revenue for all orders for three different types of parts. Parts are selected based on the combination of specific brands, a list of containers, and a range of sizes. The first type has a brand id of 12, a container of 'SM CASE', 'SM BOX' 'SM PACK', or 'SM PKG', quantity which is greater than or eqaul to 1 and less than or equal to 11, size between 1 and 5. The second type has a brand id of 23, a container of 'MED BAG', 'MED BOX', 'MED PKG', or 'MED PACK', quantity which is greater than or eqaul to 10 and less than or equal to 20, size between 1 and 10. The third type has a brand id of 34, a container of 'LG CASE', 'LG BOX', 'LG PACK', or 'LG PKG', quantity which is greater than or eqaul to 20 and less than or equal to 30, size between 1 and 15. All should be shipped by air whose SHIPMODE is either 'AIR' or 'AIR REG' and delivered in person."""
tpch_queries_eng_official[20] = r"""The Potential Part Promotion query identifies suppliers who have an excess of a forest part; an excess is defined to be more than 50% of the parts like the forest part that the supplier shipped between 1994-01-01 and 1995-01-01 for CANADA. Only parts whose names share a certain naming convention are considered."""
tpch_queries_eng_official[21] = r"""The Suppliers Who Kept Orders Waiting query identifies suppliers, for a nation which is SAUDI ARABIA, whose product was part of a multi-supplier order (with current status of 'F') where they were the only supplier who failed to meet the committed delivery date."""
tpch_queries_eng_official[22] = r"""The Global Sales Opportunity Query identifies geographies where there are customers who may be likely to make a purchase. This query counts how many customers within a specific range of country codes have not placed orders for 7 years whose average account balance is greater than 0.00. It also reflects the magnitude of that balance. Country code is defined as the first two characters of c_phone which should be one of '20', '40', '22', '30', '39', '42', or '21'. """


""" Manual English Translation from TPC-H Query and Specification """

tpch_queries_eng_manual = {}
tpch_queries_eng_manual[1] = r"""The Pricing Summary Report Query provides a summary pricing report for all lineitems shipped before 1998-09-02. The query lists totals for quantity, extended price, discounted extended price, discounted extended price plus tax, average quantity, average extended price, and average discount. These aggregates are grouped by RETURNFLAG and LINESTATUS, and listed in ascending order of RETURNFLAG and LINESTATUS. A count of the number of lineitems in each group is included."""
tpch_queries_eng_manual[2] = r"""The Minimum Cost Supplier Query finds, in EUROPE region, for each part of a BRASS type and size of 15, the supplier who can supply it at minimum supply cost. If several suppliers in that EUROPE offers the BRASS part and size of 15 at the same (minimum) cost, the query lists the suppliers with the order by account balance in descending order, and nation name, supplier name, part key in ascending order. For each supplier, the query lists the supplier's account balance, name and nation; the part's number and manufacturer; the supplier's address, phone number and comment information. Please output the columns in the order of N_NAME, P_MFGR, P_PARTKEY, S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_PHONE."""
tpch_queries_eng_manual[3] = r"""The Shipping Priority Query retrieves the shipping priority and potential revenue, defined as the sum of l_extendedprice * (1-l_discount), of the orders having the largest revenue among those that had not been shipped as of a given date of 1995-03-15 and where market segment is BUILDING. Orders are listed in decreasing order of revenue. Please output the columns in the order of O_ORDERKEY, REVENUE, O_ORDERDATE, and O_SHIPPRIORITY."""
tpch_queries_eng_manual[4] = r"""The Order Priority Checking Query counts the number of orders ordered between 1993-07-01 and 1993-10-01 in which at least one lineitem was received by the customer later than its committed date. The query lists the count of such orders for each order priority sorted in ascending priority order. Please output the columns in the order of ORDER_COUNT and O_ORDERPRIORITY"""
tpch_queries_eng_manual[5] = r"""The Local Supplier Volume Query lists for each nation in ASIA region the revenue volume that resulted from lineitem transactions in which the customer ordering parts and the supplier filling them were both within ASIA. The query is run in order to determine whether to institute local distribution centers in ASIA. The query considers only parts ordered between 1990-01-01 and 1995-01-01. The query displays the nations and revenue volume in descending order by revenue. Revenue volume for all qualifying lineitems in a particular nation is defined as sum(l_extendedprice * (1 - l_discount)). Please output the columns in the order of N_NAME and REVENUE."""
tpch_queries_eng_manual[6] = r"""The Forecasting Revenue Change Query considers all the lineitems shipped between 1994-01-01 and 1995-01-01 with discounts between .06 - 0.01 and .06 + 0.01. The query lists the amount by which the total revenue would have increased if these discounts had been eliminated for lineitems with l_quantity less than 24. Note that the potential revenue increase is equal to the sum of [l_extendedprice * l_discount] for all lineitems with discounts and quantities in the qualifying range. Please output the columns in the order of REVENUE."""
tpch_queries_eng_manual[7] = r"""The Volume Shipping Query finds, for two given nations which are INDIA and JAPAN, the gross discounted revenues derived from lineitems in which parts were shipped from a supplier in either nation to a customer in the other nation during 1995 and 1996. The query lists the supplier nation, the customer nation, the year, and the revenue from shipments that took place in that year. The query orders the answer by Supplier nation, Customer nation, and year (all ascending). Please output the columns in the order of CUST_NATION, L_YEAR, REVENUE, SUPP_NATION"""
tpch_queries_eng_manual[8] = r"""This query determines how the market share of INDA within ASIA has changed over two years for SMALL PLATED COPPER., The market share for INDA within ASIA is defined as the fraction of the revenue, the sum of [l_extendedprice * (1-l_discount)], from the products of SMALL PLATED COPPER in ASIA that was supplied by suppliers from INDIA. The query determines this for the years 1995 and 1996 presented in this order. """
tpch_queries_eng_manual[9] = r"""This query determines how much profit is made on a given line of parts, broken out by supplier nation and year., The Product Type Profit Measure Query finds, for each nation and each year, the profit for all parts ordered in that year that contain a specified dim in their names and that were filled by a supplier in that nation. The profit is defined as the sum of [(l_extendedprice*(1-l_discount)) - (ps_supplycost * l_quantity)] for all lineitems describing parts in the specified line. The query lists the nations in ascending alphabetical order and, for each nation, the year and profit in descending order by year (most recent first)."""
tpch_queries_eng_manual[10] = r"""The query identifies customers who might be having problems with the parts that are shipped to them., The Returned Item Reporting Query finds the customers, in terms of their effect on lost revenue betwen  1993-10-01 and 1994-01-01, who have returned parts. The query considers only parts that were ordered in the specified quarter. The query lists the customer's name, address, nation, phone number, account balance, comment information and revenue lost. The customers are listed in descending order of lost revenue, customer key, customer name and customer account balance. Revenue lost is defined as sum(l_extendedprice*(1-l_discount)) for all qualifying lineitems., """
tpch_queries_eng_manual[11] = r"""This query finds the most important subset of suppliers' stock in GERMANY., The Important Stock Identification Query finds, from scanning the available stock of suppliers in GERMANY, all the parts that represent a significant percentage of the total value of all available parts. The query displays the part number and the value of those parts in descending order of value."""
tpch_queries_eng_manual[12] = r"""This query determines whether selecting less expensive modes of shipping is negatively affecting the critical-priority orders by causing more parts to be received by customers after the committed date. The Shipping Modes and Order Priority Query counts, by ship mode which is MAIL and SHIP, for lineitems actually received by customers between 1994-01-01 and 1995-01-01 which is the receipt date, the number of lineitems belonging to orders for which the l_receiptdate exceeds the l_commitdate for two different specified ship modes. Only lineitems that were actually shipped before the l_commitdate are considered. The late lineitems are partitioned into two groups, those with priority URGENT or HIGH, and those with a priority other than URGENT or HIGH."""
tpch_queries_eng_manual[13] = r"""This query seeks relationships between customers and the size of their orders. This query determines the distribution of customers by the number of orders they have made, including customers who have no record of orders, past or present. It counts and reports how many customers have no orders, how many have 1, 2, 3, etc. A check is made to ensure that the orders counted do not fall into pending or do not fall into deposits. Special categories are identified in the order comment column whose pattern is neither "pending" nor "depoists"."""
tpch_queries_eng_manual[14] = r"""This query monitors the market response to a promotion such as TV advertisements or a special campaign. The Promotion Effect Query determines what percentage of the revenue between 1995-09-01 and 1995-10-01 was derived from promotional parts. The query considers only parts actually shipped and gives the percentage. Revenue is defined as (l_extendedprice * (1-l_discount))."""
tpch_queries_eng_manual[15] = r"""This query determines the top supplier so it can be rewarded, given more business, or identified for special recognition. The Top Supplier Query finds the supplier who contributed the most to the overall revenue for parts shipped between 1996-01-01 and 1996-04-01. In case of a tie, the query lists all suppliers whose contribution was equal to the maximum, presented in supplier number order."""
tpch_queries_eng_manual[16] = r"""This query finds out how many suppliers can supply parts with given attributes. It might be used, for example, to determine whether there is a sufficient number of suppliers for heavily ordered parts., The Parts/Supplier Relationship Query counts the number of suppliers who can supply parts that satisfy a particular customer's requirements which does not have a brand id of 45. The customer is interested in parts of eight different sizes which are one of 49, 14, 23, 45, 19, 3, 36, or 9, as long as they are not of MEDIUM POLISHED, not of a brand 45, and not from a supplier who has had complaints registered at the Better Business Bureau. Results must be presented in descending count and ascending brand, type, and size."""
tpch_queries_eng_manual[17] = r""" This query determines how much average yearly revenue would be lost if orders were no longer filled for small quantities of certain parts. This may reduce overhead expenses by concentrating sales on larger shipments., The Small-Quantity-Order Revenue Query considers parts of a brand 23 and with MED BAG and determines the average lineitem quantity of such parts ordered for all orders (past and pending) in the 7-year database. What would be the average yearly gross (undiscounted) loss in revenue if orders for these parts with a quantity of less than 20% of this average were no longer taken?"""
tpch_queries_eng_manual[18] = r"""The Large Volume Customer Query ranks customers based on their having placed a large quantity order. Large quantity orders are defined as those orders whose total quantity is above a certain level., The Large Volume Customer Query finds a list of the customers who have ever placed quantity orders larger than 300. The query lists the customer name, customer key, the order key, date and total price and the quantity for the order."""
tpch_queries_eng_manual[19] = r"""The Discounted Revenue query finds the gross discounted revenue for all orders for three different types of parts. Parts are selected based on the combination of specific brands, a list of containers, and a range of sizes. The first type has a brand id of 12, a container of 'SM CASE', 'SM BOX' 'SM PACK', or 'SM PKG', quantity which is greater than or eqaul to 1 and less than or equal to 11, size between 1 and 5. The second type has a brand id of 23, a container of 'MED BAG', 'MED BOX', 'MED PKG', or 'MED PACK', quantity which is greater than or eqaul to 10 and less than or equal to 20, size between 1 and 10. The third type has a brand id of 34, a container of 'LG CASE', 'LG BOX', 'LG PACK', or 'LG PKG', quantity which is greater than or eqaul to 20 and less than or equal to 30, size between 1 and 15. All should be shipped by air whose SHIPMODE is either 'AIR' or 'AIR REG' and delivered in person."""
tpch_queries_eng_manual[20] = r"""The Potential Part Promotion query identifies suppliers who have an excess of a forest part; an excess is defined to be more than 50% of the parts like the forest part that the supplier shipped between 1994-01-01 and 1995-01-01 for CANADA. Only parts whose names share a certain naming convention are considered."""
tpch_queries_eng_manual[21] = r"""The Suppliers Who Kept Orders Waiting query identifies suppliers, for a nation which is SAUDI ARABIA, whose product was part of a multi-supplier order (with current [status] of 'F') where they were the only supplier who failed to meet the committed delivery date. Please output NUMWAIT and S_NAME columne in the order of NUMWAIT descending."""
tpch_queries_eng_manual[22] = r"""The Global Sales Opportunity Query identifies geographies where there are customers who may be likely to make a purchase. This query counts how many customers within a specific range of country codes have not placed orders for 7 years whose average account balance is greater than 0.00. It also reflects the magnitude of that balance. Country code is defined as the first two characters of c_phone which should be one of '20', '40', '22', '30', '39', '42', or '21'. Please output the columns in the order of CNTRYCODE, NUMCUST, and TOTACCTBAL. It should be by in the order of CNTRYCODE ascending"""


""" GPT's English Translation from TPC-H Query and Specification """

tpch_queries_eng_gpt = {}

tpch_queries_eng_gpt[1] = r"""Conduct a detailed analysis of line items' information based on specific criteria. The analysis focuses on line items with a shipping date on or before September 2, 1998. The results are organized and summarized based on the return flag and line status.

For each unique combination of return flag and line status, various aggregate calculations are performed:

The total quantity of items ('SUM_QTY').
The total base price, calculated as the sum of extended prices ('SUM_BASE_PRICE').
The total discounted price, considering both extended price and discount ('SUM_DISC_PRICE').
The total charge, factoring in tax as well ('SUM_CHARGE').
The average quantity per line item ('AVG_QTY').
The average extended price per line item ('AVG_PRICE').
The average discount per line item ('AVG_DISC').
The total count of line items ('COUNT_ORDER').
The results are then grouped by return flag and line status to provide insights into the distribution of these aggregates based on those attributes. The final presentation orders the results in ascending order based on return flag and line status, offering a detailed and organized summary of line item information meeting the specified shipping date criteria."""

tpch_queries_eng_gpt[2] = r"""Retrieve detailed information about suppliers, parts, and nations meeting specific criteria within the 'EUROPE' region. The analysis focuses on parts with a size of 15 and a type containing 'BRASS.' Additionally, the suppliers of these parts are located in the 'EUROPE' region. The results include the account balance, name, address, phone, and comments of the suppliers, as well as details about the parts, such as part key, manufacturer, and size.

Data is sourced from the 'part,' 'supplier,' 'partsupp,' 'nation,' and 'region' tables, establishing relationships between parts, suppliers, partsuppliers, nations, and regions. The criteria for selection involve matching part and supplier keys, part size and type, supplier nation and region, and the minimum supply cost within the 'EUROPE' region.

The results are ordered in descending order based on the supplier's account balance, and then in ascending order based on nation name, supplier name, and part key. This ordering provides a comprehensive view of account balances among suppliers, organized by region, nation, supplier name, and part key."""

tpch_queries_eng_gpt[3] = r"""Conduct a thorough analysis of revenue generated from orders placed by customers in the 'BUILDING' market segment. The analysis focuses on orders placed before March 15, 1995, with associated line items shipped after March 15, 1995. The computation of revenue involves summing the extended price of line items after applying relevant discounts.

Data is sourced from the 'customer,' 'orders,' and 'lineitem' tables, establishing relationships between customers, orders, and line items. Specifically, the analysis includes only orders where the customer's market segment is 'BUILDING,' the order and shipping dates meet the specified criteria, and the customer and order keys are appropriately matched.

The results are then grouped by order key, order date, and shipping priority. This grouping allows for a detailed breakdown of revenue at the granularity of individual orders, order dates, and shipping priorities. Subsequently, the presentation orders the results in descending order based on revenue and in ascending order based on the order date, providing a comprehensive overview of revenue dynamics for 'BUILDING' segment customers."""
tpch_queries_eng_gpt[4] = r"""Retrieve detailed insights into order priorities during a specific timeframe from July 1, 1993, to October 1, 1993. The analysis considers not just the number of orders but incorporates a nuanced criterion. Only orders with associated line items where the commitment date precedes the receipt date are included in the count.

For each order priority, calculate the count of such orders meeting the aforementioned conditions. This detailed examination aims to provide a granular understanding of how order priorities are distributed, taking into account the temporal constraints and the commitment-receipt date relationship of associated line items.

The final presentation orders the results in ascending order based on the order priority, offering a clear and insightful portrayal of order priorities and their counts within the specified timeframe."""
tpch_queries_eng_gpt[5] = r"""Retrieve a detailed breakdown of the total revenue generated by orders placed by customers in the 'ASIA' region. This analysis spans the time period from January 1, 1990, to December 31, 1994. The calculation of revenue considers the extended price of line items, accounting for applicable discounts.

To achieve this, data is drawn from the 'customer,' 'orders,' 'lineitem,' 'supplier,' 'nation,' and 'region' tables. Relationships are established between customers, orders, line items, suppliers, and their respective nations and regions. Only data related to the 'ASIA' region is considered, determined by matching nation and region keys. The time frame is further constrained by including only orders placed between January 1, 1990, and December 31, 1994.

The results are then grouped by nation name, facilitating a comprehensive understanding of revenue generation across different nations within the 'ASIA' region. The grouping is based on the total revenue for each nation, calculated by summing the extended price of line items after discount adjustments.

The final presentation orders the results in descending order based on revenue, providing a detailed breakdown showcasing the nations in the 'ASIA' region, their respective total revenues, and how they contribute to the overall revenue picture during the specified timeframe."""
tpch_queries_eng_gpt[6] = r"""Compute the total revenue generated from line items that meet specific criteria. The analysis focuses on line items with shipping dates between January 1, 1994, and December 31, 1994. Furthermore, the criteria include line items with a discount falling within a narrow range of 5% (0.06 - 0.01) to 7% (0.06 + 0.01) and a quantity less than 24.

The computation involves summing the extended price of line items after applying the specified discount criteria. The selected line items are those meeting the conditions of having a shipping date within the designated timeframe, a discount within the specified range, and a quantity less than 24.

This detailed analysis aims to provide a precise understanding of the revenue generated during the specified period, considering specific discount and quantity constraints on line items."""
tpch_queries_eng_gpt[7] = r"""Generate a detailed report of revenue based on the interactions between suppliers and customers across different nations. The analysis spans the years between 1995 and 1996. The report includes the supplier and customer nations, the year of shipping, and the calculated revenue volume.

To achieve this, data is extracted from the 'supplier,' 'lineitem,' 'orders,' 'customer,' and 'nation' tables. The relationship between suppliers, line items, orders, customers, and their respective nations is established. Specifically, the nations involved are 'JAPAN' and 'INDIA,' considering both possible pairs: ('JAPAN' as the supplier nation and 'INDIA' as the customer nation, and vice versa).

The timeframe of interest for the line items is set between January 1, 1995, and December 31, 1996. For each line item, the revenue volume is computed as the extended price adjusted for the discount.

The results are then grouped by supplier nation, customer nation, and year of shipping. This detailed grouping provides insights into how revenue is distributed over the specified period across different nation pairs and shipping years.

Finally, the presentation orders the results in ascending order based on the supplier nation, customer nation, and year of shipping, offering a comprehensive overview of revenue dynamics between suppliers and customers in 'JAPAN' and 'INDIA' during the specified timeframe"""
tpch_queries_eng_gpt[8] = r"""Conduct a thorough analysis of market share for a specific type of part, 'SMALL PLATED COPPER,' within the 'ASIA' region. The analysis spans the years between 1995 and 1996. The report includes the year of the order and the calculated market share, specifically focusing on the nation 'INDIA.'

To achieve this, data is derived from the 'part,' 'supplier,' 'lineitem,' 'orders,' 'customer,' 'nation,' and 'region' tables. Relationships are established between parts, suppliers, line items, orders, customers, and their respective nations and regions. The focus is on the 'ASIA' region and the specific part type 'SMALL PLATED COPPER.'

For each order, the volume is calculated as the extended price of line items adjusted for discounts. The volume is associated with the respective nation, considering the 'INDIA' nation. The results are then grouped by the year of the order.

The market share is computed by summing the volumes associated with 'INDIA' and dividing it by the total volume. This provides a detailed understanding of the market share dynamics for the specified part type within the 'ASIA' region, specifically highlighting the contribution of 'INDIA' over the years 1995 and 1996.

Finally, the presentation orders the results in ascending order based on the year of the order, offering a comprehensive overview of market share trends for the 'SMALL PLATED COPPER' part in the 'ASIA' region during the specified timeframe."""
tpch_queries_eng_gpt[9] = r"""Perform an in-depth analysis of profit distribution across different nations over the years, focusing specifically on parts containing the term 'dim.' The analysis spans multiple tables, including 'part,' 'supplier,' 'lineitem,' 'partsupp,' 'orders,' and 'nation.'

For each relevant line item, the amount of profit is calculated, considering the extended price after discount adjustments and subtracting the supply cost multiplied by the quantity. This detailed computation is conducted for parts with names containing the term 'dim.' The results are then associated with the respective nation and year of the order.

The data is grouped by nation and year, facilitating a detailed breakdown of profit distribution over time across different nations. The final presentation orders the results in ascending order based on the nation and in descending order based on the year of the order, offering a comprehensive overview of profit dynamics associated with parts containing 'dim' across various nations."""
tpch_queries_eng_gpt[10] = r"""Conduct a comprehensive analysis of customer information and associated revenue during a specific time frame. The analysis focuses on orders placed between October 1, 1993, and December 31, 1993, where the line items were marked with a return flag 'R.' The results aim to provide detailed insights into the revenue generated by customers meeting these criteria.

Data is sourced from the 'customer,' 'orders,' 'lineitem,' and 'nation' tables, establishing relationships between customers, orders, line items, and nations. The criteria for selection involve matching customer and order keys, considering specific order date constraints, line items marked for return, and associating the customer with their nation.

For each unique customer, the analysis includes:

Customer key ('C_CUSTKEY').
Customer name ('C_NAME').
Total revenue generated, calculated as the sum of extended prices adjusted for discounts ('REVENUE').
Customer account balance ('C_ACCTBAL').
Nation name ('N_NAME').
Customer address ('C_ADDRESS').
Customer phone number ('C_PHONE').
Customer comments ('C_COMMENT').
The results are then grouped by various customer attributes, including customer key, name, account balance, phone number, nation name, address, and comments. This grouping allows for a detailed breakdown of revenue and customer information. Finally, the presentation orders the results in ascending order based on revenue, customer key, name, and in descending order based on the account balance, providing a comprehensive view of customer revenue and associated details during the specified time frame."""
tpch_queries_eng_gpt[11] = r"""Perform a detailed analysis of parts and their associated values from suppliers located in Germany. The analysis considers the supply cost and available quantity for each part. The results aim to identify parts where the total value (calculated as the sum of the supply cost multiplied by the available quantity) exceeds a certain threshold.

To achieve this, data is drawn from the 'partsupp,' 'supplier,' and 'nation' tables, establishing relationships between parts, suppliers, and nations. The focus is specifically on suppliers in Germany ('N_NAME' is 'GERMANY').

The results are grouped by part key, and a filtering condition ('HAVING') is applied to select only those groups where the sum of the supply cost multiplied by the available quantity surpasses a certain percentage of the overall value for Germany-based suppliers. This threshold is calculated in a subquery.

The final presentation orders the results in descending order based on the calculated value, providing insights into the parts with the highest values that meet the specified conditions for suppliers in Germany."""
tpch_queries_eng_gpt[12] = r"""Perform a detailed analysis of line items based on their shipping modes and the priority of associated orders. The analysis distinguishes between high-priority ('1-URGENT' and '2-HIGH') and low-priority orders. The results aim to provide a count of line items for each shipping mode falling into these priority categories.

Data is sourced from the 'orders' and 'lineitem' tables, establishing a relationship between orders and their corresponding line items. The criteria for selection include matching order and line item keys, specific shipping modes ('MAIL' and 'SHIP'), and ensuring that the commitment date is earlier than the receipt date, the shipping date is earlier than the commitment date, and the receipt date falls between January 1, 1994, and December 31, 1994.

Two counts are calculated for each shipping mode:

'HIGH_LINE_COUNT': The count of line items associated with orders marked as '1-URGENT' or '2-HIGH' priority.
'LOW_LINE_COUNT': The count of line items associated with orders not marked as '1-URGENT' or '2-HIGH' priority.
The results are then grouped by shipping mode, offering insights into the distribution of line items based on shipping modes and order priorities. The final presentation orders the results in ascending order based on the shipping mode, providing a comprehensive view of line item counts categorized by shipping mode and order priority."""
tpch_queries_eng_gpt[13] = r"""Conduct a thorough analysis of customer order counts and their distribution. The analysis distinguishes between customers who have placed orders with specific conditions and those who haven't. The results aim to provide insights into the distribution of customers based on the count of their orders and the presence of certain keywords in the order comments.

Data is sourced from the 'customer' and 'orders' tables, utilizing a left outer join to ensure all customers are included, regardless of whether they have placed orders or not. The conditions for joining include matching customer and order keys, and the exclusion of orders with comments containing the phrases 'pending' and 'deposits.'

Within the subquery ('C_ORDERS'), for each customer, the count of orders meeting the specified conditions is calculated.

The outer query then groups the results by the count of orders per customer ('C_COUNT'). For each count, it calculates the number of customers ('CUSTDIST') with that specific count of orders. The results provide a distribution of customers based on their order counts.

Finally, the presentation orders the results in descending order based on the count of customers ('CUSTDIST') and, in the case of ties, in descending order based on the count of orders per customer ('C_COUNT'). This ordering offers a comprehensive view of the distribution of customers based on their order counts and the specified conditions."""
tpch_queries_eng_gpt[14] = r"""Conduct a detailed analysis of promotional revenue as a percentage of total revenue for a specific timeframe. The analysis focuses on line items and parts where the shipping date falls between September 1, 1995, and September 30, 1995. The results aim to calculate the promotional revenue as a percentage of the total revenue generated during this period.

Data is sourced from the 'lineitem' and 'part' tables, establishing relationships between line items and their corresponding parts. The criteria for selection include matching part and line item keys, and ensuring that the shipping date is within the specified timeframe.

The calculation involves two components:

The sum of extended prices for line items with parts whose type starts with 'PROMO' (e.g., 'PROMO1', 'PROMO2'), adjusted for discounts.
The total sum of extended prices for all line items during the specified timeframe, also adjusted for discounts.
The percentage of promotional revenue is then computed as the ratio of the sum from step 1 to the sum from step 2, multiplied by 100.

The results provide valuable insights into the proportion of revenue generated from promotional parts compared to the overall revenue during the defined shipping timeframe."""
tpch_queries_eng_gpt[15] = r"""Perform an in-depth analysis of suppliers and their associated total revenue during a specific three-month period starting from January 1, 1996. The analysis focuses on line items, where the shipping date falls within this specified timeframe. The results aim to identify the supplier with the maximum total revenue during this period.

In the first part of the query, a Common Table Expression (CTE) named 'revenue0' is created. This CTE calculates the total revenue for each supplier ('SUPPLIER_NO') based on the sum of extended prices adjusted for discounts from relevant line items. The calculation is performed for line items with shipping dates between January 1, 1996, and March 31, 1996. The results are grouped by supplier.

In the second part of the query, the main selection is made from the 'supplier' table, along with the 'revenue0' CTE. The results include supplier information such as supplier key ('S_SUPPKEY'), name ('S_NAME'), address ('S_ADDRESS'), and phone number ('S_PHONE'), along with the total revenue calculated in the CTE. The selection is filtered to include only the supplier with the maximum total revenue during the specified timeframe.

The final presentation orders the results in ascending order based on the supplier key ('S_SUPPKEY'). This ordering provides a comprehensive view of supplier details for the supplier with the highest total revenue during the specified three-month period."""
tpch_queries_eng_gpt[16] = r"""Conduct an analysis of parts and their associated suppliers, considering various criteria to filter and group the results. The analysis focuses on parts and their corresponding suppliers, excluding specific conditions related to part brand, type, size, and supplier comments.

The selection is made from the 'partsupp' and 'part' tables, establishing relationships between parts and their suppliers. The criteria for inclusion involve matching part and partsupplier keys, and applying various filters:

Exclude parts with a brand of 'Brand#45.'
Exclude parts with a type starting with 'MEDIUM POLISHED.'
Include parts with specific sizes (49, 14, 23, 45, 19, 3, 36, 9).
Exclude suppliers with keys associated with comments containing the phrase 'Customer Complaints.'
The results are grouped by brand ('P_BRAND'), type ('P_TYPE'), and size ('P_SIZE'). For each unique combination of brand, type, and size, the count of distinct suppliers ('SUPPLIER_CNT') is calculated.

The final presentation orders the results in descending order based on the count of suppliers, and, in the case of ties, in ascending order based on brand, type, and size. This ordering provides a comprehensive view of the distribution of suppliers for different combinations of part brand, type, and size, meeting the specified conditions."""
tpch_queries_eng_gpt[17] = r"""Perform a detailed analysis of the average yearly extended price for a specific brand and container type of parts. The analysis focuses on line items and parts where the part brand is 'Brand#23' and the container type is 'MED BAG.' Additionally, the quantity of these parts in each line item should be less than 20% of the average quantity of the same part across all line items.

The selection is made from the 'lineitem' and 'part' tables, establishing relationships between parts and line items. The criteria for inclusion involve matching part and line item keys, and applying various filters:

Include only parts with a brand of 'Brand#23.'
Include only parts with a container type of 'MED BAG.'
Include only line items where the quantity is less than 20% of the average quantity of the same part across all line items.
The main calculation involves summing the extended prices of these line items and then dividing the result by 7.0 to obtain the average yearly extended price.

The subquery within the quantity comparison calculates 20% of the average quantity for the specific part.

The final result provides the average yearly extended price for line items meeting the specified conditions based on the 'Brand#23' brand and 'MED BAG' container type."""
tpch_queries_eng_gpt[18] = r"""Conduct a comprehensive analysis of customer orders and associated line items, focusing on specific criteria related to order quantities. The analysis aims to identify customers, orders, and line items where the total quantity of items in an order exceeds 300 units.

Data is sourced from the 'customer,' 'orders,' and 'lineitem' tables, establishing relationships between customers, orders, and line items. The criteria for inclusion involve the following:

Selection of orders where the order key is in the set of order keys obtained from a subquery. This subquery identifies order keys with a total quantity of items exceeding 300.
Matching customer key in orders and line items.
Matching order key in orders and line items.
The subquery calculates the total quantity of items per order key and selects only those order keys where the total quantity exceeds 300.

The main query then retrieves information such as customer name ('C_NAME'), customer key ('C_CUSTKEY'), order key ('O_ORDERKEY'), order date ('O_ORDERDATE'), total price of the order ('O_TOTALPRICE'), and the sum of quantities of items in the line items associated with each order.

The results are grouped by customer name, customer key, order key, order date, and total price. The presentation orders the results in descending order based on the total price of the order and then by order date. This ordering provides a detailed view of customer orders meeting the specified quantity criteria."""
tpch_queries_eng_gpt[19] = r"""Perform a comprehensive analysis of revenue generated from line items and associated parts, considering multiple sets of conditions for the selection. The analysis focuses on specific brands, containers, quantities, sizes, shipping modes, and shipping instructions for both 'Brand#12,' 'Brand#23,' and 'Brand#34.'

Data is sourced from the 'lineitem' and 'part' tables, establishing relationships between line items and parts. The selection criteria include various sets of conditions, each specified within an 'OR' clause:

For parts with 'Brand#12' and containers ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'), quantities between 1 and 11, sizes between 1 and 5, shipping modes ('AIR', 'AIR REG'), and shipping instruction 'DELIVER IN PERSON.'
For parts with 'Brand#23' and containers ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'), quantities between 10 and 20, sizes between 1 and 10, shipping modes ('AIR', 'AIR REG'), and shipping instruction 'DELIVER IN PERSON.'
For parts with 'Brand#34' and containers ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG'), quantities between 20 and 30, sizes between 1 and 15, shipping modes ('AIR', 'AIR REG'), and shipping instruction 'DELIVER IN PERSON.'
The revenue is calculated as the sum of extended prices adjusted for discounts for all line items meeting the specified conditions.

The results provide insights into the total revenue generated from line items associated with parts meeting the specified brand, container, quantity, size, shipping mode, and shipping instruction criteria."""
tpch_queries_eng_gpt[20] = r"""Conduct a thorough analysis to identify supplier names and addresses that meet specific criteria related to the availability of parts and their associated line items. The analysis involves multiple nested subqueries and focuses on suppliers located in Canada ('N_NAME' is 'CANADA').

Data is sourced from the 'supplier' and 'nation' tables, establishing relationships between suppliers and nations. The main selection criteria are defined within a set of subqueries:

Identify suppliers whose supplier keys ('S_SUPPKEY') are in the set of supplier keys obtained from a subquery. This subquery filters suppliers based on parts available in specific conditions.
The second subquery identifies part keys ('PS_PARTKEY') from the 'partsupp' table where the associated part names ('P_NAME') start with 'forest.'
The third subquery calculates a threshold quantity as 50% of the sum of line item quantities for a specific part-supplier combination, meeting additional conditions like a specific ship date range ('L_SHIPDATE' between '1994-01-01' and '1995-01-01').
The main query filters suppliers based on the specified nation ('N_NAME' is 'CANADA').
The results include supplier names ('S_NAME') and addresses ('S_ADDRESS') for suppliers meeting the specified conditions, ordered in ascending order based on supplier name."""
tpch_queries_eng_gpt[21] = r"""Conduct an in-depth analysis to identify suppliers and associated statistics based on various conditions related to order and line item details. The analysis aims to quantify the waiting time for suppliers in Saudi Arabia who have fulfilled orders but are still awaiting receipts for those orders.

Data is sourced from the 'supplier,' 'lineitem,' 'orders,' and 'nation' tables, establishing relationships between suppliers, line items, orders, and nations. The main selection criteria include various conditions:

Match supplier keys between 'supplier' and 'lineitem' (aliased as L1), and between 'orders' and 'lineitem.'
Include only orders with an order status of 'F' (fulfilled).
Consider only line items where the receipt date ('L_RECEIPTDATE') is later than the commit date ('L_COMMITDATE').
Utilize two EXISTS subqueries to filter suppliers based on the presence of other line items for the same order but from different suppliers, and the absence of line items from the same order but from different suppliers where the receipt date is later than the commit date.
Include only suppliers located in Saudi Arabia ('N_NAME' is 'SAUDI ARABIA').
The results include supplier names ('S_NAME') and the count of line items representing the waiting time ('NUMWAIT'). The presentation orders the results in descending order based on the waiting time and then in ascending order based on supplier name."""
tpch_queries_eng_gpt[22] = r"""Conduct a thorough analysis to obtain statistics on customers based on specific criteria related to their phone numbers, account balances, and order history. The analysis aims to summarize the number of customers, their total account balances, and the corresponding country codes.

Data is sourced from the 'customer' and 'orders' tables. The main selection criteria are defined within a nested subquery:

Extract the country codes ('CNTRYCODE') from the first two characters of customer phone numbers ('C_PHONE') using the SUBSTR function.
Include only customers with account balances ('C_ACCTBAL') greater than the average account balance for customers with positive balances in the specified country codes ('20', '40', '22', '30', '39', '42', '21').
Exclude customers who have placed orders using the NOT EXISTS condition.
The results, aliased as 'CUSTSALE,' are then aggregated based on country codes ('CNTRYCODE'). The summary includes the count of customers ('NUMCUST') and the sum of their account balances ('TOTACCTBAL'). The presentation orders the results in ascending order based on country codes."""
