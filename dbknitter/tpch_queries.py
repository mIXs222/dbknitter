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
tpch_queries_eng_official[2] = r"""The Minimum Cost Supplier Query finds, in EUROPE region, for each part of a BRASS type and size of 15, the supplier who can supply it at minimum supply cost. If several suppliers in that EUROPE offers the BRASS part and size of 15 at the same (minimum) cost, the query lists the suppliers with the order by account balance in descending order, and nation name, supplier name, part key in ascending order. For each supplier, the query lists the supplier's account balance, name and nation; the part's number and manufacturer; the supplier's address, phone number and comment information. Please output the columns in the order of N_NAME, P_MFGR, P_PARTKEY, S_ACCTBAL, S_ADDRESS, S_COMMENT, S_NAME, S_PHONE."""
tpch_queries_eng_official[3] = r"""The Shipping Priority Query retrieves the shipping priority and potential revenue, defined as the sum of l_extendedprice * (1-l_discount), of the orders having the largest revenue among those that had not been shipped as of a given date of 1995-03-15 and where market segment is BUILDING. Orders are listed in decreasing order of revenue. Please output the columns in the order of O_ORDERKEY, REVENUE, O_ORDERDATE, and O_SHIPPRIORITY."""
tpch_queries_eng_official[4] = r"""The Order Priority Checking Query counts the number of orders ordered between 1993-07-01 and 1993-10-01 in which at least one lineitem was received by the customer later than its committed date. The query lists the count of such orders for each order priority sorted in ascending priority order. Please output the columns in the order of ORDER_COUNT and O_ORDERPRIORITY"""
tpch_queries_eng_official[5] = r"""The Local Supplier Volume Query lists for each nation in ASIA region the revenue volume that resulted from lineitem transactions in which the customer ordering parts and the supplier filling them were both within ASIA. The query is run in order to determine whether to institute local distribution centers in ASIA. The query considers only parts ordered between 1990-01-01 and 1995-01-01. The query displays the nations and revenue volume in descending order by revenue. Revenue volume for all qualifying lineitems in a particular nation is defined as sum(l_extendedprice * (1 - l_discount)). Please output the columns in the order of N_NAME and REVENUE."""
tpch_queries_eng_official[6] = r"""The Forecasting Revenue Change Query considers all the lineitems shipped between 1994-01-01 and 1995-01-01 with discounts between .06 - 0.01 and .06 + 0.01. The query lists the amount by which the total revenue would have increased if these discounts had been eliminated for lineitems with l_quantity less than 24. Note that the potential revenue increase is equal to the sum of [l_extendedprice * l_discount] for all lineitems with discounts and quantities in the qualifying range. Please output the columns in the order of REVENUE."""
tpch_queries_eng_official[7] = r"""The Volume Shipping Query finds, for two given nations which are INDIA and JAPAN, the gross discounted revenues derived from lineitems in which parts were shipped from a supplier in either nation to a customer in the other nation during 1995 and 1996. The query lists the supplier nation, the customer nation, the year, and the revenue from shipments that took place in that year. The query orders the answer by Supplier nation, Customer nation, and year (all ascending). Please output the columns in the order of CUST_NATION, L_YEAR, REVENUE, SUPP_NATION"""
tpch_queries_eng_official[8] = r"""This query determines how the market share of INDA within ASIA has changed over two years for SMALL PLATED COPPER., The market share for INDA within ASIA is defined as the fraction of the revenue, the sum of [l_extendedprice * (1-l_discount)], from the products of SMALL PLATED COPPER in ASIA that was supplied by suppliers from INDIA. The query determines this for the years 1995 and 1996 presented in this order. """
tpch_queries_eng_official[9] = r"""This query determines how much profit is made on a given line of parts, broken out by supplier nation and year., The Product Type Profit Measure Query finds, for each nation and each year, the profit for all parts ordered in that year that contain a specified dim in their names and that were filled by a supplier in that nation. The profit is defined as the sum of [(l_extendedprice*(1-l_discount)) - (ps_supplycost * l_quantity)] for all lineitems describing parts in the specified line. The query lists the nations in ascending alphabetical order and, for each nation, the year and profit in descending order by year (most recent first)."""
tpch_queries_eng_official[10] = r"""The query identifies customers who might be having problems with the parts that are shipped to them., The Returned Item Reporting Query finds the customers, in terms of their effect on lost revenue betwen  1993-10-01 and 1994-01-01, who have returned parts. The query considers only parts that were ordered in the specified quarter. The query lists the customer's name, address, nation, phone number, account balance, comment information and revenue lost. The customers are listed in descending order of lost revenue, customer key, customer name and customer account balance. Revenue lost is defined as sum(l_extendedprice*(1-l_discount)) for all qualifying lineitems., """
tpch_queries_eng_official[11] = r"""This query finds the most important subset of suppliers' stock in GERMANY., The Important Stock Identification Query finds, from scanning the available stock of suppliers in GERMANY, all the parts that represent a significant percentage (larger than 0.0001) of the total value of all available parts. The query displays the part number and the value of those parts in descending order of value."""
tpch_queries_eng_official[12] = r"""This query determines whether selecting less expensive modes of shipping is negatively affecting the critical-priority orders by causing more parts to be received by customers after the committed date. The Shipping Modes and Order Priority Query counts, by ship mode which is MAIL and SHIP, for lineitems actually received by customers between 1994-01-01 and 1995-01-01 which is the receipt date, the number of lineitems belonging to orders for which the l_receiptdate exceeds the l_commitdate for two different specified ship modes. Only lineitems that were actually shipped before the l_commitdate are considered. The late lineitems are partitioned into two groups, those with priority URGENT or HIGH, and those with a priority other than URGENT or HIGH."""
tpch_queries_eng_official[13] = r"""This query seeks relationships between customers and the size of their orders. This query determines the distribution of customers by the number of orders they have made, including customers who have no record of orders, past or present. It counts and reports how many customers have no orders, how many have 1, 2, 3, etc. A check is made to ensure that the orders counted do not fall into pending or do not fall into deposits. Special categories are identified in the order comment column whose pattern is neither "pending" nor "depoists"."""
tpch_queries_eng_official[14] = r"""This query monitors the market response to a promotion such as TV advertisements or a special campaign. The Promotion Effect Query determines what percentage of the revenue between 1995-09-01 and 1995-10-01 was derived from promotional parts. The query considers only parts actually shipped and gives the percentage. Revenue is defined as (l_extendedprice * (1-l_discount))."""
tpch_queries_eng_official[15] = r"""This query determines the top supplier so it can be rewarded, given more business, or identified for special recognition. The Top Supplier Query finds the supplier who contributed the most to the overall revenue for parts shipped between 1996-01-01 and 1996-04-01. In case of a tie, the query lists all suppliers whose contribution was equal to the maximum, presented in supplier number order."""
tpch_queries_eng_official[16] = r"""This query finds out how many suppliers can supply parts with given attributes. It might be used, for example, to determine whether there is a sufficient number of suppliers for heavily ordered parts., The Parts/Supplier Relationship Query counts the number of suppliers who can supply parts that satisfy a particular customer's requirements which does not have a brand id of 45. The customer is interested in parts of eight different sizes which are one of 49, 14, 23, 45, 19, 3, 36, or 9, as long as they are not of MEDIUM POLISHED, not of a brand 45, and not from a supplier who has had complaints registered at the Better Business Bureau. Results must be presented in descending count and ascending brand, type, and size."""
tpch_queries_eng_official[17] = r""" This query determines how much average yearly revenue would be lost if orders were no longer filled for small quantities of certain parts. This may reduce overhead expenses by concentrating sales on larger shipments., The Small-Quantity-Order Revenue Query considers parts of a brand 23 and with MED BAG and determines the average lineitem quantity of such parts ordered for all orders (past and pending) in the 7-year database. What would be the average yearly gross (undiscounted) loss in revenue if orders for these parts with a quantity of less than 20% of this average were no longer taken?"""
tpch_queries_eng_official[18] = r"""The Large Volume Customer Query ranks customers based on their having placed a large quantity order. Large quantity orders are defined as those orders whose total quantity is above a certain level., The Large Volume Customer Query finds a list of the customers who have ever placed quantity orders larger than 300. The query lists the customer name, customer key, the order key, date and total price and the quantity for the order."""
tpch_queries_eng_official[19] = r"""The Discounted Revenue query finds the gross discounted revenue for all orders for three different types of parts. Parts are selected based on the combination of specific brands, a list of containers, and a range of sizes. The first type has a brand id of 12, a container of 'SM CASE', 'SM BOX' 'SM PACK', or 'SM PKG', quantity which is greater than or eqaul to 1 and less than or equal to 11, size between 1 and 5. The second type has a brand id of 23, a container of 'MED BAG', 'MED BOX', 'MED PKG', or 'MED PACK', quantity which is greater than or eqaul to 10 and less than or equal to 20, size between 1 and 10. The third type has a brand id of 34, a container of 'LG CASE', 'LG BOX', 'LG PACK', or 'LG PKG', quantity which is greater than or eqaul to 20 and less than or equal to 30, size between 1 and 15. All should be shipped by air whose SHIPMODE is either 'AIR' or 'AIR REG' and delivered in person."""
tpch_queries_eng_official[20] = r"""The Potential Part Promotion query identifies suppliers who have an excess of a forest part; an excess is defined to be more than 50% of the parts like the forest part that the supplier shipped between 1994-01-01 and 1995-01-01 for CANADA. Only parts whose names share a certain naming convention are considered."""
tpch_queries_eng_official[21] = r"""The Suppliers Who Kept Orders Waiting query identifies suppliers, for a nation which is SAUDI ARABIA, whose product was part of a multi-supplier order (with current [status] of 'F') where they were the only supplier who failed to meet the committed delivery date. Please output NUMWAIT and S_NAME columne in the order of NUMWAIT descending."""
tpch_queries_eng_official[22] = r"""The Global Sales Opportunity Query identifies geographies where there are customers who may be likely to make a purchase. This query counts how many customers within a specific range of country codes have not placed orders for 7 years whose average account balance is greater than 0.00. It also reflects the magnitude of that balance. Country code is defined as the first two characters of c_phone which should be one of '20', '40', '22', '30', '39', '42', or '21'. Please output the columns in the order of CNTRYCODE, NUMCUST, and TOTACCTBAL. It should be by in the order of CNTRYCODE ascending"""


""" Manual English Translation from TPC-H Query and Specification """

tpch_queries_eng_manual = {}
# TODO(Gangmmuk, Hanxi): Fill this in like tpch_queries_eng_official


""" GPT's English Translation from TPC-H Query and Specification """

tpch_queries_eng_gpt = {}
# TODO(Gangmmuk, Hanxi): Fill this in like tpch_queries_eng_official
