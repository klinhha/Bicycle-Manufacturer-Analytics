-- Query 1: Calc Quantity of items, Sales value & Order quantity by each Subcategory in L12M
SELECT FORMAT_DATE("%b %Y", a.ModifiedDate) period
  , c.Name
  , sum(OrderQty) qty_item
  , sum(LineTotal) total_sales
  , count(distinct SalesOrderID) order_cnt
FROM `adventureworks2019.Sales.SalesOrderDetail` a
left join `adventureworks2019.Production.Product` b
  using(ProductID)
left join `adventureworks2019.Production.ProductSubcategory` c
  on cast(b.ProductSubcategoryID as INT) = c.ProductSubcategoryID
where date(a.ModifiedDate) >= (SELECT DATE_SUB(MAX(DATE(ModifiedDate)), INTERVAL 12 MONTH)
                               FROM `adventureworks2019.Sales.SalesOrderDetail`)
group by period, c.Name
order by c.Name;


-- Query 2: Calc % YoY growth rate by SubCategory & release top 3 cat with highest grow rate. (qty_diff = qty_item / prv_qty - 1)
with raw_data as (
  SELECT FORMAT_DATE("%Y", a.ModifiedDate) year
    , c.Name
    , sum(a.OrderQty) qty_item
  FROM `adventureworks2019.Sales.SalesOrderDetail` a
  left join `adventureworks2019.Production.Product` b
    using(ProductID)
  left join `adventureworks2019.Production.ProductSubcategory` c
    on cast(b.ProductSubcategoryID as INT) = c.ProductSubcategoryID
  group by year, c.Name
  order by year
)
, previous as (
  SELECT *
    , lead(qty_item) over(partition by name order by year desc) prv_qty
  FROM raw_data
)
, diff as (
  SELECT *
    , round(qty_item / prv_qty - 1, 2) qty_diff
  FROM previous
  where prv_qty is not NULL
  order by qty_diff desc
)
, ranking as (
  select * 
    , dense_rank() over(order by qty_diff desc) rk
  from diff
)

select name
  , qty_item
  , prv_qty
  , qty_diff
from ranking
where rk <= 3;


-- Query 3: Ranking Top 3 TeritoryID with biggest Order quantity of every year. 
with raw_data as (
  SELECT FORMAT_DATE("%Y", a.ModifiedDate) yr
    , c.TerritoryID
    , sum(a.OrderQty) order_cnt
  FROM `adventureworks2019.Sales.SalesOrderDetail` a
  left join `adventureworks2019.Sales.SalesOrderHeader` b
    using(SalesOrderID)
  left join `adventureworks2019.Sales.Customer` c
    using(CustomerID)
  group by yr, c.TerritoryID
)
, ranking as (
  SELECT *
    , dense_rank() over(partition by yr order by order_cnt desc) rk
  FROM raw_data
  order by yr desc
)

SELECT *
FROM ranking
where rk <= 3;


-- Query 4: Calc Total Discount Cost belongs to Seasonal Discount for each SubCategory.(Discount Cost = Disct Pct * Unit Price * Item Qty)
SELECT FORMAT_DATE("%Y", a.ModifiedDate) year
  , c.Name
  , sum(d.DiscountPct * a.UnitPrice * a.OrderQty) total_cost
FROM `adventureworks2019.Sales.SalesOrderDetail` a
left join `adventureworks2019.Production.Product` b
  using(ProductID)
left join `adventureworks2019.Production.ProductSubcategory` c
  on cast(b.ProductSubcategoryID as INT) = c.ProductSubcategoryID
left join `adventureworks2019.Sales.SpecialOffer` d
   on a.SpecialOfferID = d.SpecialOfferID
where lower(Type) like '%seasonal discount%' 
group by year, c.Name;


-- Query 5: Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
with info as (
  SELECT extract(month FROM ModifiedDate) month_order
    , extract(year FROM ModifiedDate) year
    , CustomerID
    , count(distinct SalesOrderID) sales_total
  FROM `adventureworks2019.Sales.SalesOrderHeader` 
  where Status = 5 and extract(year FROM ModifiedDate) = 2014
  group by 1,2,3
)
, row_n as (
  select *
    , row_number() over(partition by CustomerID order by month_order) row_num
  from info
)
, first_order as (
  select distinct month_order as month_join, year, CustomerID
  from row_n
  where row_num = 1
)
, all_join as (
  select distinct a. CustomerID
    , a.month_order
    , a.year
    , b.month_join
    , concat('M', a.month_order - b.month_join) as month_diff
  from info a
  left join first_order b
    using(CustomerID)
  order by CustomerID
)

select distinct month_join, month_diff
  , count(distinct CustomerID) customer_cnt
from all_join
group by 1,2
order by 1,2;


-- Query 6: Trend of Stock level & MoM diff % by all product in 2011. 
with raw_data as (
  SELECT b.Name
    , extract(month FROM c.ModifiedDate) mth
    , extract(year FROM c.ModifiedDate) yr
    , sum(c.StockedQty) stock_qty
  FROM `adventureworks2019.Production.Product` b
  left join `adventureworks2019.Production.WorkOrder` c
    using (ProductID)
  where extract(year FROM c.ModifiedDate) = 2011
  group by 1,2,3
  having stock_qty is not NULL
  order by 1
)
, stock_previous as (
  select *
    , lead(stock_qty) over(partition by Name order by mth desc) stock_prv
  from raw_data
)

select *
  , round(case when stock_prv is null then 0
              else (stock_qty / stock_prv - 1) * 100 end, 1) diff
from stock_previous;


-- Query 7: Calc Ratio of Stock / Sales in 2011 by product name, by month.
with sale_info as (
  select extract(month FROM a.ModifiedDate) mth
      , extract(year FROM a.ModifiedDate) yr
      , a.ProductID
      , b.Name
      , sum(OrderQty) sales
  from `adventureworks2019.Sales.SalesOrderDetail` a 
  left join `adventureworks2019.Production.Product` b 
    using(ProductID)
  where extract(year FROM a.ModifiedDate) = 2011
  group by 1,2,3,4
  order by mth desc, sales
  )
, stock_info as (
  select extract(month FROM c.ModifiedDate) mth
    , extract(year FROM c.ModifiedDate) yr
    , b.Name
    , sum(StockedQty) stock
  from `adventureworks2019.Production.WorkOrder` c
  left join `adventureworks2019.Production.Product` b
    using (ProductID)
  where extract(year FROM c.ModifiedDate) = 2011
  group by 1,2,3
  order by mth desc, stock
)

select d.mth
  , d.yr
  , d.ProductID
  , e.Name
  , COALESCE(d.sales, 0) sales
  , COALESCE(e.stock, 0) stock
  , round(COALESCE(e.stock, 0) / COALESCE(d.sales, 0), 1) ratio
from sale_info d
join stock_info e
  on d.Name = e.Name
    and d.mth = e.mth
order by d.mth desc, ratio desc;


-- Query 8: No of order and value at Pending status in 2014
SELECT extract(year FROM ModifiedDate) yr
  , Status 
  , count(distinct PurchaseOrderID) order_Cnt
  , sum(TotalDue) value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader`
where extract(year FROM ModifiedDate) = 2014
  and Status = 1
group by 1, 2;