# Bicycle-Manufacturer-Analytics
## I. Business Context

The business is facing key questions:

- Which product categories are driving sales?

- How is performance changing over time?

- Which regions perform best?

- Are discount campaigns effective?

- How well are we managing inventory?

- Are we retaining customers?

=> This project answers those questions using **AdventureWorks2019 dataset on BigQuery**.

## II. Objective

Transform raw transactional data into actionable insights to:

- Improve product performance
- Optimize inventory management
- Evaluate discount strategies
- Understand customer retention
- Support data-driven decision making

## III. Tools Used
- SQL (Google BigQuery)
- AdventureWorks2019 Dataset

## IV. Analysis Journey
### Query 1: Calc Quantity of items, Sales value & Order quantity by each Subcategory in L12M
``` sql
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
```

#### Result:
<img width="563" height="675" alt="Image" src="https://github.com/user-attachments/assets/fa02a8a6-b140-4c6c-b06c-e6d3736329e8" />

### Query 2: Calc % YoY growth rate by SubCategory & release top 3 cat with highest grow rate. (qty_diff = qty_item / prv_qty - 1)
``` sql 
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
```

#### Result:
<img width="635" height="108" alt="Image" src="https://github.com/user-attachments/assets/4a8bc36b-2ad4-41bd-a6b7-a848bf4c9033" />

### Query 3: Ranking Top 3 TeritoryID with biggest Order quantity of every year. (If there's TerritoryID with same quantity in a year, do not skip the rank number)
``` sql
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
```

#### Result:
<img width="484" height="351" alt="Image" src="https://github.com/user-attachments/assets/c14483b5-4b1f-47fa-8b88-1053cfbe121e" />

### Query 4: Calc Total Discount Cost belongs to Seasonal Discount for each SubCategory.(Discount Cost = Disct Pct * Unit Price * Item Qty)
``` sql
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
```

#### Result:
<img width="480" height="80" alt="Image" src="https://github.com/user-attachments/assets/b1b8ce9c-b997-461e-9a9b-b80545175bc3" />

### Query 5: Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
``` sql
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
```

#### Result:
<img width="511" height="675" alt="Image" src="https://github.com/user-attachments/assets/22a41053-b722-411e-9389-987bf8d54668" />

### Query 6: Trend of Stock level & MoM diff % by all product in 2011. If %gr rate is null then 0. Round to 1 decimal
``` sql
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
```

#### Result:
<img width="644" height="675" alt="Image" src="https://github.com/user-attachments/assets/c050eec0-c95c-4c47-8bf0-87e8dc6b04d4" />

### Query 7: Calc Ratio of Stock / Sales in 2011 by product name, by month. (Order by month desc, ratio desc. Round Ratio to 1 decimal)
``` sql
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
```

#### Result:
<img width="794" height="674" alt="Image" src="https://github.com/user-attachments/assets/b8ea259d-b260-4e73-a48c-1fcc93a5a6ce" />

### Query 8: No of order and value at Pending status in 2014
``` sql
SELECT extract(year FROM ModifiedDate) yr
  , Status 
  , count(distinct PurchaseOrderID) order_Cnt
  , sum(TotalDue) value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader`
where extract(year FROM ModifiedDate) = 2014
  and Status = 1
group by 1, 2;
```

#### Result: 
<img width="459" height="53" alt="Image" src="https://github.com/user-attachments/assets/2beaf3b1-479b-4641-ab9e-421129db5bfe" />



