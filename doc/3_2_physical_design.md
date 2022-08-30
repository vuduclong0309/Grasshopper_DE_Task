# Grasshopper Data Engineering Case Study
## DataMart Physical Design Summary:

*Due to time constrain, I can't fully code out the data mart, but I will provide the idea / key pseudo code to reconstruct this

### Tech stack selection
- Apache Spark + Apache Airflow

### Table selection

ODS Table:
ods__raw_order

- Pseudo ddl sql:

```
CREATE TABLE ods__raw_order (
  seq_num STRING NOT NULL,
  ... --Other column from csv file
  PARTITION KEY (order_date_minute),
  PRIMARY KEY (seq_num)
);
```

```
INSERT OVERWRITE TABLE ods__raw_order PARTITION (order_date_minute)
SELECT
    *,
    get_minute(time) AS order_date_minute
FROM
    delta_l1_csv
```

DIM Table: None

DWD Table
- dwd__orders_add
- dwd__orders_delete
- dwd__orders_trade

Example ddl:

```
CREATE TABLE dwd__orders_add (
  subset of column in ods table, but only column related to add order
);
```

Example dml:

```
INSERT OVERWRITE TABLE dwd__orders_add PARTITION (order_date_minute)
SELECT
    (column related to add order)
    get_minute(time) AS order_date_minute
FROM
    ods__raw_order
WHERE
    ods__raw_order.add_side IN ('BUY', 'SELL')
```

DWS TABLE:
- dws__orders_sell
- dws__orders_buy

Example ddl:
```
CREATE TABLE dws__orders_sell (
    order_date_minute   DATETIME  NOT NULL,
    sell_price          DOUBLE    NOT NULL,
    sell_qty            INT       NOT NULL
);
```

```
INSERT OVERWRITE TABLE dws__orders_sell PARTITION (order_date_minute)
SELECT
    sell_price,
    SUM(sell_qty) AS sell_qty
    order_date_minute
FROM
    -- T - 1 minute data
    (
        SELECT *
        FROM  dwd__orders_add
        WHERE buy_side == 'SELL' and order_date_minute = T-1  
    )
    UNION
    (
        SELECT *
        FROM  dwd__orders_delete
        WHERE buy_side == 'SELL' and order_date_minute = T-1
    )
    -- T data, aggregate from DWD table
    -- Pardon me I don't know the logic of the TRADE order yet
    SELECT SUM (etc)
    FROM some join of dwd table
    WHERE order_date_minute = T
WHERE
    add_side = 'SELL'
```

ADS table:
- ads__l1_order_book

```
CREATE TABLE ads__l1_order_book (
  column in or expected_l1_data_v3.1
);
```

Due to complexity of the dml, I can't generate logic for this table, but it will have the step:
- Retrieving T-1 minutes sell side & buy side info from dws order tables
- From T-1 minutes data, we add entry for minutes T by adding latest order one by one (as demonstrated in naive implementation)

For visualized table design, please refer to the image 3_2_physical_design.png
