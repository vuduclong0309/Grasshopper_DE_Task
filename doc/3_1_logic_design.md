# Grasshopper Data Engineering Case Study
### Logical Design
- Business Process: order.
- Granularity: 1 entries per atomic order:
  - Note: for complex order like modify order, we split into 1 DELETE and 1 ADD order (as per sample data)
  - For future complex order, we would try to normalize the order into atomic order

- Involved dimension: Order, Date.
- Update frequency: minute for batches.

Required metrics:
- Non-additive: Lowest sell and highest sell order

|                | Datetime   | Orders |
| :---           |:---:   |  :---: |
| ADD (SELL/BUY) |  x     |    x   |
| DELETE         |  x     |    x   |        
| TRADE          |  x     |    x   |

### Table Selection
- Source table:
  - From data csv, we should ingest our data into table separated by minutes ods__raw_order
  - This table should have the same schema, but partition by order time (rounded by minutes)

- Dimension table: both date time & orders should be degenerate dimension, as order is already a fact it self. Thus no dim order is needed:
  - ~~dim_date~~
  - ~~dim_order~~
- Fact table:
  - Date time would be the natural partition key of table, we doesnt need to include by this column
  - For orders, we will have 1 fact (dwd) table to capture each type of atomic order
    - dwd__orders_add
    - dwd__orders_delete
    - dwd__orders_trade
- Aggregated fact tables:
  - We need min ask & max bid order for each, this can be simplified into 2 step
    - Find sum of all bid / ask order (aggregatable)
    - Find min / max order from selecting max bid price / min ask price
  - We construct two aggregated tables:
    - dws__orders_sell
    - dws__orders_buy
    - Those batches aggregated table would be used as periodic snapshot to prevent reaggregated for new order, and can be used in conjuction with real time table for latest minute.
- Application service tables:
  - We need generate the entry table for outputting the required outcome:
    - ads__order_book
  - Once all the fact table are generated, we can easily generate this by:
    - Retrieving T-1 minutes sell side & buy side info from dws order tables
    - From T-1 minutes data, we add entry for minutes T by adding latest order one by one (as demonstrated in naive implementation)
