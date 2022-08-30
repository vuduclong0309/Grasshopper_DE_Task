# Grasshopper Data Engineering Case Study
## DataMart Logical Design
- Data Warehouse Choice: Kimball model:
  - Business process oriented
  - Fast designing
  - Data is not complexed yet (though can be prone to extension in future)
    - Nonetheless, type of order tend to be slow changing.

Note*
- Top-down Inmon might be preferred if there is more time:
  - Effort for implementing requirement changes is lower
  - Benefits in term of future maintainance cost

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
- Dimension table: both date time & orders should be degenerate dimension, as order is already a fact it self. Thus no dim order is needed:
  - ~~dim_date~~
  - ~~dim_order~~
- Date time would be the natural partition key of table, we doesnt need to include by this column
- For orders, we will have 1 fact (dwd) table to capture each type of atomic order
  - dwd__orders_add
  - dwd__orders_delete
  - dwd__orders_trade
