# Grasshopper Data Engineering Case Study
## Further consideration:

In this section we will discuss on the consideration question of problem requirements

### How would you deal with data which arrives out of order
- As I designed a minute batches pipeline in previous section, this data mart automatically allow for data arrive late up to 1 minute (as long as it still in minute T)
- For data arrive out of sequence for longer time, we need to backfill more and more period, but this is not advised.


### How it might be possible for a unified batch and streaming implementation to work:
- We use Lambda architecture, in other word:
  - The result of all buy/sell order & their quantity can be aggregated
  - For aggregated data up to minute T-1, we use data from batch pipeline as designed in this repo (Batch layer)
  - For aggregated data of minute T, we use real time application (Speed layer) e.g Spark Stream
  - Service Layer generate order book by aggregated data from batch & speed layer.
