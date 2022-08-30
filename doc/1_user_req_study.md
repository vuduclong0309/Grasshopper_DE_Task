# Grasshopper Data Engineering Case Study

## Key facts:
### Source data:
  - 1 delta table (assumption per problem statement)
      - Somewhat narrow: 16 columns
      - Under multiple csv batches, 1 example csv given as example
      - Entries represent order, multiple order types.
  - Throughput: ~6000 entries / min, peak not determined (from source data evaluation)
  - Business / Trading oriented, can arrive out of order (based on requirement statement)
  - Should be real-time / as real time as possible (based on problem statement)
  - Number of maintainer is 1 (assumption)
  - Time given: 2 - 4 days, no prior setup.

## Feasibility study:
- Due to time constrain, fully implement a mart is challenging.
- Some requirement is unclear (i.e trade order), need further clarification
  - From sample csv file, only 4 trades fall in this category atm
    - Will temporary ignore logic
- Feasible solution suggestion:
  - Implement a naive solution, which take sample input and output expected csv for logic demonstration (1-2 days)
  - Design discussion for a data mart to satisfy the business requirement in real time (1 days)
