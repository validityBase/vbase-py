# Dataset Commitments

Below is a survey of the principles for making effective commitments 
that deliver maximum value for data consumers.

## General Principles

- Commit production data to create an objective verifiable record of the data.
Commitments resemble running a stock market index
where a credible party permanently records all rebalancing trades.
- Commit data as soon as it is available to consumers to achieve
high fidelity of record timestamps.
- Commit revisions and corrections to data similarly.
The sequence of commitments thus created offers an accurate view of the data that consumers
can replay as if they were receiving live historical records.
- Prompt commitment of revisions reduces or eliminates
lengthy and expensive live testing required for legacy financial data.
With legacy workflows, consumers can't be sure of the precise timing
of data delivery and revisions unless these were delivered into their production processes,
requiring expensive testing.

## External Examples

vBase commitments and the ability to `rewind the information to "as it actually was" ` 
enable bitemporal modeling for any data: https://en.wikipedia.org/wiki/Bitemporal_modeling.
Indeed, the underlying objective cryptographic commitments enable verification,
bitemporal modeling, and historical simulation for any untrusted 3rd party data
as if the consumer received historical data in production or generated it themselves.

Some non-vBase datasets with support for (low-fidelity) vintage timestamps are
important macroeconomic series with frequent revisions:
- ADP: Total Nonfarm Private Payroll Employment: https://alfred.stlouisfed.org/series/downloaddata?seid=ADPWNUSNERSA.
- U.S. Bureau of Labor Statistics: All Employees, Total Nonfarm: https://alfred.stlouisfed.org/series?seid=PAYEMS

## vBase Commitment Examples

We will use the above Total Nonfarm (PAYEMS) series 
to illustrate how vBase commitments establish high-fidelity "vintage timestamps"
identifying when data and its revisions became available.

For simplicity, we will assume all commitments are made at midnight GMT, 
beginning with the `20221104` vintage. 

### Initial commitment

The initial dataset is the full history for the `2022-11-04` vintage: 
```
t           value
1939-01-01	29923
1939-02-01	30100
1939-03-01	30280
...
2022-08-01	152732
2022-09-01	153047
2022-10-01	153308
```

These initial and subsequent records can be committed as 
`VBaseJsonSeries` or `VBaseStringSeries` objects.
If data is available in an AWS S3 bucket,
it can also be committed using the `commit_s3_objects` tool:
https://docs.vbase.com/reference/vbase-py-tools/commit_s3_objects.

Data receives the following timestamp:
```
Timestamp('2022-11-04 00:00:00')
```
The initial dataset history has been established.

### Incremental commitment

At the next reporting period, an incremental commitment records observation for the new month:
```
t           value
2022-11-01  153548
```

Data receives the following timestamp upon commitment:
```
Timestamp('2022-12-02 00:00:00')
```

### Revisions

At the `2022-12-02` reporting period, historical data was also revised.
This revision can be submitted as a dataset of changed values:
```
t           value
2022-09-01	153001
2022-10-01	153285
```

Alternatively, the entire history can be re-submitted:
```
t           value
1939-01-01	29923
1939-02-01	30100
1939-03-01	30280
...
2022-08-01	152732
2022-09-01	153001
2022-10-01	153285
```

Both approaches will record revisions with a high-fidelity timestamp:
```
Timestamp('2022-12-02 00:00:00')
```

Regardless of which method the producer finds most convenient,
vBase consumer and simulation APIs will make the latest known data available for a given
query or simulation time.
