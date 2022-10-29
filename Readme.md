# Licensing

Please refer to LICENSE.txt and DATA_DISCLAIMER.txt. This software is supplied as-is, use at your own risk and
information from using this software does NOT constitute financial advice.

# Maven Dependency

Library
```
<dependency>
    <groupId>com.limemojito.oss.trading.trading-data-stream</groupId>
    <artifactId>model</artifactId>
    <version>1.1.0</version>
</dependency>
```
Check out the source to see a working example in example-cli (Spring Boot command line).

---
# Changes

## 1.1.0
* Stop searches at the Beginning of Time - arbitrarily defaulted to be 2010-01-01T00:00:00Z.
* Allow configuration of the Beginning of Time.
* Aggregate for a number of bars before a given instant.
* Aggregate for a number of bars after a given instant.
* Added delayed n-retry with exponential backoff on Dukascopy IO failure.  Defaults to 3X and 2 second.
* Upped the rate limit after discovering 500s are occasional errors on empty paths.
* Added a cache primer application to test load limits (concurrent requests).

## 1.0.0
* Initial release to open source.  
* Search ticks by symbol, time range, 
* Aggregate bars by time range.
* Convert models (Tick, Bar) to csv.
* Cache dukascopy files Direct No Cache, Local Cache and S3 Cache.
* Combine caches into cache chains such as Local -> S3 -> Direct.
* Example CLI

---
# Quickstart

```
mvn clean install
```

this produces the model jar, example-cli and a cache primer application.

## NZDUSD M5 bars for 2018-01-02T00:00:00Z -> 2018-01-02T00:59:59Z as CSV.

*note* that files are cached locally in ~/.dukascopy-cache. See LocalDukascopyCache.java for details.

```
java -jar example-cli/target/example-cli-1.1.0.jar --symbol=NZDUSD --period=M5 \
  --start=2018-01-02T00:00:00Z --end=2018-01-02T00:59:59Z --output=test-nz.csv  
```

## AUDUSD M5 bars for 2018-01-02T00:00:00Z -> 2018-01-02T00:59:59Z as CSV with S3 cache.

*note* this application cache chain is local <- s3 <- direct - ie the S3 cache is only used if it is not cached locally (~/.dukascopy-cache).
See S3DukascopyCache.java and the chain configuration in DataStreamCli.java for details.
```
aws s3 mb s3://test-tick-bucket
java -jar example-cli/target/example-cli-1.1.0.jar --spring.profiles.active=s3 \
  --bucket-name=test-tick-bucket --symbol=AUDUSD --period=M5 --start=2018-01-02T00:00:00Z \
  --end=2018-01-02T00:59:59Z --output=test-au.csv  
```
   
## Prime a local cache with AUDUSD and EURUSD 2 months
```
java -jar cache-primer/target/cache-primer-1.1.0.jar --symbol=AUDUSD --symbol EURUSD \
  --start=2018-01-01T00:00:00Z --end=2018-03-01T00:59:59Z  
```

## Prime a s3 cache with AUDUSD and EURUSD 2 months
*note* this application cache chain is s3 <- local <- direct.
See S3DukascopyCache.java and the chain configuration in CachePrimer.java for details.
```
aws s3 mb s3://test-tick-bucket
java -jar cache-primer/target/cache-primer-1.1.0.jar --spring.profiles.active=s3 \
  --bucket-name=test-tick-bucket --symbol=AUDUSD --symbol EURUSD \
  --start=2018-01-01T00:00:00Z --end=2018-03-01T00:59:59Z  
```


---
# Implementation notes

Times are supplied in UTC as this matches the Dukascopy epoch data.

Dukascopy file format was reverse engineered using internet forums, the wayback machine, and bit twiddling.

## Spring support

Model classes have annotations to support @Value configuration. See DataStreamCLi for an example
of configuring with a SpringBoot application.

## What is StreamData for?

Our models derive from "StreamData" which is an abstraction for having physical streams of these data records
that may exist together on a common transport. You may not need this approach for what you are building.

## Tick to bar aggregation

Price represent Bid tone.

See BarTickStreamAggregator.java for details.

## Dukascopy Tick Data Exploration

Inspiration from C++ library:

https://github.com/ninety47/dukascopy

### Tick Dukascopy File Format

Note that dukascopy is a UTC+0 offset so no time adjustment is necessary.
Dukascopy historical data.

The files I downloaded are named something like '00h_ticks.bi5'. These 'bi5' files are LZMA compressed binary data
files. The binary data file are formatted into 20-byte rows.

32-bit integer: milliseconds since epoch

32-bit float: Ask price

32-bit float: Bid price

32-bit float: Ask volume

32-bit float: Bid volume

The ask and bid prices need to be multiplied by the point value for the symbol/currency pair.
The epoch is extracted from the URL (and the folder structure I've used to store the files on disk). It represents the
point in time that the file starts from e.g. 2013/01/14/00h_ticks.bi5 has the epoch of midnight on 14 January 2013.
Example using C++ to work file format, including format and computation of “epoch time”:

LZ compression/decompression can be done with apache commons compress:

https://commons.apache.org/proper/commons-compress/

This format is “valid” after experimentation.

```
[   TIME  ] [   ASKP  ] [   BIDP  ] [   ASKV  ] [   BIDV  ]
[0000 0800] [0002 2f51] [0002 2f47] [4096 6666] [4013 3333]
```

* TIME is a 32-bit big-endian integer representing the number of milliseconds that have passed since the beginning of
  this hour.
* ASKP is a 32-bit big-endian integer representing the asking price of the pair, multiplied by 100,000.
* BIDP is a 32-bit big-endian integer representing the bidding price of the pair, multiplied by 100,000.
* ASKV is a 32-bit big-endian floating point number representing the asking volume, divided by 1,000,000.
* BIDV is a 32-bit big-endian floating point number representing the bidding volume, divided by 1,000,000.

### Tick Data Format

Note that epoch milliseconds is relative to UTC timezone.
source is live | historical

```
{
   "epochMilliseconds": 94875945798,
   "symbol": "EURUSD",
   "bid" :134567,
   "ask" : 134520,
   "source": "live"
}
```
