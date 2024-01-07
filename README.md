# PayTmChallenge

## DISCLAIMER
The code has been tested and run successfully only on my personal laptop. As such, only running via IntelliJ/Code Editor is supported.
The code has not been tested on a standalone cluster. However, it should be able to run on a cluster with minimal changes to the run config.
## How to run the app
1. The main class is `com.paytm.PayTmWeatherChallengeRunner`

2. Run Config: 
```
--app-name PayTm --master-url local[1] --spark-properties spark.driver.cores=3,spark.submit.deployMode=client  --station-path /Full/Path/To/stationlist.csv --country-path /Full/Path/To/countrylist.csv --data-path /Full/Path/To/data/ --year 2019 --cleaned-countries-file-name clean_countries.csv --result-output-path /Full/Path/To/result.txt
```
3. If Running via IntelliJ, 
   1. set "Add dependencies with 'provided' scope to classpath"
   2. Make sure the same Java version is selected at all places. 
   3. This code has been tested on Java 8 and Scala 2.13.12
    

## Limitations
1. While filtering the data for a year, it is possible that data for a year flows in to files from the previous or the following year. For example 2019 data could be present in 2018 Dec 31 file and 2020 Jan 1 file. My current solution does not takes this case into consideration.
2 pom.xml has most dependencies, yet certain dependencies might be missing to run on a cluster.

## Known Bugs
1. CountryStatsPreProcessing.cleanCountriesFile() will break if the countries file has more than 2 columns.
2. CountryStats.getCountryAverageMetricByRank() can return invalid result if the desired rank is not in dataset.
3. CountryStats.getCountryWithConsecutiveDaysOfIndicator() should validate for metricIndex value.

## Areas of Improvement
1. All the input parameters as mandatory, regardless of the metric being calculated.
2. Input Configuration can be modified to decide which metric to calculate, for example metric_name=FRSHTT can be served via config.
3. Logger has not been configured which would be required for a production use case.
4. Certain methods(for example: the ones that read DataFrame from a file) can be refactored to be unit-tested.
5. Present code writes the result to a file just once after all the given metrics are calculated. Instead, it can write the output multiple times as each metric becomes available.