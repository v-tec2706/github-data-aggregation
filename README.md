# GH Aggregations

This simple program calculates some aggregation based on [Github Archive](https://www.githubarchive.org/) data. 
## How to run? 
#### Download the data
To download the data run following script from the root directory of the project:
```bash
./download_data.sh
```
You can specify argument with date range you want to download. Check the required format on the page linked above.
Example: 
```bash
./download_data.sh 2015-01-01-{0..23}
```
By default `2018-01-{01..31}-{0..23}` will be used. 
#### Run calculations
```bash
sbt run
```
The output will appear in `./data/...` directory. 

#### Run tests
```bash
sbt test
```

## Notes



