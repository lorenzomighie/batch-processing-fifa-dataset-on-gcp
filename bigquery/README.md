## BigQuery Exploratory Analysis

### Partitioning
Most of the queries will divide the insights of players and teams by two columns: version and gender.
Since the dataset is very unbalanced with regard to the gender, version is selected.

Even though the dataset is pretty small (about 100 MB) and partitioning should not influence the performance, empirical results show that queries done with partition on *version* column process 10 times less Bytes when running for some sample queries.

### Clustering

On the other hand Clustering the table on some aggregating factor like *club_name* and *gender* do not show any improvement.

