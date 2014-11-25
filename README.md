
## Calculate entropies for DB Table columns on Spark

### Build

    sbt/sbt package

### Run

#### Compute mutual information for two columns 
    ./bin/spark-submit --class ColumnPairMI --master yarn-client --executor-memory 4G --num-executors 4 ../column_entropy/target/scala-2.10/calculate-column-entropy_2.10-1.0.jar <column1> <column2> <table>

#### Compute mutual information for all column pairs
 
    ./bin/spark-submit --class ColumnPairsMI --master yarn-client --executor-memory 4G --num-executors 4 ../column_entropy/target/scala-2.10/calculate-column-entropy_2.10-1.0.jar <table>

#### Compute mutual information for all column pairs and export to Hive table

    ./bin/spark-submit --class ColumnPairsMIExport --master yarn-client --executor-memory 4G --num-executors 4 ../column_entropy/target/scala-2.10/calculate-column-entropy_2.10-1.0.jar <table> <export table>
 
### Results

It reposts entropies for first and second columns and the mutual information between these two columns.

    column: first_column entropy = 11.558634302032608
    column: second_column entropy = 1.0844520129107182
    Mutual information: 1.0844520129107647

   
