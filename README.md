
## Calculate entropies for DB Table columns on Spark

### Build

    sbt/sbt package

### Run

   ./bin/spark-submit --class ColumnEntropy --master yarn-client --executor-memory 4G --num-executors 4 ../column_entropy/target/scala-2.10/calculate-column-entropy_2.10-1.0.jar first_column second_column column_for_count table_name

### Results

It reposts entropies for first and second columns and the mutual information between these two columns.

    column: first_column entropy = 11.558634302032608
    column: second_column entropy = 1.0844520129107182
    Mutual information: 1.0844520129107647

   
