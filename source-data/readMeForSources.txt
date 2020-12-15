Wikipedia metadata files (article and redirect names) are created from wikipedia source in 1b-wikipedia-metaDataExtraction file. Size ~10GB

unigram_freq.csv in this folder is downloaded from some place on the interwebs. Source unknown.

Wikipedia graph database (neo4j) is downloaded from https://lts2.epfl.ch/Datasets/Wikipedia/
-->
Create new Neo4j database with version 3.X (3.5.19 used)
Password set to "password"
Open Neo4j terminal (from Neo4j Desktop)
cd to "installation-3.5.19\bin"
Run command:
neo4j-admin load --from=F:\wikipedia-data\data\EPFL\wikipedia_nrc.dump
where from= is "path to data"
see: https://neo4j.com/docs/operations-manual/current/tools/dump-load/?_ga=2.1663985.136166572.1598537667-7985116.1594986097