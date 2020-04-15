REM for IndividualDetails.csv
kafka-topics.bat --zookeeper localhost:2181 --topic individual-details --create --partitions 1 --replication-factor 1

REM for covid_19_india.csv
kafka-topics.bat --zookeeper localhost:2181 --topic state-wise-history --create --partitions 1 --replication-factor 1

REM for HospitalBedsIndia.csv
kafka-topics.bat --zookeeper localhost:2181 --topic hospital-beds --create --partitions 1 --replication-factor 1

REM for ICMRTestingLabs.csv
kafka-topics.bat --zookeeper localhost:2181 --topic icmr-testing-labs --create --partitions 1 --replication-factor 1

REM for population_india_census2011.csv
kafka-topics.bat --zookeeper localhost:2181 --topic population-india-census-twenty-eleven --create --partitions 1 --replication-factor 1

REM for AgeGroupDetails.csv
kafka-topics.bat --zookeeper localhost:2181 --topic age-group-details --create --partitions 1 --replication-factor 1

REM for StatewiseTestingDetails.csv
kafka-topics.bat --zookeeper localhost:2181 --topic state-wise-testing-history --create --partitions 1 --replication-factor 1

REM for ICMRTestingDetails.csv
kafka-topics.bat --zookeeper localhost:2181 --topic icmr-testing-detils --create --partitions 1 --replication-factor 1