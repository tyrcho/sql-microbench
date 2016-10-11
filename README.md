# build jar file

mvn package -DskipTests

# run

copy and edit bench.conf to set URL, thread count ...

java -jar microbench.jar