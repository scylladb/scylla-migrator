# Contributing to scylla-migrator

## Testing procedure

Tests are implemented in the `tests` sbt submodule. They simulate the submission of a Spark job as described in the README. Therefore, before running the tests you need to build the migrator fat-jar and to set up a stack with a Spark cluster and databases to read data from and to write to.

1. Build the migrator fat-jar and its dependencies

   ~~~ sh
   ./build.sh
   ~~~
   
2. Set up the testing stack with Docker
   
   ~~~ sh
   docker compose -f docker-compose-tests.yml up
   ~~~

3. Run the tests

   ~~~ sh
   sbt test
   ~~~

   Or, to run a single test:

   ~~~ sh
   sbt testOnly com.scylladb.migrator.BasicMigrationTest
   ~~~

4. Ultimately, stop the Docker containers

   ~~~ sh
   docker compose -f docker-compose-tests.yml down
   ~~~

Make sure to re-build the fat-jar everytime you change something in the implementation:

~~~ sh
sbt migrator/assembly
~~~

And then re-run the tests.
