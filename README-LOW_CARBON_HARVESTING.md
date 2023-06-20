This branch modifies YARN scheduler to simulate low carbon harvesting scenario.

=============== pre-requisites ===============================================

We start from the Discreet Time Simulation implementation that is done for Hadoop YARN in https://issues.apache.org/jira/browse/YARN-1187.

For that, we checked out to hash: 654555783db0200aef3ae830e381857d2b46701e, and applied the provided patch from https://issues.apache.org/jira/secure/ViewProfile.jspa?name=108anup.

=============== Build the project ============================================
- Switch to Java 8. Set Java Home accordingly.
- Following changes were done to build the project without errors
    - Updated node version to above 12 to build /pvol/projects/hadoop/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-applications/hadoop-yarn-applications-catalog/hadoop-yarn-applications-catalog-webapp/pom.xml
    - Commented integration test scenarios
- Execute 'mvn package -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true'
- Upon successfull completion, the simulator is located at hadoop-3.3.5-src/hadoop-dist/target/hadoop-3.3.5/share/hadoop/tools/sls
