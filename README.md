# flink-phoenix-sample
This sample generates some measurements, aggregates the measurements over time and writes the maximum value to Phoenix.
The example creates the table if it doesn't exist already so there is no setup required.

# Instructions for running sample
## Create a Cluster with Flink, Yarn, HBase and Phoenix
## If using Ranger and Kerberos, create a user and create policies that allow the user to access hbase
## Create the sample.properties config file
### From Cloudera Manager, find the zookeeper hosts.
### Create a file called sample.properties using the template below.

```text
phoenix.measurement.table=measurements
phoenix.db.url=jdbc:phoenix:<comma separated zk nodes>:2181:/hbase;autocommit=true
phoenix.db.user=<user to connect to hbase/phoenix>
phoenix.db.pw=<pw for user connecting to hbase/phoenix>
phoenix.db.batchSize=10
```
## Build the flink sample jar.
``text
mvn clean package
``
## Scp the jar file to an edge node that has the flink client installed.
## Run the flink job
### copy the hbase-site.xml from /etc/hbase/conf or download it from cloudera manager.
### If running securely, Download a keytab and create keystore.  The script below is an example of using the cdp api to do this:
For more info on running securely, see the [flink secure tutorial](https://github.com/cloudera/flink-tutorials/tree/1.12-csa1.4.0.0/flink-secure-tutorial)
```shell script
if [[ $# -ne 1 ]]; then
    echo "deploy_cybersec <environment_name>" >&2
    exit 2
fi

env_name=$1
cdp environments get-keytab --environment-name $env_name | jq -r '.contents' | base64 --decode > user.keytab
cybersec_user_princ=`ktutil --keytab=user.keytab list | awk '(NR>3) {print $3}' | uniq`
echo "PRINCIPAL="$cybersec_user_princ
```
### Start the job.
```shell
flink run --jobmanager yarn-cluster -yjm 1024 -ytm 1024 --detached --yarnname "Flink Phoenix Sample" \
-yD security.kerberos.login.keytab=user.keytab \
-yD security.kerberos.login.principal=cduby@SE-SANDB.A465-9Q4K.CLOUDERA.SITE \
-yD security.ssl.internal.enabled=true \
-yD security.ssl.internal.keystore=keystore.jks \
-yD security.ssl.internal.key-password=storepass \
-yD security.ssl.internal.keystore-password=storepass \
-yD security.ssl.internal.truststore=keystore.jks \
-yD security.ssl.internal.truststore-password=storepass \
-yt keystore.jks \
-yt hbase-site.xml \
flink-phoenix-sample-1.0-SNAPSHOT.jar sample.properties
```


