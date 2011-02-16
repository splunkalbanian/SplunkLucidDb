

EXPECTED_ARGS=2
E_BADARGS=65
if [ $# -ne $EXPECTED_ARGS ]
then
  echo "Usage: $0 <luciddb-home> <pg2luciddb-home>"
  exit $E_BADARGS
fi

LUCIDDB_HOME=$1
PG2LUCID_HOME=$2

if [ ! -d $LUCIDDB_HOME ]
then
    echo "LUCIDDB_HOME=$LUCIDDB_HOME - is not a valid directory"
    exit 1
fi

if [ ! -f $LUCIDDB_HOME/bin/lucidDbServer ]
then
    echo "Invalid LUCIDDB_HOME=$LUCIDDB_HOME, could not find lucidDbServer"
    exit 2
fi

### Now check the validity of PG2LUCID_HOME

if [ ! -d $PG2LUCID_HOME ]
then
    echo "PG2LUCID_HOME=$PG2LUCID_HOME - is not a valid directory"
    exit 3
fi


if [ ! -f $PG2LUCID_HOME/pg2luciddb ]
then
    echo "Invalid PG2LUCID_HOME=$PG2LUCID_HOME - could not find pg2luciddb"
    exit 4
fi

#################################################################################
######################### VALIDATION COMPLETED ###### ###########################
#################################################################################

pushd . > /dev/null
RELEASE_HOME=`dirname $0`
cd $RELEASE_HOME
RELEASE_HOME=$PWD
popd



#1. COPY splunk.jar to plugin dir
echo "Copying $RELEASE_HOME/splunk.jar to $LUCIDDB_HOME/plugin/"
cp $RELEASE_HOME/splunk.jar $LUCIDDB_HOME/plugin

#2 create init.sql
echo "Creating init.sql"
cat $RELEASE_HOME/init.sql.template | sed "s#/path/to/luciddb/#$LUCIDDB_HOME#g" > $RELEASE_HOME/init.sql


echo "#######################################################################################"
echo "# Splunk plugin for LucidDb successfully installed !"
echo "# To get started: "
echo "# 1. start luciddb, it needs a terminal so use screen, $LUCIDDB_HOME/bin/lucidDbServer"
echo "# 2. edit the following file to match your environment: $RELEASE_HOME/init.sql"
echo "# 3. connect to luciddb using $LUCIDDB_HOME/bin/sqlineClient"
echo "# 4. once connected issue: !run $RELEASE_HOME/init.sql"
echo "# 5. (optional) start the pg2luciddb bridge, this needs a terminal too, so use screen."
echo "# 6. start running queries against splunk foreign tables"
echo "#######################################################################################"


