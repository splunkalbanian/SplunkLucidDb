
EXPECTED_ARGS=1
E_BADARGS=65
if [ $# -ne $EXPECTED_ARGS ]
then
  echo "Usage: $0 <install-dir>"
  exit $E_BADARGS
fi

type -P java &>/dev/null || { echo "java required but it's not installed (or not in path).  Aborting." >&2; exit 1; }


INSTALL_HOME=$1
RELEASE_HOME=`dirname $0`

if [ ! -d $INSTALL_HOME ]
then
    echo "INSTALL_HOME=$INSTALL_HOME - is not a valid directory. Aborting"
    exit 2
fi


# get absolute INSTALL_HOME, RELEASE_HOME
pushd . > /dev/null
cd $INSTALL_HOME
INSTALL_HOME=$PWD
popd > /dev/null 


pushd . > /dev/null
cd $RELEASE_HOME
RELEASE_HOME=$PWD
popd > /dev/null


echo "INSTALL_DIR=$INSTALL_HOME"
echo "RELEASE_DIR=$RELEASE_HOME"


pushd . > /dev/null

cd $INSTALL_HOME
echo "Downlowing luciddb-bin-linux64-0.9.3.tar.bz2 ...."
wget "http://sourceforge.net/projects/luciddb/files/luciddb/luciddb-0.9.3/luciddb-bin-linux64-0.9.3.tar.bz2/download" 2> /dev/null

echo "Downlowing PG2LucidDB_1.0.0.0.zip ...."
mkdir PG2LucidDB
cd PG2LucidDB
wget "http://pg2luciddb.googlecode.com/files/PG2LucidDB_1.0.0.0.zip" 2> /dev/null
unzip PG2LucidDB_1.0.0.0.zip

echo "Downloading done! Installing ..."

echo "Installing luciddb ..."
cd ../
bunzip2 -c luciddb-bin-linux64-0.9.3.tar.bz2 | tar -xvf -
cd luciddb-0.9.3
sh install/install.sh
cd ../

# finish installing PG2LucidDB
cp PG2LucidDB/build/pg_catalog_plugin.jar  luciddb-0.9.3/plugin/ 

echo "Fixing PG2LucidDB/lib/LucidDbClient.jar"
# replace PG2LucidDB luciddb drivers (the ones that ship are old and default to RMI)
mv PG2LucidDB/lib/LucidDbClient.jar        PG2LucidDB/lib/LucidDbClient.jar.orig
cp luciddb-0.9.3/plugin/LucidDbClient.jar  PG2LucidDB/lib/LucidDbClient.jar

# replace PG2LucidDB.properties
echo "Fixing PG2LucidDB/conf/PG2LucidDB.properties"
mv PG2LucidDB/conf/PG2LucidDB.properties PG2LucidDB/conf/PG2LucidDB.properties.orig

cp $RELEASE_HOME/PG2LucidDB.properties   PG2LucidDB/conf/PG2LucidDB.properties

cd $RELEASE_HOME
# now install the splunk plugin
sh install.sh $INSTALL_HOME/luciddb-0.9.3 $INSTALL_HOME/PG2LucidDB


popd > /dev/null





