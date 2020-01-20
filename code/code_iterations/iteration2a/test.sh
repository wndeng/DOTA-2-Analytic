NAME=Analytic
FILE=data.json # data file
DIR=project/ # hdfs directory
RES=out # output directory

INPATH=$DIR$FILE
OUTPATH=$DIR$RES
# hdfs dfs -put $FILE $DIR 

javac -classpath `yarn classpath` -d . ${NAME}Mapper.java
javac -classpath `yarn classpath` -d . ${NAME}Reducer.java
javac -classpath `yarn classpath`:. -d . ${NAME}.java
jar -cvf $NAME.jar *.class
hdfs dfs -rm -r $OUTPATH
hadoop jar $NAME.jar $NAME $INPATH $OUTPATH
