NAME=Profile
FILE=data.json # data file
DIR=bd/hw9/ # hdfs directory
RES=out2 # output directory

INPATH=$DIR$FILE
OUTPATH=$DIR$RES
# hdfs dfs -put $FILE $DIR 

javac -classpath `yarn classpath` -d . ${NAME}Mapper.java
javac -classpath `yarn classpath` -d . ${NAME}Reducer.java
javac -classpath `yarn classpath`:. -d . ${NAME}.java
jar -cvf $NAME.jar *.class
hdfs dfs -rm -r $OUTPATH
hadoop jar $NAME.jar $NAME $INPATH $OUTPATH
echo iteration 1: > output
hdfs dfs -cat $OUTPATH/part-r-00000 >> output