#!/bin/sh

# Not using appscript because it vomits debug output to stdout by default

CLASSPATH=$(dirname $0)/../lib/buildtool-format-runtimeAppPathing.jar
MAIN_CLASS=optimus.buildtool.format.app.FilterJavaRuntimeOptions

/ms/dist/msjava/PROJ/azulzulu-openjdk/11.0.7/bin/java -cp $CLASSPATH $MAIN_CLASS $@