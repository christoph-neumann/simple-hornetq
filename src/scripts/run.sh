#!/bin/bash

# Run the SingleMessage example of no other class is given
class="app.SingleMessage"
if [ "$1" != "" ]; then
	class="app.$1"
fi

app_home=`pwd`
java=`which java`

options="-Xmx256m -XX:MaxPermSize=128m -server"

echo "java:      $java"
echo "class:     $class"
echo "options:   $options"

# Put all the jars in lib on the classpath
classpath="${app_home}/lib"
libs=`ls ${app_home}/lib/*.jar`
for i in $libs; do
    classpath=$classpath:$i
done
echo ""

$java $options -cp $classpath $class $*
