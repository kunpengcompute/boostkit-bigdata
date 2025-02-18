#!/bin/bash
rm -f *.jar
function copy_jar() {
  for jar in $(find . -name "*.jar" | grep -v "ut")
  do
    jar_name=$(basename "$jar")
    if [ ! -f "./$jar_name" ]; then
      \cp -f "$jar" .
    fi
  done
}

#mvn clean package -Pspark-3.1
#copy_jar
mvn clean package -Pspark-3.4
copy_jar