#!/bin/bash -e

###
#
# Create Table Test
# Insert ${TARGET} rows
#
# Useful when checking how fast things are going
#
###

echo "Executing test $0"

TARGET=10000

echo "CREATE TABLE Test (id INTEGER, name TEXT);" | target/release/baildon-glue -c inserting

for ((i=0;i<TARGET;i++)); 
do 
   value=$(printf "value_%04d" $i)
   echo "INSERT INTO Test VALUES(${i}, '$value');"
done | target/release/baildon-glue inserting
