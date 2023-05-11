#!/bin/bash -e

###
#
# Insert ${TARGET} k/v pairs
#
# Useful when checking how fast things are going
#
###

echo "Executing test $0"

TARGET=10000

for ((i=0;i<TARGET;i++)); 
do 
   key=$(printf "key_%04d" $i)
   value=$(printf "value_%04d" $i)
   echo "insert $key $value"
done | target/release/baildon-store -c default.db
