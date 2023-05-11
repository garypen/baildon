#!/bin/bash -e

###
#
# Insert ${TARGET} k/v pairs
#
# Useful when checking insert is working
#
###

echo "Executing test $0"

TARGET=100

for ((i=0;i<TARGET;i++)); 
do 
   key=$(printf "key_%04d" $i)
   value=$(printf "value_%04d" $i)
   echo "BEFORE INSERT key: ${key}"
   if ((i==0)); then
       target/release/baildon-store -c default.db insert "${key}" "${value}"
   else
       target/release/baildon-store default.db insert "${key}" "${value}"
   fi
   target/release/baildon-store default.db verify
done

echo "AFTER INSERT nodes"
target/release/baildon-store default.db nodes
