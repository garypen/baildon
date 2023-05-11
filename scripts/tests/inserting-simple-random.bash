#!/bin/bash -e

###
#
# Insert ${TARGET} k/v pairs in random order
#
# Useful when checking insert is working
#
###

echo "Executing test $0"

TARGET=100

for ((i=0;i<TARGET;i++)); 
do 
   num=$((RANDOM % 100))
   key=$(printf "key_%04d" $num)
   value=$(printf "value_%04d" $i)
   echo "BEFORE INSERT nodes"
   target/release/baildon-store default.db nodes
   if ((i==0)); then
       target/release/baildon-store -c default.db nodes
   else
       target/release/baildon-store default.db nodes
   fi
   echo "BEFORE INSERT key: ${key}"
   target/release/baildon-store default.db insert "${key}" "${value}"
   target/release/baildon-store default.db verify
done

echo "AFTER INSERT nodes"
target/release/baildon-store default.db nodes
