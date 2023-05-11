#!/bin/bash -e

###
#
# Insert ${TARGET} k/v pairs, then delete them
#
# Useful when checking that basic deletion is working
#
###

echo "Executing test $0"

TARGET=100

for ((i=0;i<TARGET;i++)); 
do 
   key=$(printf "key_%04d" $i)
   value=$(printf "value_%04d" $i)
   if ((i==0)); then
       target/release/baildon-store -c default.db insert "${key}" "${value}"
   else
       target/release/baildon-store default.db insert "${key}" "${value}"
   fi
done

for ((i=0;i<TARGET;i++));
do 
   key=$(printf "key_%04d" $i)
   echo "BEFORE DELETE key: ${key}"
   echo "BEFORE DELETE nodes"
   target/release/baildon-store default.db nodes
   target/release/baildon-store default.db delete "${key}"
   echo "AFTER DELETE nodes"
   target/release/baildon-store default.db nodes
   echo "AFTER DELETE verify"
   target/release/baildon-store default.db verify
done
