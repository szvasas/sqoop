#!/bin/bash
echo "SELECT 1 from DUAL;" | sqlplus -L SQOOPTEST2/ABCDEF@//localhost:1521/sqoop
