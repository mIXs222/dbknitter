# How to Generate TPC-H Data

Adapted from [queries-tpch-dbgen-mysql](https://github.com/catarinaribeir0/queries-tpch-dbgen-mysql).

[Download the DBGEN and reference data](http://www.tpc.org/tpch/spec/tpch_2_16_0.zip) (enter contact, check email, downlaod the zip file). Unzip the downloaded file
Navigate through the command line to DBGEN folder  
```bash
cd Downloads/tpch_2_16_0/tpch_2_15_0/dbgen/
```  

Make a copy of the dummy makefile  
```bash
cp makefile.suite makefile
```  

In dbgen folder find the created makefile and insert highlighted values (bold) to this file.  
```makefile
################
## CHANGE NAME OF ANSI COMPILER HERE
################
CC      = gcc
# Current values for DATABASE are: INFORMIX, DB2, TDAT (Teradata)
#                                  SQLSERVER, SYBASE, ORACLE, VECTORWISE
# Current values for MACHINE are:  ATT, DOS, HP, IBM, ICL, MVS, 
#                                  SGI, SUN, U2200, VMS, LINUX, WIN32 
# Current values for WORKLOAD are:  TPCH
DATABASE= *QLSERVER*
MACHINE = *LINUX*
WORKLOAD = *TPCH*
#
...
```  

Run make command.  
```bash
make
```  

Generate the files for population. (The last numeric parametr determines the volume of data with which will be your database then populated - I decided that 0.1 (=100MB) is fine for my purposes, since I am not interested in the database benchmark tests.  
```bash
./dbgen -s 0.1
```  

This will generate `*.tbl` tables (really in "CSV" but delimited by `|`) locally. Typically, move them into a subdirectory for tidyness.
```bash
mkdir -p data/s01/
mv *.tbl data/s01/
```  
