The purpose of this repo is to show different ways to ingest data into Databricks in a true incremental fashion

The main approaches are as follows:

1. Custom watermarking process
2. DLT 
3. Structured streaming 

This repo will provide examples of each and show when each approach would make sense to use

How to use: 
- Fill in the desired catalog and schema in the 00_set_up_work file and then run this file
- Start running the 00_zip_code file to populate all the zip codes in the US 