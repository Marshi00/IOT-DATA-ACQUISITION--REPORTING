# IOT-DATA-ACQUISITION--REPORTING
<h1 align="center">
  <br>
 # In progress
 <br>
 <br>
  <!-- 
 <img src="images/gg2.jpg"  width="1100">
 -->
  <br>
  Development Technologies 
  <br>
</h1>



<p align="center">
  <img width="75px" hspace="20" src="images/python.png"  />
  <img width="75px" hspace="20" src="images/pandas.png"  />
  <img width="75px" hspace="20" src="images/plc.png" />
  <img width="75px" hspace="20" src="images/iot_comms.png" />
  <img width="75px" hspace="20" src="images/mysql.png" />

 
  
</p>

<p align="center">
  <a href="#Purpose">Purpose</a> •
  <a href="#CodeFlow">Code Flow</a> •
  <a href="#Screen">Screen</a> •

</p>

## Purpose
<p align="justify"> The purpose of your project is to efficiently collect, manage, and analyze data related to various attributes of water systems. This includes data such as chlorine levels, pump, filter, and generator statuses, and water pumped into the city, among other desired attributes. The project utilizes a Master Programmable Logic Controller (PLC) to collect data at specific intervals, with critical signals collected every 5 minutes, important signals every 15 minutes, and normal signals every 60 minutes. The Master PLC stores approximately 8 days of records using a First-In-First-Out (FIFO) logic, ensuring the latest records are retained within that timeframe. A Python script is then employed to extract and manipulate the data from the PLC and transfer it to a MySQL database. This script is scheduled to run every hour to move the data from the Master PLC to the database. The script also ensures data integrity by comparing the last records in the database with the last read from the Master PLC. If a match is not found, the script looks into the historical records of the past 8 days to determine where it left off and captures any missed data. In the event that a match cannot be found in the historical records, the script identifies the closest record on the Master PLC based on absolute time and continues reading from that point onward. Once the data is moved to the database, it is aggregated in a star schema and made available for analysis and reporting through parametrized SQL calls. This project aims to provide a comprehensive and reliable system for water data management and analysis.</p>

> **Note**
> Resources related to each step are divided into their respective folders.

## CodeFlow

Step 0: Initialize necessary variables and establish connections to the Master PLC and MySQL database.

Step 1: Set up the Master PLC to collect data from various fields such as water attributes, pump and filter statuses, and water pumped into the city. Configure the collection intervals: 5 minutes for critical signals, 15 minutes for important signals, and 60 minutes for normal signals.

Step 2: Continuously monitor the Master PLC for new data. Every 5 minutes (for critical signals), 15 minutes (for important signals), or 60 minutes (for normal signals), collect the desired attributes and statuses from the fields.

Step 3: Store the collected data in the Master PLC, utilizing a First-In-First-Out (FIFO) logic. Maintain approximately 8 days of records in the PLC.

Step 4: Implement a Python script to extract data from the Master PLC and transfer it to the MySQL database. Schedule the script to run every hour.

Step 5: Within the Python script, compare the last records in the database with the last read from the Master PLC to ensure data integrity. If they don't match, identify the point where the script left off.

Step 6: If a match is found, continue reading from the next available data point in the Master PLC and move the data to the database.

Step 7: If a match cannot be found in the historical records, determine the closest record on the Master PLC based on absolute time. Read from that point onward and transfer the data to the database.

Step 8: Once the data is successfully moved to the MySQL database, aggregate it in a star schema to facilitate analysis and reporting.

Step 9: Use parametrized SQL calls to perform various analyses and generate reports based on the aggregated data.

Step 10: Repeat steps 2-9 at regular intervals (every hour) to ensure continuous data collection, updates, and analysis.



---

> Website(https://) -
> GitHub (https://github.com/Marshi00) - 
> Linkedin (https:)
