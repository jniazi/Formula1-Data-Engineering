## Formula One Races Data Warehousing
### Project Description
The goal of this project is to built a scalable and efficient data pipeline to ingests, processes, and analyzes large volumes of data in near real-time, and stores the processed data in a scalable way.

The dataset consists of data for the <a href='https://en.wikipedia.org/wiki/Formula_One' method='Post'>Formula One</a> series, from the beginning of the world championships in 1950 until 2021 using <a href='http://ergast.com/mrd/'>Ergast API</a>. The data is formatted in JSON and CSV in different possible ways including single- and multi-line JSON and single- and multi-file CSVs.

### Solution Architect

![architecture](https://user-images.githubusercontent.com/51984649/231533779-744b1d92-8991-42f5-8461-0f83c54ebb76.PNG)

This architecture has three stages.
<ul>
  <li><b>Data ingestion</b>: in this stage, the raw data is ingested into the delta lake and stored in the raw layer.</li>
  <li><b>Data transformation</b>: in this stage, the data is cleaned and processed in a way that can be used for analysis.</li>
  <li><b>Analysis</b>: in this stage, the data is analyzed.</li>
</ul>

### Implementation
For the implementation of this solution, Azure Databricks, Data Factory, Storage services, and GitHub are used as follows:
 <ul>
  <li>Leveraged Azure Databricks to ingest, process, and analyze the data using Python and SQL notebooks.</li>
  <li>Used Databricks Delta Lake to store the data in the Azure Blob container</li>
  <li>Implemented incremental load to ensure that the data was up-to-date.</li>
  <li>Used Azure Data Factory to create the data pipeline.</li>
  <li>Connected Power BI to Databricks to create interactive reports.</li>
  <li>Used GitHub for version control and collaboration</li>
 <ul>

### Some Visualizations of the Data
![dominant drivers](https://user-images.githubusercontent.com/51984649/231537453-14870cec-32ef-4f72-8628-299ef98d88be.PNG)
![dominant teams](https://user-images.githubusercontent.com/51984649/231537480-3d6fb82c-bcf2-41dc-a6e2-2c933c5c75b7.PNG)

