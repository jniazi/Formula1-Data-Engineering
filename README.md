## Formula One Races Data Warehousing
### Project Description
The goal of this project is to built a scalable and efficient data pipeline to ingests, processes, and analyzes large volumes of data in near real-time, and stores the processed data in a scalable way.

The dataset consists of data for the <a href='https://en.wikipedia.org/wiki/Formula_One' method='Post'>Formula One</a> series, from the beginning of the world championships in 1950 until 2021 using <a href='http://ergast.com/mrd/'>Ergast API</a>. The data is formatted in JSON and CSV in different possible ways including single- and multi-line JSON and single- and multi-file CSVs.

### Solution Architect

![architecture](https://user-images.githubusercontent.com/51984649/231533779-744b1d92-8991-42f5-8461-0f83c54ebb76.PNG)

This architecture has three stages and I leveraged Azure Databricks to implement it.
<ul>
  <li>Data ingestion: in this stage the raw data is ingested into the delta lake and stored into raw layer.</li>
  <li></li>
  <li></li>
</ul>
