<p align="center">
<img src="https://github.com/Alterra-Greeve/.github/assets/133726246/3a58ead2-7977-4f31-8f29-bb54e55dc34b" width="350" />
</p>

<p align="center">
  <b>Greeve</b><br>
  Final Project Capstone Alterra Academy<br>
  <a href="https://intip.in/GreeveVisualization">Explore the Greeve Visualization Dashboard</a> 
  <br><br>
</p>

# About Project
<p align="justify">
  Greeve is an innovative app that aims to increase user awareness and participation in environmental conservation and make it easier to purchase eco-friendly goods and equipment. The app not only offers a platform to purchase environmental goods and equipment, but also provides information on the impact of users' activities on the environment as well as how to measure and reduce that impact. The following is greeve documentation from the data engineer side.
</p> 

# Our Data Engineer Team

| Name                        |
| --------------------------- |
| Adhira Riyanti Amanda       |
| Muhammad Dzikri Rizaldi     |
| Zahra Putri Zanuarti        |

# High Level Architecture
![High Level Architecture](/Pictures/High%20Level%20Architecture.png)

- **Data Collection**: We gather data from various sources, such as application logs using `Cloud Logging` and `MySQL` databases. This data includes user activities, transactions, challenges, and other relevant information.

- **ETL Process**: Using `Apache Airflow`, we schedule and manage the process of extracting data, transforming it into a usable format with `Python` scripts, and then loading it into `Firebase` for storage. This process ensures the data is clean and structured properly.

- **Automation and Storage**: We automate the transfer of data from `Firebase` to `Google BigQuery` tables using `Google Cloud Functions`. 

- **Data Analysis**: In `BigQuery`, we perform detailed analysis to understand patterns and trends in user behavior, such as how users choose eco-friendly products and participate in challenges.

- **Data Visualization**: We use `Looker Studio` to create interactive dashboards. These visualizations help stakeholders easily understand and make decisions based on the data, such as assessing the effectiveness of environmental initiatives.

# Star Schema
![Star Schema](/Pictures/Star%20Schema.png)
We use this star schema design in Big Query tables to simplify queries and improve their performance

# Visualization Dashboard
### [Looker Studio Dashboard](https://intip.in/GreeveVisualization)
![Looker Studio Dashboard](/Pictures/Visualization%20Dashboard.png)



