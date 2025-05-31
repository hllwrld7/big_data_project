# big_data_project
Project for my Big Data class.

## What does it do?
It is basically an application that does the following things in a sequence:
1. Searches datasets on Kaggle with the "video game" keyword.
2. Downloads the .csv files for the datasets that match the search criteria.
3. Creates tables for these .csv files in the MySQL database.
4. Loads data from the files to the database.
5. Merges the tables based on the "Genre" column to make one big table.

The table is then loaded to Power BI so that the aggregated data can be visualized and analyzed.
Examples (they're all in the big_data_project.pbix file):
![EU Sales by Genre.](/pictures/eu_sales_by_genre.png)
![Best selling publisher.](/pictures/best_selling_publisher.png)
![Sales by platform.](/pictures/sales_by_platform.png)

## How to run it?
The only thing needed is a MySQL database.
Go to \BigDataProject\BigDataProject\appsettings.json, configure it, and it should run.