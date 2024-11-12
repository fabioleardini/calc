# Calc Assessment

## Scenario
A “developer”, whom we shall name Bob, was tasked to build a service to calculate the tax payable for 1500 employees located across 4 different countries using the respective country’s tax calculation regime. It became evident that Bob proceeded, in desperation, to outsource the work to ChatGPT. Bob generated all the code through several prompts and then shipped the code to production.

After a few serious support queries came in from very angry clients, Bob decided to go on lunch and never return.

You have been approached to help fix the buggy service and save the company’s future.

## Resources
You have been provided with a zip file containing all of Bob’s code.
In this zip file you will find a .Net Solution containing the following projects
-	**PaySpace.Calculation.Assessment.Console** – The console application that Bob wrote.
-	**PaySpace.Calculation.Assessment.Database** – A database project which you can use to schema compare with a SQL Server database. This project contains the table definitions that Bob wrote, as well as a PostDeployment.sql script which you will need to use to seed your database with data.

## The Original Spec
The original spec as handed to Bob is as follows:
Build a service that will bulk calculate and store the tax and net salary amounts for a given list of gross salaries using the specified country’s tax calculation regime.

We support three different taxation regimes:
-	**Progressive** – Where income is taxed according to the applicable bracket a tax table (read up on progressive tax calculations if you are unfamiliar with how they work).
-	**Flat rate** – Where tax is a flat rate provided the income exceeds a given minimum amount.
-	**Percentage based** – Where tax is calculated as a percentage of income.

The tax calculation regime for each one of the countries we will support are as follows:

| Country      | Tax Regime  | Brackets/Rates                                                                                                |
| ------------ | ----------- | ------------------------------------------------------------------------------------------------------------- |
| USA          | Progressive | ![image](https://github.com/PaySpaceSA/calc-assignment/assets/159135258/dfabe11c-0bb0-4c4f-897c-aa24328b3ba2) |
| South Africa | Progressive | ![image](https://github.com/PaySpaceSA/calc-assignment/assets/159135258/5041d874-60a2-4cb2-b93d-ebd527e4f361) |
| Zimbabwe     | Flat Rate   | 20 000 ZWL if income exceeds 150000 ZWL                                                                       |
| Mauritius    | Percentage  | 30%                                                                                                           |


As part of this task:
-	A table has already been created named TaxCalculation. In the PostDeployment script, this table is seeded with data.
-	Design a schema to store the country’s respective brackets and rates.
-	Add two new columns to the TaxCalculation table to store the calculated tax and net pay amounts for each of the given income amounts.

## Your Mission
Bob made a concerted effort to design the database schemas. The service that he wrote does succeed in querying the relevant data and writing the calculated values accordingly. However, based on the number of support queries that Bob got, there is a bug somewhere in the calculation. The service also seems rather slow in calculating the tax amounts for only 1500 employees.

We will be assessing you on the following:
-	Performance: The service is slow and has a lot of room for improvement. We will be assessing your ability to optimize and justify on a technical level why the changes you have made improve performance.
-	Bug fixes: There is a bug somewhere that caused all the support queries. Try to find this bug and fix it. Additional acknowledgement will be awarded if you spot any other bugs that have not been picked up yet.
-	Clean up: The entire service is smashed into a single file. Refactor the code as much as possible to make it more extensible and readable (Keep SOLID in mind). 
-	Unit Tests: Add unit tests to cover the various elements that make up the service and the calculation regime logic.
-	SQL: Your ability to write efficient SQL and any other changes that would benefit the solution.

Note:
-	The PostDeployment.sql seeds your database with all the relevant rates, brackets and income amounts to calculate for their respective countries.
-	Brackets are fictitious and doensn't need to match current legislation.
-	The use of stored procedures is encouraged for interacting with the Database.
-	Feel free to make use of any packages that you see fit.
-	Once completed, please zip your repo & share it with TA Specialist you are working with via a Google Drive / similar link.
-	DON'T push your solution to Github or any other public platform.

Hint: All the support queries that Bob received was from USA and SA clients. 
