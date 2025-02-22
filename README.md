## Description of tasks for this course

### Lab 1

1. Create a table `MyTable` with fields `id` (number), `val` (number).
1. Write an anonymous block that writes 10,000 random integer records to the MyTable table.
1. Write a custom function that outputs `TRUE` if there are more even val values in the `MyTable` table, `FALSE` if there are more odd values, and `EQUAL` if the number of even and odd values are equal.
1. Write a function that, based on the entered ID value, will generate and output to the console the text value of the insert command to insert the specified string.
1. Write procedures that implement DML operations (`INSERT`, `UPDATE`, `DELETE`) for the specified table.
1. Create a function that calculates the total reward for the year. The input to the function is the value of the monthly salary and the percentage of annual bonuses.<br>In the general case, $`total\_remuneration= (1+ percentage\_of\_annual\_bonuses)*12*monthly\_salary\_value`$.<br>Provide that the percentage is entered as an integer and it is required to convert it to a fractional number. Provide protection against entering incorrect data.

### Lab 2

1. Build two tables `GROUPS` and `STUDENTS` realizing group directory and student directory respectively.

   #### Groups
   
   |Field|Type|Constraints|Description|
   |-|-|-|-|
   |ID|Number|Primary key|Unique identifier for each record in a database table|
   |Name|Varchar2|NOT NULL and length of the value should be less than 100 characters|Name of the group|
   |C_val|Number|Non-negative integer|Count of the students in the group|

   #### Students
   
   |Field|Type|Constraints|Description|
   |-|-|-|-|
   |ID|Number|Primary key|Unique identifier for each record in a database table|
   |Name|Varchar2|NOT NULL and length of the value should be less than 100 characters|Name of the student|
   |Group_id|Number|Foreign key|References the ID field in the `GROUPS` table, enforcing referential integrity|
   
1. Realize triggers for the tables of task 1 integrity check (check for uniqueness of `ID` fields), autoincremental key generation and uniqueness check for `GROUP.NAME` field.
1. Implement a trigger that implements foreign key with cascading deletion between the `STUDENTS` and `GROUPS` tables.
1. Realize a trigger that implements journaling of all actions on the data of the `STUDENTS` table.
1. Based on the data from the previous task, implement a procedure to restore the information at the specified time and time offset.
1. Realize a trigger that will update the `C_VAL` information of the `GROUPS` table accordingly in case of data changes in the `STUDENTS` table.

### Lab 3

1. Write a procedure/function whose input is two text parameters (`dev_schema_name`, `prod_schema_name`), which are the names of database schemas (conventionally, the scheme for development (`Dev`) and industrial scheme (`Prod`)), the output of the procedure should provide a list of tables that are in the `Dev` schema, but not in `Prod`, or in which the structure of tables is different. The table names should be sorted according to the order of their possible creation in the prod schema (foreign key in the schema should be taken into account). In case of looped relationships, display a corresponding message.
1. Improve the previous script to take into account the possibility of comparing not only tables, but also procedures, functions, package indexes.
1. Improve the previous script to generate a ddl-script for updating objects, as well as taking into account the need to delete objects in the prod schema that are absent in the `Dev` schema.

### Lab 4

In modern development more and more often ORM-mechanisms are used, which mask SQL-code from the user, allowing to use some own API. Transfer data “wrapped” in some structure, so that the mechanism itself interacts with the database. Our task will be to write such a mechanism. To do this, we need to develop our own JSON or XML format structure.
Write a mechanism that allows you to realize at the API level the mechanism of formation and execution of dynamic SQL queries:
1. SELECT: the input is JSON/XML (student's choice), which specifies the type of query (`SELECT`), names of output columns, names of tables, conditions for combining tables for the query, filtering conditions. It is necessary to realize the parsing of input data, forming a query and executing it, to give the output cursor.
1. Nested queries: finalize item 1 so that it would be possible to pass a nested query (`IN`, `NOT IN`, `EXISTS`, `NOT EXISTS` conditions) as a filtering condition. Form the query, execute it, and pass the cursor to the output.
1. DML: realize the possibility to pass conditions for generation and execution of `INSERT`, `UPDATE`, `DELETE` queries as a structured file, with realization of the possibility to pass both conditions and subqueries as a filter (similar to block 2).

### Lab 5

One of the key features of modern databases is the ability to build retrospective queries, i.e. to execute queries so that they produce the result as it was in some foreseeable past, or to bring the data back to some point in the past (rollback changes). Our task is to make it possible to undo the changes made to some point in the past. For this purpose it is necessary to: 
1. Create three tables of arbitrary structure, necessary conditions: each table needs a primary key. The tables have at least 3 columns. Provide for the presence of foreign keys and the presence of columns of character type, numeric type and date-time type.
1. Implement a mechanism to save data changes in these tables (only DML changes are of interest).
1. Realize an overloaded batch procedure on the input of which is given either date-time or an interval in milliseconds in the first case should be rolled back all changes to a given date-time, in the second on the specified number of milliseconds back.
1. Provide a procedure for creating a report on changes that have occurred either since the last report or since the specified date-time. The report should contain information on each table about the number of INSERT, UPDATE, DELETE, changes that are canceled in the report should not be specified. The report should be generated in HTML format.
