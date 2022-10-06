# taxi-service 
[Demo](https://whispering-retreat-18835.herokuapp.com)

## Description
This is a simple project made to show my skills in Java, JDBC, WEB, OOP, SOLID. 
In this project I used basic CRUD operations.
One-to-one, one-to-many, and many-to-many relationships are used in databases.

## Technologies
- Java 11
- Servlet Api
- JDBC
- JSTL
- JSP
- log4j 2
- TomCat 9.0.50
- MySQL
- Bootstrap 5

## How to use
- First, run the project.
- You can then create an account to log into the app.
- After that, you will have the following features:
    - Add car, driver or manufacturer
    - Display cars, drivers or manufacturers
    - Delete car, driver or manufacturer
    - Add driver to car
    - Remove driver from car
    - Display all cars for current driver

## Setup
- Clone this project
- Create the required tables using file resources/init_db.sql
- Add your db configurations in util/ConnectionUtil (username, password, url)
````
    private static final String URL = "Url connection string to DB";
    private static final String USERNAME = "USERNAME";
    private static final String PASSWORD = "PASSWORD";
````
- Config TomCat
- Run project using TomCat
