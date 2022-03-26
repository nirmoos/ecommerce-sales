# Ecommerce Sales

## Installation

Use the requirements.txt file to install all the necessary packages (better to use a separate virtual environment)

```commandline
pip install -r requirements.txt
```

## Requirements

  1. Python 3.8+
  2. MySQL  8.0+

## Prerequisites

Please follow the following steps prior to execution of the program

* Setting the mock data for the execution
  1. Download the sample data from [kaggle](https://www.kaggle.com/datasets/carrie1/ecommerce-data)
  2. Copy it to the data folder under root folder
  3. Rename it to data.csv

* Setting the database connection (follow this [how to guide](https://www.digitalocean.com/community/tutorials/how-to-install-mysql-on-ubuntu-20-04) to do the below steps)
  1. Install mysql database if it is not done already
  2. Create a user with a password
  3. Create a database
  4. Grant the user appropriate privileges on the database
  5. Create a .env file under root folder and provide values from the above steps (Please use the .env.sample file for the keys)

## Usage

Navigate to the root folder prior to execution

```commandline
python main.py --force-load false
```

force-load argument is optional and defaults to false. If it is provided, it will reload data from CSV