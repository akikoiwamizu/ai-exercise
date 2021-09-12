
# Analyst Institute - Data Engineer Skills Test

**Date**: September 12, 2021

## Installation

Use [conda](https://www.anaconda.com/products/individual) to create a virtual environment and load in the required packages.

```bash
conda create -n <your_environment_name> python=3.8
conda activate <your_environment_name>
pip install -r requirements.txt
```

Usage Examples:

```python
>>> import main
>>> main.run("files", "oh11.csv", "oh11_schema.txt", "dev", "hotel.oh_cd11_raw")
>>> main.run("files", "oh_cd11_clean.csv", "oh11_schema.txt", "dev", "hotel.oh_cd11_clean")
```

## Overview

For your skills evaluation, you will be loading a voter file into an Amazon Redshift Database and joining it
to a data set. This evaluation will test your ability to load large datasets, clean them and prepare them
for delivery to analysts.

## Instructions

**#1: Load the file into the your schema of the Redshift Database. Name the table oh_cd11_raw. Please provide any code that was utilized in order to load in the data.**

* Corresponding Python Scripts: main.py and utils/


**#2: Analyze the table for any issues that you’ve found in the data. This could include obvious coding errors or duplicates. Use your best judgement to transform and update the table into a new table, in your schema, named oh_cd11_clean. List the changes made and provide all code that was used to do so. Also, list any checks that you made, but didn’t find any errors.**

* Corresponding Jupyter Notebook: poc/cleaning_file.ipynb**


**#3: Provide counts of exp_02 with a column indicating if they voted in the 2020 general election (g2020 column).**

* Corresponding SQL Scripts: sql/


**#4: Provide counts of exp_01 by state house district (state_hd column).**

* Corresponding SQL Scripts: sql/


**#5: Provide the number of unique addresses on the file.**

* Corresponding SQL Scripts: sql/


**#6: Provide the average of exp_03 segmented by decile from smallest to largest.**

* Corresponding SQL Scripts: sql/



## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

## License
[MIT](https://github.com/UC-Berkeley-I-School/Project2_Bi_McCleary_Iwamizu/LICENSE)
