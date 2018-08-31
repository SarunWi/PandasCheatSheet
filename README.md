# PandasCheatSheet

Cheat sheet for using pandas library for manage large excel file

## Prerequisites for using

1. install python 2.7 or 3+ [python 3.7 download](https://www.python.org/downloads/)
2. set path for python
3. install related library using pip  (pip should automatically install after install python, if not you can manually download pip in this following link) [pip download](https://pip.pypa.io/en/stable/reference/pip_download/)

``` cmd
pip install pandas

pip install numpy
```

## Usage

this repository already have an example method for 3 scenario.

1. _merge_2_file_ : to merge 2 file together using same External_Key column

2. _merge_not_in_ : to filter row in 1st file that 'External_Key_Field' is not in 2nd file

3. _find_record_ : to find record which have 'External_Key_Field' in specified list

please have a look at main.py file in main function

``` python
def main():
    _merge_2_file_()
```

above code mean that you attemp to run function _merge_2_file_() when run this python file

to run python file using cmd

``` cmd
....directory\python main.py
```

### note

this is just an example of how to use pandas library read and write a really large file

for more detail about this library please have a look at link below

[Panda merge document](https://pandas.pydata.org/pandas-docs/stable/merging.html)

### Authors

* **Sarun Wiriyapistan** - *Initial work* - [SarunWi](https://github.com/SarunWi)