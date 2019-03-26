# MaSTr download scripts

There are two scripts for downloading information from [https://www.marktstammdatenregister.de/MaStR](https://www.marktstammdatenregister.de/MaStR).

 1. **fetch_solar_mastr_numbers.py** fetch general information about solar units (especially their mastr numbers that 
 are used in second script) and save them as CVS
 2. **fetch_solar_data** retrieves detail information about the solar units fetched in first step.
 
 To run these script you have to have API key and Mastr number. Both scripts have CLI interface, so just run 
 --help to see more details.
 
 ## Setup
 
 **Requirements:**
 
 * Python 3.6 and higher
 * pip
 * Mastr number & API key (you have to register for that)
 
 You might wannna create virtual environment first.
 
 ```shell
$ pip install -r requirements.txt 
```

