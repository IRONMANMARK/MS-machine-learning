# Using machine learning approach to identify peptide modification sequence from mass spectrum
## overview
This project is using machine learning approach to identify the ppeptide modification sequence from mass spectrum. <br>
First the library.py will read in the raw data and put it in a database, the database is using sqlite package witch is a build in database for Python.<br>
Second, the fetch_data.py will purge the raw data in the database and create two tables that one contains unique peptide sequence and one contains unique peptide modification sequence.<br>
Finally the cnn.py will use the unique moodification sequence as an index
