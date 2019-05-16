# Using machine learning approach to identify peptide modification sequence from mass spectrum
## overview
This project is using machine learning approach to identify the ppeptide modification sequence from mass spectrum. <br>
First the library.py will read in the raw data and put it in a database, the database is using sqlite package witch is a build in database for Python.<br>
Second, the fetch_data.py will purge the raw data in the database and create two tables that one contains unique peptide sequence and one contains unique peptide modification sequence.<br>
Finally the cnn.py will use the unique moodification sequence as an index to pick out the mass spectrum data that belong to a specific peptide and put it in a matrix and use this matrix to train the model once at a time.<br>
I am using 90% of the data to train the model and 10% the data to test the data.
## how to use
* first run the library.py to generate the raw data database from mgf file.
* second run the fetch data to select part of the peptide data in the databse and generate the unique peptide sequence table and modification table.
* third run the cnn.py which using the unique peptide sequence index to extract the specific data out. And using the data to train the model.
## Important notice
* The mgf file file is just a small mgf file used to test the code. The original mgf raw data file will generate a 245GB database file and contains 250 million peptide mass spectrum data and around 2.2 x 10^8 entrys. So the file here is just a test of concept.
* Do NOT use the database you generated to run the cnn.py. Because the sample is too small, it doesn't mean anything. Also, using the databse generated will cause seriously issue. Because the labels for the trainning is smller than it should have. Use the database provide here to run the cnn.py.
* When you run the cnn.py code after some time there will be a resource exhausted error. This is because the code is NOT meant for laptop or ordinary desktop. This cnn.py has already shrink 8 times to just be able run on a 64GB memory server. So you can try this code but it will have a resourse exhausted error after several seconds. And the code can run perfectly fine if there is proper server.
* It will take at least a week even for that test database I provided to finish the training in a server, so the result will not be available.
* The training time is super long so that is why I fetch fraction of the whole data.
* When the cnn.py runing, it should look like this. The warning I got is just because I do not got enough memory on my laptop.<br>
![image](https://github.com/IRONMANMARK/MS-machine-learning/blob/master/visualization/cnn_benchmark2.png)
![image](https://github.com/IRONMANMARK/MS-machine-learning/blob/master/visualization/cnn_run_benchmark.png)
## database visualization
* I am using SQLite Expert to visualize the databaze<br>
![image](https://github.com/IRONMANMARK/MS-machine-learning/blob/master/visualization/database_visual.png)
![image](https://github.com/IRONMANMARK/MS-machine-learning/blob/master/visualization/database_visual2.png)
## Packages need to be installed
* Tensorflow
* keras
* tqdm
