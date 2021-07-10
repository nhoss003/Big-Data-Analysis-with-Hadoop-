# Big-Data-Analysis-with-Hadoop-



<Overall description of the mapreduce jobs>
We chose to implement 3 jobs, because in the first job we calculated the average temperature for each month for each US state for each different station id.
In the second job, we calculated the average temperature for the same state and month for each station. In the same job, we calculated minimum month and temperature
and maximum month and temperature and difference. The third job contains the result in ascending ordering according to the difference.  

-First Job: (30 seconds)
It contains two maps and one reduce function. 

First map function 
Input: WeatherStationLocations.csv file. 
It filters its records and generates only those which have "US" in column country and the column state is not empty. As a result, we take
all the US station ids. 
Output: key->Station Id, value->state with prefix S

Second map function
Input: all the txt files
Output: key->Station Id, value->(month, temperature) with prefix D

Reduce function
Input: First and Second Map Output
Here, we do the join between the csv file and the txt files where the station id is the same. We also compute the average temperature of
each state, for each month. We do that for each different station of each state. 
Output: key->state, value->month, average temperature

-Second Job:(6 seconds)
it contains one map and one reduce function

Map funcion
Input: file created from the previous reduce function
Output: key->state, value->month, average temperature

Reduce function
Input: Map output
Since we computed the average temperature for each different station of each state, we now have multiple records for the same state and month.
That is why we calculate the average for the same state and month for each different station. Then, we compute the minimum temperature and month,
along with the maximum temperature and month for each state. Finally, we calculate the difference between maximum and minimum.
Output: key-> state, value-> min temperature, min month, max temperature, max month, difference

-Third Job (6 seconds)
it contains one map and one reduce function

Map function
Input: file created from the previous reduce function
Since we need to order the result according to the difference with ascending order, we need to create a map having as key the difference.
Output: key->difference, value->state, min temperature, min month, max temperature, max month

Reduce function
Input: Map output
Output: key->difference, value->state, min temperature, min month, max temperature, max month

<Join>
We filtered the csv file to contain only the US stations and then we did the join with the 4 txt files where the station id was the same
to take the month and the temperature. We chose to do the join after the filtering because we wanted to decrease the number of passes in
the large txt files that we had to do to calculate the average temperatures. 

<Number of reducers> 
We chose 8 reducers because we saw that the first job (takes the most time to be processed) takes 30 seconds to be processed
in relation to 44 seconds that it takes originally. 

<Execution & Testing>
We wrote a bash script called run which is included in the zip file. You can run the script by issuing the command ./run. Before doing so you need
to make sure that the run file has the right per permissions to be executed (i.e. execute chmod u+x run).
The script requires the following variables to be set:
1) jar=path to jar file location. the default jar file location is /home/nhoss001/preprocess.jar. The permissions of the jar are r and x for all users.
2) pathToStations=this is the path to the folder containing the WeatherStationLocations.csv file.
3) pathToData= this is the path to the folder containing the temperature data
4) reducers= this is a variable that specifies the number of reducers in the first job. It enables more parallelism for faster execution. It can be set
to 1 if we only need 1 reducer.

The paths specified above are relative and will work on z5 if they are specified like this. In our jar file we also make use relative folders to
store intermediate results. These folders are stored in /user/(username) according to the username of the person that logs in z5. The folders are 
automatically deleted after the jar execution stops. We use relative paths since we didn't know the actual structure of the TAs folder. It shouldn't
be a problem when the script is executed. Additionally, we assume that the jar file will be accessed from our folder since in the assignment there
was no mention on submitting also the jar file and also it was asked from us to specify the node in which we performed the experiments (z5). 
Please let us know if we need to submit it.

The script will run and after the multiple jobs are finished it will print the result in the screen using the command hdfs dfs -cat.

