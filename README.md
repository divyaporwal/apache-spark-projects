# Apache Spark Projects

Add the soc-LiveJournal1Adj.txt and the userdata.txt file to databricks or your local folder.
Export jar files from the projects and run them using the following commands.

Input:
Input files
1. soc-LiveJournal1Adj.txt<br/>
The input contains the adjacency list and has multiple lines in the following format:<br/>
<User><TAB><Friends><br/>
<User> is a unique integer ID(userid) corresponding to a unique user.<br/>
 
2. userdata.txt<br/>
The userdata.txt contains dummy data which consist of<br/>
column1 : userid (<User>) <br/>
column2 : firstname<br/>
column3 : lastname<br/>
column4 : address<br/>
column5: city<br/>
column6 :state<br/>
column7 : zipcode<br/>
column8 :country<br/>
column9 :username<br/>
column10 : date of birth.<br/>
  
#### Program 1: MapReduce program in Hadoop to implements a simple "Mutual/Common friend list of two friends". This program will find the mutual friends between two friends.<br/>

##### Logic : <br/>
Let's take an example of friend list of A, B and C. <br/>

Friends of A are B, C, D, E, F. <br/>
Friends of B are A, C, F. <br/>
Friends of C are A, B, E <br/>
So A and B have C, F as their mutual friends. A and C have B, E as their mutual friends. B and C have only A as their mutual friend. <br/>

##### Map Phase :
In map phase we need to split the friend list of each user and create pair with each friend. <br/>

Let's process A's friend list <br/>
(Friends of A are B, C, D, E , F) <br/>
Key | Value <br/>
A,B | B, C, D, E, F <br/>
A,C | B, C, D, E, F <br/> 
A,D | B, C, D, E, F <br/>
A,E | B, C, D, E, F <br/>
A,F | B, C, D, E, F

Let's process B's friend list <br/>
(Friends of B are A, C, F) <br/>
Key | Value <br/>
A,B | A, C, F <br/>
B,C | A, C, F <br/>
B,F | A, C, F <br/>
We have created pair of B with each of it's friends and sorted it alphabetically. So, the first key (B,A) will become (A,B).

##### Reducer Phase : 
After map phase is shuffling data item into group by key. Same keys go to the same reducer. <br/>
A,B | B, C, D, E, F <br/>
A,B | A, C, F <br/>

Shuffling into {A,B} group and sent to the same reducer. <br/>
A,B | {B, C, D, E , F}, {A, C, F} <br/>

So, finally at the reducer we have 2 lists corresponding to 2 people. Now, we need to find the intersection to get the mutual friends. <br/>

##### Optimization 
To optimize the solution i.e. to make the intersection faster I have used similar concept as merge operation in merge sort. 
I have sorted the friend list in the map phase. So, in reducer side we get 2 sorted lists. This way we can use the merge like operation to take only the matching values instead of going for all possible combinations in O(N2).

Please, make sure that the keys are sorted alphabetically so that we get friends list for 2 person on the same reducer.


#### Program 2. Find top-10 friend pairs by their total number of common friends. For each top-10 friend pair print detail information in decreasing order of total number of common friends. 

##### Output format can be:
<Total number of Common Friends> <TAB> <First Name of User A> <TAB> <Last Name of User A> <TAB><address of User A> <TAB><First Name of User B> <TAB> <Last Name of User B> <TAB><address of User B>

#### Program 3. List the 'user id' and 'rating' of users that reviewed businesses classified as “Colleges & Universities” in list of categories.
##### Data set info :
The dataset files are as follows and columns are separate using ‘::’ business.csv. review.csv. user.csv.

##### Dataset Description :
The data set comprises of three csv files, namely user.csv, business.csv and review.csv.<br/>
Business.csv file contain basic information about local businesses.<br/>
Business.csv file contains the following columns "business_id"::"full_address"::"categories"<br/>

'business_id': (a unique identifier for the business) 'full_address': (localized address),'categories': [(localized category names)]<br/>

review.csv file contains the star rating given by a user to a business. Use user_id to associate this review with others by the same user. Use business_id to associate this review with others of the same business.
review.csv file contains the following columns "review_id"::"user_id"::"business_id"::"stars" 'review_id': (a unique identifier for the review)<br/>

'user_id': (the identifier of the reviewed business),<br/>
'business_id': (the identifier of the authoring user),<br/>
'stars': (star rating, integer 1-5),the rating given by the user to a business<br/>
user.csv file contains aggregate information about a single user across all of Yelp user.csv file contains the following columns "user_id"::"name"::"url"<br/>
user_id': (unique user identifier),<br/>
'name': (first name, last initial, like 'Matt J.'), this column has been made anonymous to preserve privacy<br/>
'url': url of the user on yelp<br/>
Note: :: is Column separator in the files.<br/>

Required files are 'business' and 'review'.
##### Sample output
User id Rating 0WaCdhr3aXb0G0niwTMGTg 4.0


#### Program 4. List the business_id , full address and categories of the Top 10 businesses located in "NY" using the average ratings.
##### Sample output:
business_id | full address | categories | avg rating <br/>
xdf12344444444, CA 91711 List['Local Services', 'Carpet Cleaning'] 5.0

#### Program 5 : Spark Streaming Word Count. This is used to give a word count of the streaming data.
Run the sparkstream_word_count.py program and provide it the kafka host and the topic name.<br/>
Command : ./spark-submit sparkstream_word_count.py localhost:9092 test <br/>
"test" is the topic name and localhost:9092 is the kafka server address. <br/>

Run Stream Producer to produce continuous data and publish it on topic test. <br/>
Please, add the Guardian website APIs in the program by registering for guardian APIs and creating an API key.
