File description

**map_reduce.py**
A back-end for a MapReduce system and a couple MapReduce jobs: word count, and set difference.


**blog_spark.py**

Search for mentions of industry words in the blog authorship corpus. The goal here is to first find all of the possible industries in which bloggers were classified. Then, to search each blogger’s posts for mentions of those industries and, counting the mentions by month and year. 

Download the corpus here: http://u.cs.biu.ac.il/~koppel/BlogCorpus.htm (Note: the site rate-limits the speed of the download. It will take several minutes.)

Each file in the corpus is named according to information about the blogger: user_id.gender.age.industry.star_sign.xml

Within each xml file, there is a “<date>” tag which indicates the date of a proceeding “<post>”, which contains the text of an individual blog post. 


**wordcount_setdiff_spark.py**
Implemented WordCount and SetDifference in Spark