# Search Engine for Movie Plot Summaries

In this project, it will work with a dataset of movie plot summaries.

I will use the tf-idf technique to accomplish the above task. For more details on how to compute tf-idf using MapReduce, I refer these website to finish this project:

[Chapter 4 of the reference book](https://lintool.github.io/MapReduceAlgorithms/)

This project was done using Scala code that can run on a Databricks cluster or other clusters.

1. Extract and upload the ﬁle plot summaries.txt to Databricks or S3 folder. Also upload a ﬁle containing user’s search terms.

2. This program will **remove stopwords** by a method of your choice. It will create **a tf-idf for every term** and every document using the MapReduce method.

3. Read the search terms from the search ﬁle and output following:

   1. User enters a single term: It will output the top 10 documents with the highest tf-idf values for that term.

   2. User enters a query consisting of multiple terms: An example could be “Funny movie with action scenes”. In this case, it will need to evaluate cosine similarity between the query and all the documents and return top 10 documents having the highest cosine similarity values. 

      For cosine similarity, I refer following resources:

      [Resource Link 1](http://text2vec.org/similarity.html )

      [Resource Link 2](https://janav.wordpress.com/2013/10/27/tf-idf-and-cosine-similarity/)

      [Resource Link3](https://courses.cs.washington.edu/courses/cse573/12sp/lectures/17-ir.pdf)


