In this project, we worked with a dataset of movie plot summaries. We are interested in building a search engine for the plot summaries.
You will use the tf-idf technique to accomplish the above task. 

This project used Scala code that can run on a notebook cluster.

Below are the steps of the project:

1. Remove stopwords by a method of your choice.

2. Create a tf-idf for every term and every document using the MapReduce method.

4. Read the search terms from the search ﬁle and output following:

(a) User enters a single term: You will output the top 10 documents with the highest tf-idf values for that term.

(b) User enters a query consisting of multiple terms: An example could be “Funny movie with action scenes”. In this case, we evaluated cosine similarity between the query and all the documents and return top 10 documents having the highest cosine similarity values. 

5. Use the movie.metadata.tsv ﬁle to lookup the movie names and display movie name on the screen.
