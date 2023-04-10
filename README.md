# Categorizing text content using the Wikipedia category graph, Neo4j and Python

This repo reproduces a methodology proposed by [Biuk-Aghai & Cheang, 2011, Wikipedia Category Visualization Using Radial Layout](https://www.opensym.org/ws2011/_media/proceedings%253Ap193-biuk-aghai.pdf) to link Wikipedia articles to their most representative category branch in the very dense Wikipedia category graph.

![Radial visualization of English Wikipedia from Biuk-Aghai & Cheang, 2011, Wikipedia Category Visualization Using Radial Layout](wiki-visualization.png)
*Radial visualization of English Wikipedia from Biuk-Aghai & Cheang, 2011*

Wikipedia articles are typically categorized into several categories that form a directed cyclical graph. This is an example of the article "Finland"'s categories:

>Categories: FinlandNorthern European countriesMembers of the Nordic CouncilMember states of the European UnionMember states of the Union for the MediterraneanMember states of the United NationsMember states of NATOPost–Russian Empire statesRepublicsStates and territories established in 1917Swedish-speaking countries and territoriesFennoscandiaCountries in EuropeChristian statesCountries of Europe with multiple official languagesOECD members

Articles belong to several categories and categories can belong to several parent categories that themselves can belong to their own child categories. The category graph can form a branch from the article all the way to one of 40 [Main Topics](https://en.wikipedia.org/wiki/Category:Main_topic_classifications) that include "People", "Geography", "Food and drink" etc. By calculating the strongest and most representative path through the category graph one can use the information included in Wikipedia to attach category meta data to key words. E.g. deducing that the word "Finland" will belong to the top category "Geography".

The Wikipedia category graph is extracted from the english Wikipedia data dump and loaded into a Neo4j graph database. The algorithm proposed by Biuk-Aghai & Cheang is applied to a list of key words giving the corresponding categorization branch for each word.

See folders final-notebooks and source-data for workflow.

Identify keywords from trivia question; Connect to corresponding Wikipedia article; Identify article category parents; Calculate strongest category path from article to Wikipedia's main topics
