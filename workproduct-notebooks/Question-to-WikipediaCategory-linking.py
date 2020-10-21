#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# NOTE
# cypher.forbid_exhaustive_shortestpath=true set in neo4j conf file
# https://neo4j.com/docs/operations-manual/current/configuration/neo4j-conf/

# dbms.transaction.timeout and dbms.lock.acquisition.timeout set to 10s in neo4j.conf


# ### Setup

# In[ ]:


import pandas as pd
from string import punctuation, digits
import re
import ast
import stopit
from py2neo import *
import time


# In[ ]:


pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.max_colwidth', 1000)


# In[ ]:


print("Started!")


# #### Keyword to article title mapping data

# In[ ]:


#%%time
redirects = pd.read_csv("F:/wikipedia-data/outputs/redirect.csv")


# In[ ]:


#%%time
articles = pd.read_csv("F:/wikipedia-data/outputs/articles.csv")


# In[ ]:


#%%time
articles.dropna(subset = ["title"], inplace = True)
redirects.dropna(subset = ["title"], inplace = True)


# In[ ]:


#%%time
articles["titleLower"] = articles["title"].apply(lambda x: x.lower())
redirects["titleLower"] = redirects["title"].apply(lambda x: x.lower())


# #### Graph database connection

# In[ ]:


# Connect to neo4j database - start database separately
graph = Graph()


# In[ ]:


# Set timeout-limit in seconds for database calls
maxSearchTime = 90


# #### Trivia data

# In[ ]:


#%%time
t_data = pd.read_pickle("C:/Users/Fredi/kodningsprojekt/wikipedia-categories/workproduct-files/t_dataMaster-keywordsIdentified.pkl")


# ### Functions

# #### Search term to wikipedia article name linking

# In[ ]:


#Returns wikipedia article formatted for database search, if not found, returns FALSE
def inArticles(a):
    match = articles.loc[articles["titleLower"] == a.lower(), :]
    if len(match) > 0:
        return match.iloc[0, 1].replace(" ", "_")
    else:
        return False


# In[ ]:


#Returns wikipedia article formatted for database search, if not found, returns FALSE
def inRedirects(a):
    match = redirects.loc[redirects["titleLower"] == a.lower(), :]
    if len(match) > 0:
        return match.iloc[0, 2].replace(" ", "_")
    else:
        return False


# In[ ]:


#Get first link from article based on title (DB formatting). Return False if no links exist
def getFirstLink(a):
    #match will be a pandas series of len=1
    match = articles.loc[articles["title"] == a.replace("_", " "), "links"]
    
    if len(match) > 0:
        #Change first series value into list
        asList = ast.literal_eval(match.iloc[0]) 
        #result = asList[0].replace(" ", "_")
        result = asList[0]
        
        #Take string only until |
        result = re.sub("(\|)(.+)", '', result)
        result = re.sub("(\|)", '', result)
        
        return result
    else:
        return False


# #### Wikipedia article name to neo4j database calls

# In[ ]:


# Call database for category tree and parents of given wikipedia title
@stopit.threading_timeoutable(default='Database call timed out (' + str(maxSearchTime) + ' seconds)')
def getCategoryInfo(a):
    # kill function if runs to long (>2min ?)
        # https://stackoverflow.com/questions/14920384/stop-code-after-time-period
        # https://pypi.org/project/stopit/#id14
    
    #result [wikipediID, path to MTC, parents]
    result = []
    
    try:
        articleID = articleByTitle(a).iloc[0,1]
        parents = parentCategories(articleID)
        
        if "Disambiguation_pages" in parents["pages.title"].values:
            firstLink = getFirstLink(a)
            return getWikipediaInfo(firstLink)
        else:
            path = chosenPathArticleToMTC(articleID)
        
        return [a, articleID, path, parents]
    
    except (IndexError, ValueError, TypeError, ClientError, AttributeError):
        return "Database call not successful (error)"
        
        


# In[ ]:


# Performs search functions from given search term --> Output from wikipedia database
def getWikipediaInfo(a):
    
    term = a.lower()
    
    out = inArticles(term)
    if out != False:
        return getCategoryInfo(out, timeout = maxSearchTime)
        #return getCategoryInfo(out)
    
    out = inRedirects(term)
    if out != False:
        return getCategoryInfo(out, timeout = maxSearchTime)
        #return getCategoryInfo(out)
    
    return "Search term not found"
        
    
    # if search term is in articles
        # Perform database search
        # Return (WikipediaID, Category tree, Parent categories)
    # else if search term is in redirects
        # Perform database search
        # Return (WikipediaID, Category tree, Parent categories)
    # else return FALSE


# #### neo4j database calls

# ##### Return node info based on wikipedia id

# In[ ]:


def nodeInfo(a):
    commandToRun = 'MATCH (pages:Page {id: %s})                 RETURN pages' % (a)
    return graph.run(commandToRun).data()


# ##### Return similarity statistics for two sets (intersection, union, Jaccard coefficient)

# In[ ]:


# compute similarity statistics
def similarityStats(a,b):
    intSize = len(a.intersection(b))
    unionSize = len(a.union(b))
    
    if unionSize == 0:
        jaccard = 0
    else:
        jaccard = intSize / unionSize
    
    return (intSize, unionSize, jaccard)


# ##### Return identifying information of parent categories of chosen article or category as pandas dataframe

# In[ ]:


# Give wikipedia id as integer as function argument (e.g. cateogry "Finland" = 693995)
def parentCategories(a):
    wikiID = a
    commandToRun = 'MATCH (pages:Category:Page)                 <-[:BELONGS_TO]-                 (:Page {id: %s})                 RETURN pages.title, pages.id' % (wikiID)

    # ensure that a dataframe with correct columns is returnd also when query is empty
    out = pd.DataFrame(columns = ["pages.title", "pages.id"])
    out = out.append(graph.run(commandToRun).to_data_frame())
    
    return out


# ##### Return identifying information of children (both category and article) of chosen category as pandas dataframe

# In[ ]:


# Give wikipedia id as integer as function argument (e.g. cateogry "Finland" = 693995)
def childPages(a):
    wikiID = a
    commandToRun = 'MATCH (pages:Page)                 -[:BELONGS_TO]->                 (:Page {id: %s})                 RETURN pages.title, pages.id' % (wikiID)

    # ensure that a dataframe with correct columns is returnd also when query is empty
    out = pd.DataFrame(columns = ["pages.title", "pages.id"])
    out = out.append(graph.run(commandToRun).to_data_frame())
    
    return out


# ##### Return dataframe with all articles (but not categories) with given title [only one result is expected]

# In[ ]:


# Give wikipedia title as string as function argument
# Will not work if article title contains "-character --> "ClientError". Escape fixes do not work, not worth debugging.
def articleByTitle(a):
    ArticleToFind = a
    commandToRun = 'MATCH (articles:Page {title: "%s"})                     WHERE NONE(art IN [articles] WHERE art:Category)                     RETURN articles.title, articles.id, ID(articles)' % (ArticleToFind)
    return graph.run(commandToRun).to_data_frame()


# ##### Return dataframe with all categories (but not articles) with given title [only one result is expected]

# In[ ]:


# Give wikipedia title as string as function argument
def categoryByTitle(a):
    CategoryToFind = a
    commandToRun = 'MATCH (categories:Category:Page {title: "%s"})                     RETURN categories.title, categories.id, ID(categories)' % (CategoryToFind)
    return graph.run(commandToRun).to_data_frame()


# ##### Return dataframe containing shortest path between input node (article or category) and Main_topics_classification category

# In[ ]:


def shortestPathToMTC(a):
    # (:Page {id: 7345184}) is Main_topics_classifications category node
    inputNode = a

    commandToRun = 'MATCH path=shortestPath(                     (:Page {id: %s})-[:BELONGS_TO*0..10]->(:Page {id: 7345184}))                     UNWIND nodes(path) AS pages                     RETURN pages.title, pages.id, ID(pages)' % (inputNode)

    return pd.DataFrame(graph.run(commandToRun).data())


# ##### Return similarity value between two categories as defined by Biuk-Aghai & Cheang (2011)

# In[ ]:


# Take as input tuple containing depth of category to compare to as well as intersection of children between two categories

'''
Given parent category p and child category c,
and given a root category node r, we calculate the category similarity
Sp;c as: Sp;c = Dc - Cp;c / k , where Dc is the depth of category
c in the category graph, i.e. the shortest distance from the root
category node r; Cp;c is the number of co-assigned articles of categories
p and c; and k is a constant that is empirically determined.
Through experimentation we have found that a value of k = 2 produces
the best results, i.e. results that agree with human intuition as
to similarity of a given pair of categories. A smaller value of Sp;c
indicates a greater similarity (i.e. a smaller distance between the
nodes). The number of co-assigned articles Cp;c of parent category
p and child category c is simply the cardinality of the intersection
of their assigned article sets: Cp;c = jAp \ Acj, where Ap and Ac
are the sets of articles assigned to categories p and c, respectively.
'''
# Depth is calcualted for parent when going bottom-up in graph
# C is calculated using intersection of both child articles and sub-categories

def similarityBAC(a):
    d = a[0]
    c = a[1]
    k = 2
    return d - (c/k)


# ##### Return dataframe containing all parent categories of category a and similarity statistics to each as well as parent depth to MTC

# In[ ]:


# Give wikipedia id as integer as function argument (e.g. cateogry "Finland" = 693995)
def parentSimilarities(a):
    parents = parentCategories(a)
    children = childPages(a)
    
    if len(parents) == 0:
        raise ValueError("Category processed does not have parents (likely input category to chosenPathUpToMTC() if called)")
    
    # Create columns with similarity stats using functions similarityStats
    parents["similarities"] = parents["pages.id"].apply(lambda x: similarityStats(set(children["pages.id"]), set(childPages(x)["pages.id"])))
    parents[["intersection", "union", "jaccard"]] = pd.DataFrame(parents['similarities'].tolist(), index = parents.index)
    parents.drop(["similarities"], axis = 1, inplace = True)
        
    # Add column with parent category depth (steps to Main_topics_classifications node)
    parents["depth"] = parents["pages.id"].apply(lambda x: len(shortestPathToMTC(x))-1)
    
    # Add column with similarityBAC
    parents["similarityBAC-aid"] = list(zip(parents["depth"], parents["intersection"]))
    parents["similarityBAC"] = parents["similarityBAC-aid"].apply(lambda x: similarityBAC(x))
    parents.drop(["similarityBAC-aid"], axis = 1, inplace = True)
    
    # Sort ascending
    parents.sort_values(by = "similarityBAC", ascending = True, inplace = True)
    parents.reset_index(drop = True, inplace = True)
    
    return parents


# ##### Return node based on neo4j database ID [NOTE: not same as wikipedia ID used elsewhere]

# In[ ]:


def getWithNeoID(a):
    return NodeMatcher(graph).get(a)


# ##### Return dataframe containing info of what category to choose from parentSimilarities() output

# In[ ]:


'''
we choose which parent link to keep according to
following rules: (1) Choose the parent whose similarity value Sp;c
is lower; (2) If Sp1;c = Sp2;c, choose the parent whose depth D is
lower; (3) If Dp1 = Dp2, choose the parent with the larger value
of Cp;c; (4) If Cp1;c = Cp2;c, choose the parent with the lower
page ID.
'''
# Takes parentSimilarities() / or potentially child similarities output dataframe as input
def chooseCategoryPath(a):
    a.sort_values(by = ["similarityBAC", "depth"], ascending = True, inplace = True)
    a["mostSimilar"] = "False"
    a["comment"] = ""
    
    # Set value for mostSimilar to "Not connected" for rows with depth = -1 i.e. no connection to MTC
    a.loc[a["depth"] == -1, "comment"] = "Not connected"
    
    # Set value for mostSimilar to "True" for rows that are not "Not connected" and that have the minimum value of similarityBAC
    workingDF = a.loc[a["comment"] != "Not connected"]
    selectedIndexes = workingDF.loc[workingDF["similarityBAC"] == workingDF["similarityBAC"].min()].index
    
    a.loc[selectedIndexes, "mostSimilar"] = "True"
    a.loc[selectedIndexes, "comment"] = "Lowest similarityBAC"
    
    workingDF = a.loc[a["mostSimilar"] == "True"]
    
    if len(workingDF) > 1:
        # Set all mostSimilar of partial dataframe and output back to False, then set min depth rows to true
        workingDF["mostSimilar"] = "False"
        a["mostSimilar"] = "False"
        selectedIndexes = workingDF.loc[workingDF["depth"] == workingDF["depth"].min()].index
        
        a.loc[selectedIndexes, "mostSimilar"] = "True"
        a.loc[selectedIndexes, "comment"] = a.loc[selectedIndexes, "comment"] + "; Lowest depth"
        
        workingDF = a.loc[a["mostSimilar"] == "True"]
        
        # If several rows now set to true, test for highest intersection
        if len(workingDF) > 1:
            # Set all mostSimilar of partial dataframe and output back to False, then set max intersection rows to true
            workingDF["mostSimilar"] = "False"
            a["mostSimilar"] = "False"
            selectedIndexes = workingDF.loc[workingDF["intersection"] == workingDF["intersection"].max()].index
            
            a.loc[selectedIndexes, "mostSimilar"] = "True"
            a.loc[selectedIndexes, "comment"] = a.loc[selectedIndexes, "comment"] + "; Highest intersection"
            
            workingDF = a.loc[a["mostSimilar"] == "True"]
            
            # If several rows now set to true, choose row with lowes pages.id
            if len(workingDF) > 1:
                # Set all mostSimilar of partial dataframe and output back to False, then set min wikipedia id row (only one) to true
                workingDF["mostSimilar"] = "False"
                a["mostSimilar"] = "False"
                selectedIndexes = workingDF.loc[workingDF["pages.id"] == workingDF["pages.id"].min()].index
                
                a.loc[selectedIndexes, "mostSimilar"] = "True"
                a.loc[selectedIndexes, "comment"] = a.loc[selectedIndexes, "comment"] + "; Lowest wikipedia id"
    
    
    return a


# ##### Return dataframe containing info of chosen path to MTC (iterates chooseCategoryPath() upwards)

# In[ ]:


# Iterate chooseCategoryPath() from input category (wikipedia id as input) until MTC is reached. Return dataframe with chosen path rows
# Root node category "Main_topic_classifications" has pages.id = 7345184

# NOTE: Error if input category does not have parents
def chosenPathUpToMTC(a):
    mtcFound = False
    nextStep = a
    chosenPath = pd.DataFrame()
    
    while(not mtcFound):
        allParents = parentSimilarities(nextStep)
        allParents = chooseCategoryPath(allParents)
        
        # If allParents contains MTC category
        if(len(allParents.loc[allParents["pages.id"] == 7345184]) == 1):
            rowToAppend = allParents.loc[allParents["pages.id"] == 7345184]
            mtcFound = True
        else:
            rowToAppend = allParents.loc[allParents["mostSimilar"] == "True"]
            nextStep = int(allParents.loc[allParents["mostSimilar"] == "True", "pages.id"])
        
        chosenPath = chosenPath.append(rowToAppend)
        chosenPath.reset_index(drop = True, inplace = True)
    
    
    return chosenPath


# ##### Article strength calculations

# In[ ]:


# Return dataframe with all pages linking to or from input page
def linksBetween(a):
    wikiID = a
    commandToRun = 'MATCH (pages:Page)                 -[:LINKS_TO]-                 (:Page {id: %s})                 RETURN pages.title, pages.id' % (wikiID)

    # ensure that a dataframe with correct columns is returnd also when query is empty
    out = pd.DataFrame(columns = ["pages.title", "pages.id"])
    out = out.append(graph.run(commandToRun).to_data_frame())
    
    return out


# In[ ]:


# a as pages.id for artice, c as pages.id for parent category
def articleClassificationStrength(a, c):
    aLinks = set(linksBetween(a)["pages.id"])
    cChildren = set(childPages(c)["pages.id"])
    
    intersectionSize = len(aLinks.intersection(cChildren))
    
    return 1 + intersectionSize


# In[ ]:


def strongestArticleParents(a):
    parents = parentCategories(a)
    parents["depth"] = parents["pages.id"].apply(lambda x: len(shortestPathToMTC(x)) -1 )
    parents.loc[parents["depth"] != -1 , "Strength"] = parents["pages.id"].apply(lambda x: articleClassificationStrength(a, x))
    parents.sort_values(by = ["Strength"], ascending = False, inplace = True)
   
    
    return parents


# In[ ]:


def chosenPathArticleToMTC(a):
    strongestParent = strongestArticleParents(a)
    path = chosenPathUpToMTC(strongestParent.iloc[0,1])
    
    path.loc[-1] = strongestParent.iloc[0, :3]
    path.sort_index(inplace = True)
    path.reset_index(drop = True, inplace = True)
    
    return path


# #### Run "question to wikipedia category" analyses on list of search terms

# In[ ]:


# Take list with search terms
# Run getWikipediaInfo() until search term works
# If result is False (search term not found) or "Database call not successful (error)" (search term found but path to MTC not available)
    # --> continue to next search term
# Return [categoriesFound (boolean), [n x Result], [1 x successful output]]

#@stopit.threading_timeoutable(default='Find question categories jammed:' + str(maxSearchTime * 4) + ' seconds)')

# Behaviour of stopit is erratic. Nesting stopits (here and in getCategoryInfo) leaads to unexpected results. Does however not seem to make loop fail if output formatted correctly.
# --> Ensure jamming error is distinct, repeat processes for jammed indexes later.

@stopit.threading_timeoutable(default='findQuestionCategories() jammed')
def findQuestionCategories(a):
    categoriesFound = False
    result = []
    getWikipediaInfo_out = [None, None, None, None]
    toReturn = []
    possibleFailureMessages = ('Search term not found', 'Database call timed out (' + str(maxSearchTime) + ' seconds)', 'Database call not successful (error)')
    
    for term in a:
        termResult = getWikipediaInfo(term)        
        
        if termResult not in possibleFailureMessages:
            categoriesFound = True
            # CHANGE: NOT NECESSARY TO SAVE TERM
            result.append( ("SUCCESS") )
            getWikipediaInfo_out = termResult
            break
        else:
            result.append( (termResult) )
    
    if len(result) == 0:
        result.append( ("NO SEARCH TERMS GIVEN") )
    
    # Insert categorieFound  at start of toReturn
    toReturn.append(categoriesFound)
    toReturn.append(result)
    toReturn.append(getWikipediaInfo_out)
    
    return toReturn
       
        


# ### Run "question to wikipedia category" analyses in batches

# #### Run code on given data and clean

# In[ ]:


# Seems that running batches > 1 leads to jamming not caught by maxSearchTime for stopit in getCategoryInfo
# This is probably related to .apply not working together with neo4j database calls
# --> Easiest to resolv by running only batches of n = 1 (does not seem to give significant)
batchSize = 5


# In[ ]:


dfToIterate = t_data


# In[ ]:


batchRuns = pd.read_csv("C:/Users/Fredi/kodningsprojekt/wikipedia-categories/workproduct-files/batchRuns.csv", delimiter=";")


# In[ ]:


startIndex = batchRuns.iloc[-1,1] + 1
stopIndex = startIndex + batchSize - 1
batchNr = len(batchRuns)


# In[ ]:


#%%time
while startIndex <= len(dfToIterate) - 1:
    
    if (len(dfToIterate) - startIndex) < batchSize:
        stopIndex = len(dfToIterate) - 1
    
    dfToProcess = pd.DataFrame(dfToIterate.loc[startIndex : stopIndex , "searchTerms"])
    
    startTime = time.gmtime()
    
    # Run findQuestionCategories() and clean result
    #dfToProcess["findQuestionCategories_Out"] = dfToProcess["searchTerms"].apply(lambda x: findQuestionCategories(x, timeout = 1))
    dfToProcess["findQuestionCategories_Out"] = dfToProcess["searchTerms"].apply(lambda x: findQuestionCategories(x))
    
    # ADD:
    # For loop that iterates over rows in dfToProcess
    # If values is "findQuestionCategories() jammed"
        # Then run findQuestionCategories again with timeout = MaxSearchTime
    # If value is still "Find question categories jammed"
        # Then change result to correctly formated list
            # [categoriesFound (boolean), [n x Result], [1 x successful output]]
            # --> [False, ["findQuestionCategories() jammed"], [None, None, None, None]]
        
    for row in dfToProcess.index:
        if dfToProcess.loc[row, "findQuestionCategories_Out"] == 'findQuestionCategories() jammed':
            #secondTry = findQuestionCategories(dfToProcess.loc[row, "searchTerms"], timeout = 10)
            secondTry = findQuestionCategories(dfToProcess.loc[row, "searchTerms"])
            
            if secondTry == 'findQuestionCategories() jammed':
                dfToProcess.loc[row, "findQuestionCategories_Out"] = [False, ["findQuestionCategories() jammed"], [None, None, None, None]]
                
            else:
                dfToProcess.loc[row, "findQuestionCategories_Out"] = secondTry
       
    
    
    dfToProcess[['wikipediaSearchSuccessful','findQuestionCategories_meta', 'findQuestionCategories_result']] = pd.DataFrame(dfToProcess["findQuestionCategories_Out"].tolist(), index= dfToProcess.index)
    dfToProcess.drop(columns = ["findQuestionCategories_Out"], inplace = True)
    dfToProcess[["wikipediaArticleTitle", "wikipediaArticleID", "categoryPath", "parentCategories"]] = pd.DataFrame(dfToProcess["findQuestionCategories_result"].tolist(), index= dfToProcess.index)
    dfToProcess.drop(columns = ["findQuestionCategories_result"], inplace = True)
    
    # Save result to pickle in dedicated folder
    dfToProcess.to_pickle("C:/Users/Fredi/kodningsprojekt/wikipedia-categories/workproduct-files/batchRuns/batch" + str(batchNr) + "_" + str(startIndex) + "-" + str(stopIndex) +  ".pkl")
    
    # Update metadata to csv
    endTime = time.gmtime()
    runTime = time.mktime(endTime)-time.mktime(startTime)
    startTime = time.strftime("%Y-%m-%d %H:%M:%S", startTime)
    endTime = time.strftime("%Y-%m-%d %H:%M:%S", endTime)
    newRow = [startIndex, stopIndex, startTime, endTime, runTime]
    batchRuns = batchRuns.append(pd.Series(newRow, index = batchRuns.columns), ignore_index = True)
    batchRuns.to_csv("C:/Users/Fredi/kodningsprojekt/wikipedia-categories/workproduct-files/batchRuns.csv", sep=";", index = False)
    
    # Read new parameters from csv for next loop
    batchRuns = pd.read_csv("C:/Users/Fredi/kodningsprojekt/wikipedia-categories/workproduct-files/batchRuns.csv", delimiter=";")
    startIndex = batchRuns.iloc[-1,1] + 1
    stopIndex = startIndex + batchSize - 1
    batchNr = len(batchRuns)
    
    if (len(dfToIterate) - startIndex) < batchSize:
        stopIndex = len(dfToIterate) - 1
    
    print("LOOP!")


# In[ ]:


print("ALL DONE!")

