import re
import csv
import sys
import datetime

'''
This file contains a number of functions that can be called as user-defined
functions in a Pig script, simplifiying much of the logic to do these tasks.  
Unfortunately, many of the functions here seem to execute very slow, and it
might be worth looking into alternative ways of processing HBase rows, such
as using Spark.
@author Mitch Wagner
'''

@outputSchema("text:chararray")
def getHashtags(text):
    '''
    Extracts the hashtags from the tweet text passed to the function, and
    concatenates all of them with semi-colons.
    '''
    if (text == None):
        return ""
    return ';'.join(str(elem) for elem in re.findall("(?:\s|\A^)[#]+([A-Za-z0-9-_]+)", text))

@outputSchema("text:chararray")
def getMentions(text):
    '''
    Extracts the mentions from the tweet text passed to the function, and
    concatenates all of them with semi-colons.
    '''
    if (text == None):
        return ""
    return ';'.join(str(elem) for elem in re.findall("(?:\s|\A^)[@]+([A-Za-z0-9-_]+)", text))

@outputSchema("text:chararray")
def getRetweetStatus(text):
    '''
    Determines whether or not the tweet whose text was passed to this function
    is a retweet, using a regex to look for the telltale presence of the term
    "RT" that indicates this.
    '''
    if (text == None):
        return ""
    l = re.findall("(?:^|\s)(RT)[@]?", text, flags=re.IGNORECASE)
    if len(l) > 0:
        return "1"
    else:
        return "0"

@outputSchema("month:chararray")
def getCreatedMonth(unixtime):
    '''
    Takes in a Unix timestamp, and determines the month that said timestamp
    refers to, returning that value.
    '''
    if (unixtime == None):
        return ""
    return datetime.datetime.fromtimestamp(int(unixtime)).strftime("%m")

@outputSchema("year:chararray")
def getCreatedYear(unixtime):
    '''
    Takes in a Unix timestamp, and determines the year that said timestamp
    refers to, returning that value. 
    '''
    if (unixtime == None):
        return ""
    return datetime.datetime.fromtimestamp(int(unixtime)).strftime("%Y")

@outputSchema("text:chararray")
def removeAtSymbols(text):
    '''
    Removes @ symbols from the text passed to the function, and returns the
    augmented text.
    '''
    if (text == None):
        return ""
    return text.replace("@", "")

@outputSchema("text:chararray")
def removeOctothorpes(text):
    ''' 
    Removes octothoropes from the text passed to the function, and returns the
    augmented text. 
    '''
    if (text == None):
        return ""
    return text.replace("#", "")

@outputSchema("text:chararray")
def removeBadWords(text):
    '''
    Removes a number of bad words from the text passed to the function,
    returning the augmented text There are likely several words that could be 
    added to this list. When adding new words, one must be mindful that
    those words are not subwords of words that are actually appropriate.
    For example, the word 'ass' is actually a subtoken of 'morass', which
    is a perfectly acceptable word, and we are not taking that into 
    account here.
    '''
    if (text == None):
        return ""
    text = text.replace("damn", "d**m")
    text = text.replace("bitch", "b***h")
    text = text.replace("crap", "c**p")
    text = text.replace("piss", "p**s")
    text = text.replace("dick", "d**k")
    text = text.replace("cunt", "c**t")
    text = text.replace("slut", "sl*t")
    text = text.replace("shit", "sh*t")
    text = text.replace("pussy", "p***y")
    text = text.replace("fuck", "f**k")
    text = text.replace("ass", "a**")
    return text
 
@outputSchema("text:chararray")
def removeStopWords(text):
    '''
    Removes a number of English stopwords from the text, as given by the
    following resource: http://xpo6.com/list-of-english-stop-words/ 

    Unlike with the bad words above, we went to the trouble of ensuring that
    only words that exactly match these tokens are replaced, given that
    many of these words are components of larger, more important words.
    Unfortunately, this is a very slow process, likely due to several
    regex replacements, and would be a key target for optimizations.
    '''
    if (text == None):
        return ""
    stopwords = ["a", "about", "above", "above", "across", "after",
    "afterwards", "again", "against", "all", "almost", "alone", "along", "already",
    "also","although","always","am","among", "amongst", "amoungst", "amount",
    "an", "and", "another", "any","anyhow","anyone","anything","anyway",
    "anywhere", "are", "around", "as",  "at", "back","be","became",
    "because","become","becomes", "becoming", "been", "before", "beforehand",
    "behind", "being", "below", "beside", "besides", "between", "beyond", "bill",
    "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con",
    "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down",
    "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere",
    "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything",
    "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire",
    "first", "five", "for", "former", "formerly", "forty", "found", "four", "from",
    "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have",
    "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon",
    "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie",
    "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself",
    "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many",
    "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most",
    "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither",
    "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone",
    "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once",
    "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours",
    "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put",
    "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious",
    "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty",
    "so", "some", "somehow", "someone", "something", "sometime", "sometimes",
    "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the",
    "their", "them", "themselves", "then", "thence", "there", "thereafter",
    "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv",
    "thin", "third", "this", "those", "though", "three", "through", "throughout",
    "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve",
    "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via",
    "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever",
    "where", "whereafter", "whereas", "whereby", "wherein", "whereupon",
    "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole",
    "whom", "whose", "why", "will", "with", "within", "without", "would", "yet",
    "you", "your", "yours", "yourself", "yourselves", "the"]

    for word in stopwords:
        rgx = re.compile(r"\b" + word + r"\b")
        text = rgx.sub("", text)
        
    return text    


 
