from bs4 import BeautifulSoup
from bs4.element import Comment
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import RegexpTokenizer
from nltk import FreqDist
import string

## tokenizes current page
## uses porter stemmer to reduce base words
## preprocessing steps

def parsePage(page):
    with open(page, encoding = 'latin1') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
        return soup


# https://stackoverflow.com/questions/1936466/beautifulsoup-grab-visible-webpage-text
def filterHTML(page):
    if page.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(page, Comment):
        return False
    return True  


def stemList(word):
    ps = PorterStemmer()
    return ps.stem(word)

        
def tokenizer(page):
##    sandyStopWordsPathGen(nltk.data.path) # Can remove
    soup = parsePage(page)
    raw_string = soup.find_all(text = True)
    filtered_string = filter(filterHTML, raw_string)
    body_string = ' '.join(x.strip() for x in filtered_string)
    stop = stopwords.words('english') + list(string.punctuation)
    toker = RegexpTokenizer(r'\w{3,}')
    tokens = [stemList(w) for w in toker.tokenize(body_string) if w not in stop]
    #.lower()
    return dict(FreqDist(tokens))



    
 
