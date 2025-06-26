import os
import sys
from datetime import datetime, timedelta
from pymongo import MongoClient
from wordcloud import WordCloud
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
import spacy
from spacy.cli import download
from sklearn.feature_extraction.text import TfidfVectorizer
from io import BytesIO
import base64
import pandas as pd
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from dotenv import load_dotenv
load_dotenv()

try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

lemmatizer = WordNetLemmatizer()

# Database Setups - Updated to use environment variables
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB", "brand_monitoring")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "processed_articles")
WORDCLOUD_COLLECTION = "wordclouds"

# Email Configuration - Updated to use environment variables
EMAIL_CONFIG = {
    "EMAIL_SENDER": os.getenv("EMAIL_SENDER"),
    "EMAIL_PASSWORD": os.getenv("EMAIL_PASSWORD"),
    "EMAIL_RECEIVER": os.getenv("EMAIL_RECEIVER"),
    "SMTP_SERVER": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
    "SMTP_PORT": int(os.getenv("SMTP_PORT", 587)),
}

# Stopwords and NLTK data
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')

try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt')

try:
    nltk.data.find('corpora/wordnet')
except LookupError:
    nltk.download('wordnet')

def load_custom_stopwords():
    stop_words = set(stopwords.words('english'))  # Load English stop words
    custom_stop_words = set([

    # General irrelevant terms
    "thing", "things", "stuff", "something", "anything", "everything", "nothing",
    "someone", "anyone", "everyone", "nobody", "it", "this", "that", "these",
    "those", "there", "here", "need", "make", "want", "now", "people",
    "really", "very", "quite", "probably", "maybe", "almost", "just", "only",
    "even", "still", "yet", "too", "more", "less", "much", "many", "some",
    "few", "any", "all", "lot", "lots", "plenty", "can", "will", "good", "bad",
    "better", "best", "same", "different", "great", "poor", "high", "low",
    "think", "love", "know", "time", "say", "go", "pay", "buy", "sell", "see",
    "live", "year", "money", "now", "get", "use", "look", "sure", "take",
    "happen", "work", "lot", "area", "state", "country", "city", "number",

    # Numbers
    "one", "two", "three", "four", "five", "ten", "hundred", "thousand",
    "million", "billion",

    # Words from the Word Cloud irrelevant to condos
    "need", "make", "want", "like", "great", "nice", "thing", "things",
    "some", "many", "now", "people", "will", "home", "house", "time",
    "know", "even", "still", "just", "look", "year", "go", "back", "right",
    "think", "use", "take", "thank", "help", "money", "price", "buy",
    "canada", "toronto",

    # Conditionally irrelevant, depending on focus
    "awesome", "informative", "best", "worse", "good", "bad", "yet"
    # General filler and vague terms
    "thing", "things", "stuff", "something", "anything", "everything", "nothing",
    "someone", "anyone", "everyone", "nobody", "people", "others", "it", "this",
    "that", "these", "those", "there", "here",

    # Pronouns
    "i", "me", "you", "he", "she", "we", "they", "them", "us", "my", "your",
    "his", "her", "our", "their", "mine", "yours", "ours", "theirs",

    # Verbs (generic, non-specific)
    "is", "was", "are", "were", "be", "been", "being", "am", "do", "did",
    "does", "have", "had", "has", "make", "makes", "made", "go", "goes",
    "went", "gone", "get", "gets", "got", "put", "puts", "take", "takes",
    "took", "taken", "let", "lets", "say", "says", "said", "see", "sees",
    "saw", "seen", "come", "comes", "came", "do", "done", "use", "used",
    "using", "give", "gives", "gave", "given", "want", "wants", "wanted",
    "allow", "allows", "allowed", "happen", "happens", "happened",

    # Adverbs and vague qualifiers
    "really", "very", "quite", "probably", "possibly", "maybe", "almost",
    "often", "sometimes", "always", "never", "ever", "just", "only", "even",
    "already", "still", "yet", "too", "enough", "more", "less", "much",
    "many", "some", "few", "any", "all", "lot", "lots", "plenty",

    # Adjectives
    "new", "old", "big", "small", "large", "little", "good", "bad", "better",
    "best", "worse", "worst", "same", "different", "great", "poor", "high",
    "low", "hard", "easy", "fast", "slow", "early", "late", "long", "short",

    # Numbers
    "one", "two", "three", "four", "five", "six", "seven", "eight", "nine",
    "ten", "hundred", "thousand", "million", "billion",

    # Time and date words
    "today", "yesterday", "tomorrow", "morning", "afternoon", "evening",
    "night", "week", "month", "year", "years", "hour", "hours", "minute",
    "minutes", "second", "seconds",

    # Prepositions and conjunctions
    "and", "or", "but", "so", "because", "if", "then", "than", "as", "like",
    "with", "without", "within", "between", "among", "about", "around",
    "above", "below", "over", "under", "before", "after", "through", "into",
    "onto", "out", "off", "on", "in", "at", "by", "for", "from", "of", "to",
    "up", "down", "over", "under",

    # Expressions and contractions
    "it's", "don't", "can't", "doesn't", "won't", "isn't", "aren't", "wasn't",
    "weren't", "hasn't", "haven't", "hadn't", "shouldn't", "wouldn't",
    "couldn't", "we're", "they're", "i'm", "you're", "he's", "she's", "it's",
    "that's", "what's", "there's", "here's", "who's", "how's", "let's",

    # Miscellaneous generic terms
    "home", "house", "place", "location", "area", "state", "country", "city",
    "neighborhood", "thing", "way", "case", "part", "point", "type", "form",
    "number", "kind", "bit", "lot", "side", "end", "level", "line", "group",
    "question", "problem", "solution", "issue", "cost", "price", "value",
    "money", "job", "work", "service", "item", "stuff", "way", "matter",
    "example", "fact", "reason", "idea",


    'house', 'people', 'apartment', 'building', 'live', 'like', 'the', 'to',
    'and', 'in', 'is', 'you', 'of', 'for', 'it', 'this',
    'that', 'they', 'are', 'have', 'not', 'be', 'my', 'on', 'likes', 'with',
    'i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your',
    'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she',
    'her', 'hers', 'herself', 'it', 'its', 'itself', 'they', 'them', 'their',
    'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that',
    'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
    'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an',
    'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of',
    'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through',
    'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down',
    'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then',
    'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both',
    'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not',
    'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 'can', 'will',
    'just', 'don', 'should', 'now', 'd', 'll', 'm', 'o', 're', 've', 'y', 'ain', "n't",
    'aren', 'couldn', 'didn', 'doesn', 'hadn', 'hasn', 'haven', 'isn', 'ma', 'mightn',
    'mustn', 'needn', 'shan', 'shouldn', 'wasn', 'weren', 'won', 'wouldn', 'get',
    'would', 'great', 'one', 'video', 'pay', 'home', 'housing', 'good', 'canada', 'years', 'thank',
    'never', 'time', 'need', 'much', 'buy', 'thanks', 'toronto', 'going', 'even', 'want', 'make',
    'go', 'know',"'ll", "'tis", "'twas", "'ve", "10", "39", "a", "a's", "able", "ableabout",
    "about", "above", "abroad", "abst", "accordance", "according", "accordingly",
    "across", "act", "actually", "ad", "added", "adj", "adopted", "ae", "af", "affected",
    "affecting", "affects", "after", "afterwards", "ag", "again", "against",
    "ago", "ah", "ahead", "ai", "ain't", "aint", "al", "all", "allow", "allows",
    "almost", "alone", "along", "alongside", "already", "also", "although", "always",
    "am", "amid", "amidst", "among", "amongst", "amoungst", "amount", "an", "and",
    "announce", "another", "any", "anybody", "anyhow", "anymore", "anyone", "anything",
    "anyway", "anyways", "anywhere", "ao", "apart", "apparently", "appear", "appreciate",
    "appropriate", "approximately", "aq", "ar", "are", "area", "areas", "aren",
    "aren't", "arent", "arise", "around", "arpa", "as", "aside", "ask", "asked", "asking",
    "asks", "associated", "at", "au", "auth", "available", "aw", "away",
    "awfully", "az", "b", "ba", "back", "backed", "backing", "backs", "backward", "backwards",
    "bb", "bd", "be", "became", "because", "become", "becomes", "becoming",
    "been", "before", "beforehand", "began", "begin", "beginning", "beginnings", "begins",
    "behind", "being", "beings", "believe", "below", "beside", "besides", "best",
    "better", "between", "beyond", "bf", "bg", "bh", "bi", "big", "bill", "billion", "biol",
    "bj", "bm", "bn", "bo", "both", "bottom", "br", "brief", "briefly", "bs",
    "bt", "but", "buy", "bv", "bw", "by", "bz", "c", "c'mon", "c's", "ca", "call", "came",
    "can", "can't", "cannot", "cant", "caption", "case", "cases", "cause", "causes",
    "cc", "cd", "certain", "certainly", "cf", "cg", "ch", "changes", "ci", "ck", "cl",
    "clear", "clearly", "click", "cm", "cmon", "cn", "co", "co.", "com", "come",
    "comes", "computer", "con", "concerning", "consequently", "consider", "considering",
    "contain", "containing", "contains", "copy", "corresponding", "could", "could've",
    "couldn", "couldn't", "couldnt", "course", "cr", "cry", "cs", "cu", "currently", "cv",
    "cx", "cy", "cz", "d", "dare", "daren't", "darent", "date", "de", "dear",
    "definitely", "describe", "described", "despite", "detail", "did", "didn", "didn't",
    "didnt", "differ", "different", "differently", "directly", "dj", "dk", "dm",
    "do", "does", "doesn", "doesn't", "doesnt", "doing", "don", "don't", "done", "dont",
    "doubtful", "down", "downed", "downing", "downs", "downwards", "due", "during",
    "dz", "e", "each", "early", "ec", "ed", "edu", "ee", "effect", "eg", "eh", "eight",
    "eighty", "either", "eleven", "else", "elsewhere", "empty", "end", "ended",
    "ending", "ends", "enough", "entirely", "er", "es", "especially", "et", "et-al", "etc",
    "even", "evenly", "ever", "evermore", "every", "everybody", "everyone",
    "everything", "everywhere", "ex", "exactly", "example", "except", "f", "face", "faces",
    "fact", "facts", "fairly", "far", "farther", "felt", "few", "fewer", "ff",
    "fi", "fifteen", "fifth", "fifty", "fify", "fill", "find", "finds", "fire", "first",
    "five", "fix", "fj", "fk", "fm", "fo", "followed", "following", "follows", "for",
    "forever", "former", "formerly", "forth", "forty", "forward", "found", "four", "fr",
    "free", "from", "front", "full", "fully", "further", "furthered", "furthering",
    "furthermore", "furthers", "fx", "g", "ga", "gave", "gb", "gd", "ge", "general",
    "generally", "get", "gets", "getting", "gf", "gg", "gh", "gi", "give", "given",
    "gives", "giving", "gl", "gm", "gmt", "gn", "go", "goes", "going", "gone", "good",
    "goods", "got", "gotten", "gov", "gp", "gq", "gr", "great", "greater", "greatest",
    "greetings", "group", "grouped", "grouping", "groups", "gs", "gt", "gu", "gw", "gy",
    "h", "had", "hadn't", "hadnt", "half", "happens", "hardly", "has", "hasn",
    "hasn't", "hasnt", "have", "haven", "haven't", "havent", "having", "he", "he'd",
    "he'll", "he's", "hed", "hell", "hello", "help", "hence", "her", "here", "here's",
    "hereafter", "hereby", "herein", "heres", "hereupon", "hers", "herself", "herse",
    "hes", "hi", "hid", "high", "higher", "highest", "him", "himself", "himse", "his",
    "hither", "hk", "hm", "hn", "home", "homepage", "hopefully", "how", "how'd", "how'll",
    "how's", "howbeit", "however", "hr", "ht", "htm", "html", "http", "hu",
    "hundred", "i", "i'd", "i'll", "i'm", "i've", "i.e.", "id", "ie", "if", "ignored", "ii",
    "il", "ill", "im", "immediate", "immediately", "importance", "important",
    "in", "inasmuch", "inc", "inc.", "indeed", "index", "indicate", "indicated", "indicates",
    "information", "inner", "inside", "insofar", "instead", "int", "interest",
    "interested", "interesting", "interests", "into", "invention", "inward", "io", "iq", "ir",
    "is", "isn", "isn't", "isnt", "it", "it'd", "it'll", "it's", "itd", "itll",
    "its", "itself", "itse", "ive", "j", "je", "jm", "jo", "join", "jp", "just", "k", "ke",
    "keep", "keeps", "kept", "keys", "kg", "kh", "ki", "kind", "km", "kn", "knew",
    "know", "known", "knows", "kp", "kr", "kw", "ky", "kz", "l", "la", "large", "largely",
    "last", "lately", "later", "latest", "latter", "latterly", "lb", "lc", "least",
    "length", "less", "lest", "let", "let's", "lets", "li", "like", "liked", "likely",
    "likewise", "limited", "line", "lineage", "link", "links", "list", "literally",
    "little", "ll", "long", "longer", "longest", "look", "looking", "looks", "loose", "low",
    "lower", "lowest", "ltd", "lulu", "ly", "m", "made", "mainly", "major",
    "majority", "make", "makes", "making", "many", "may", "maybe", "me", "mean", "meaning",
    "meantime", "meanwhile", "merely", "mg", "might", "might've", "mightn't",
    'yet', 'showed', 'today', 'called', 'nobody', 'soon', 'seems', 'seen', 'wish', 'run', '50', '15',
    'agree', 'understand', 'saying', 'happy', 'taking', 'talk', 'informative',
    'vancouver', 'reason', 'hard', 'current', 'side', 'times', 'days', 'show', 'ease', 'took', 'wont',
    'whats', 't', "mightnt", "million", "mine", "minus", "miss", "more", "moreover", "most",
    "mostly", "much", "must", "must've", "mustn't", "mustnt", "my", "myself", "myse", "myseft",
    'so', 'non', 'wonder', 'try', 'feel', 'sad', 'man', 'something', 'tell', 'stop', 'us', 'excellent',
    'way', 'info', 'try', '20', 'day', 'wants', 'coming', 'still', 'person', 'idea', 'went', 'use', 'stay',
    'hear', 'sorry', 'sense', '100', '30', 'life', 'nice', 'bad', 'next', 'months', 'right', 'real',
    'think', 'love', 'without', 'said', 'content', 'trying', 'take', 'nothing', 'since', 'things', 'undertand',
    'wow', 'say', 'sounds', 'sure', 'ontario', 'part', 'door', 'situation', 'lol', 'put', 'helpful', 'happen', 'put',
    'guy', 'well', 'two', 'lot', 'see', 'told', '000', 'per', 'worse', 'hope'])  # Add custom words if needed
    stop_words.update(custom_stop_words)
    return stop_words

def send_email(subject, body):
    try:
        EMAIL_SENDER = EMAIL_CONFIG["EMAIL_SENDER"]
        EMAIL_RECEIVER = EMAIL_CONFIG["EMAIL_RECEIVER"]
        EMAIL_PASSWORD = EMAIL_CONFIG["EMAIL_PASSWORD"]
        SMTP_SERVER = EMAIL_CONFIG["SMTP_SERVER"]
        SMTP_PORT = EMAIL_CONFIG["SMTP_PORT"]

        if not all([EMAIL_SENDER, EMAIL_RECEIVER, EMAIL_PASSWORD]):
            print("Missing email configuration. Email not sent.")
            return

        msg = MIMEMultipart()
        msg["From"] = EMAIL_SENDER
        msg["To"] = EMAIL_RECEIVER
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())

        print("Email notification sent.")
    except Exception as e:
        print(f"Failed to send email notification: {e}")

# Loading Data
def fetch_articles_for_today():
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][MONGO_COLLECTION]

    # Get today's date in string format to match database format
    today_str = datetime.utcnow().strftime('%Y-%m-%d')
    
    # Query for articles where scraped_date starts with today's date
    # This handles both string formats: "2025-06-26T22:43:03.898854" and "2025-06-26T00:00:00"
    query = {
        "scraped_date": {
            "$regex": f"^{today_str}"
        }
    }

    docs = list(collection.find(query))
    print(f"Found {len(docs)} articles scraped today ({today_str})")
    
    return pd.DataFrame(docs)

# WordCloud Generator
def generate_relevant_wordcloud_base64(df, stopwords=None):
    stopwords = stopwords or set()
    texts = df['content'].dropna().tolist()
    full_text = " ".join(texts).lower()

    # 1️Named Entities
    doc = nlp(full_text)
    entities = [ent.text.lower() for ent in doc.ents if ent.label_ in {"ORG", "PERSON", "GPE", "PRODUCT"}]

    # 2️TF-IDF Keywords
    tfidf = TfidfVectorizer(stop_words="english", max_features=100)
    tfidf_matrix = tfidf.fit_transform(texts)
    keywords = tfidf.get_feature_names_out().tolist()

    # 3️Combine + Lemmatize + Filter
    words = entities + keywords
    lemmatized = [lemmatizer.lemmatize(w) for w in words if w not in stopwords]
    text_blob = " ".join(lemmatized)

    # 4️WordCloud
    wc = WordCloud(width=800, height=400, background_color='white').generate(text_blob)

    buffer = BytesIO()
    wc.to_image().save(buffer, format="PNG")
    return f"data:image/png;base64,{base64.b64encode(buffer.getvalue()).decode('utf-8')}"

# Store in Mongo
def store_wordcloud_in_mongo(base64_uri, date_str):
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB][WORDCLOUD_COLLECTION]
    
    collection.update_one(
        {"date": date_str},
        {"$set": {
            "wordcloud_data_uri": base64_uri,
            "date": date_str,
            "created_at": datetime.utcnow()
        }},
        upsert=True
    )

# Main Runner
def run_wordcloud_pipeline():
    df = fetch_articles_for_today()
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    if df.empty:
        print(f"[{date_str}] No articles scraped today — skipping WordCloud.")
        subject = f"[WordCloud SKIPPED] No articles scraped on {date_str}"
        body = f"No articles were scraped today ({date_str}), so no WordCloud was generated."
        send_email(subject, body)
        return
    
    stopwords = load_custom_stopwords()
    img_b64 = generate_relevant_wordcloud_base64(df, stopwords)
    store_wordcloud_in_mongo(img_b64, date_str)

    subject = f"[WordCloud SUCCESS] {date_str}"
    body = f"WordCloud successfully generated and stored for {date_str}.\n\nArticles processed: {len(df)}"
    send_email(subject, body)

    print(f"[{date_str}] WordCloud generated + stored with {len(df)} articles.")

# Main Function
if __name__ == "__main__":
    try:
        print("Running GitHub-scheduled WordCloud pipeline...")
        run_wordcloud_pipeline()
    except Exception as e:
        print(f"WordCloud generation failed: {e}")
        sys.exit(1)
