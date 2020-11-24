import sys
import pandas as pd
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk.stem import wordnet
from nltk.tokenize import word_tokenize

lemma = wordnet.WordNetLemmatizer()


def lemmatizeWord(text):
    wordTokens = word_tokenize(text)
    lemmas = [lemma.lemmatize(word) for word in wordTokens]
    return ' '.join(lemmas)

try:
    sid = SentimentIntensityAnalyzer()

    inputData = sys.stdin
    inputDataFrame = pd.read_csv(inputData, header=None)
    inputDataFrame.rename(columns={0: "Reviews"}, inplace=True)
    # Cleanising the Volume and Open Columns
    inputDataFrame["Reviews"] = inputDataFrame["Reviews"].apply(lambda x: x.replace("]", ""))
    inputDataFrame["Reviews"] = inputDataFrame["Reviews"].apply(lemmatizeWord)
    inputDataFrame['Scores'] = inputDataFrame['Reviews'].apply(lambda review: sid.polarity_scores(review))
    inputDataFrame['Compound'] = inputDataFrame['Scores'].apply(lambda score_dict: score_dict['compound'])
    inputDataFrame['Polarity'] = inputDataFrame['Compound'].apply(
        lambda compoundScore: "Positive" if compoundScore >= 0 else "Negative")
    outputDataFrame = inputDataFrame['Polarity'].tolist()
    for values in outputDataFrame:
        print(values)

except Exception as ex:
    print(ex.with_traceback(), "Unexpected Error Occured",ex.__class__)
