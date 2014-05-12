#!/usr/bin/env python
import sys
import nltk
import pickle
def word_feats(words):
    return dict([(word, 'True') for word in words])
for line in sys.stdin:
    words = nltk.word_tokenize(line)
    if words[0] is 'neg':
        del words[0]
        set = (words,'neg')
    else:
        del words[0]
        set = (words,'pos')
    features =  (word_feats(set[0]),set[1])
    print '%s\t%s' % (features, 1)