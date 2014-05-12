#!/usr/bin/env python
import random
import pickle
import ast
featuresets=[]
'''overriding show_most_informative_features to return the words and ratio'''
count1 = 0
def show_most_informative_features(self, n=10):
    strlist = []
    cpdist = self._feature_probdist
    for (fname, fval) in self.most_informative_features(n):
        def labelprob(l):
            return cpdist[l,fname].prob(fval)
        labels = sorted([l for l in self._labels
                         if fval in cpdist[l,fname].samples()],
                        key=labelprob)
        if len(labels) == 1: continue
        l0 = labels[0]
        l1 = labels[-1]
        if cpdist[l0,fname].prob(fval) == 0:
            ratio = 'INF'
        else:
            ratio = '%8.1f' % (cpdist[l1,fname].prob(fval) /
                               cpdist[l0,fname].prob(fval))
        strlist.append((fname,ratio,l1))
    return strlist

for line in sys.stdin:
    line = line.strip()
    features, count = line.split('\t', 1)
    features =ast.literal_eval(features)
    featuresets.append(features)
random.shuffle(featuresets)
classifier = nltk.NaiveBayesClassifier.train(featuresets)
feature_word_list=show_most_informative_features(classifier,200)
for (key,val,l1) in feature_word_list:
    print '%s\t%s' % (key,val)