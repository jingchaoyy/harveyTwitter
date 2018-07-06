"""
Created on 7/5/2018
@author: Jingchao Yang
"""
from dataPreprocessing import gazetteer_from_fuzzyMatch
from toolBox import fuzzy_gazatteer

self_roadScores = fuzzy_gazatteer.fuzzyLocMatch(gazetteer_from_fuzzyMatch.roads_from_tw,
                                                gazetteer_from_fuzzyMatch.roads_from_url)
self_placeScores = fuzzy_gazatteer.fuzzyLocMatch(gazetteer_from_fuzzyMatch.places_from_tw,
                                                 gazetteer_from_fuzzyMatch.places_from_url)

