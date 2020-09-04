# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class ReatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_sortecd_output)
            ]
    def mapper_get_ratings(self, _,line):
        (userID, movieID,rating,timestamp)=line.split('\t')
        yield movieID,1
    def reducer_count_ratings(self, key, values):
        yield key, str(sum(values)).zfill(5)
    def reducer_sortecd_output(self, count, movies):
        for movie in movies:
            yield movie,count

if _name_=='_main_':
    RatingBreakDown.run()
        