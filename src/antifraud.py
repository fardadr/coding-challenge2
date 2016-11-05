# Written by Gene Der Su for Insight Coding Challenge.

import sys
import copy

class Graph:
    """
        This class is a compiles a graph with the given batch_data.
        It uses dictionary to represent the edges. Each vertex has
        a key in the directionary. All the neighboring vertices are
        stored as set as values.
    """
    def __init__(self, batch_data):
        """
            Initialize the object with the given batch_data.
            Stores dictionary of sets for each transaction.
        """
        self.graph = {}
        with open(batch_data, 'r') as data:
            next(data)
            for row in data:
                row_split = row.strip().split(',')
                id1 = row_split[1]
                id2 = row_split[2]
                if id1 in self.graph:
                    self.graph[id1].add(id2)
                else:
                    self.graph[id1] = set([id2])
                if id2 in self.graph:
                    self.graph[id2].add(id1)
                else:
                    self.graph[id2] = set([id1])
                    
    def feature1(self, id1, id2):
        """
            Check whether id1 and id2 are connected in the graph.
            Return "trusted" if they are, "unverified" if not.
        """
        if id1 in self.graph:
            if id2 in self.graph[id1]:
                return "trusted"
        return "unverified"

    def feature2(self, id1, id2):
        """
            Check whether id1 and id2 has common connection the graph.
            Return "trusted" if they are, "unverified" if not.
        """
        # id1 and id2 might not be in the graph
        if id1 in self.graph and id2 in self.graph:
            if len(self.graph[id1].intersection(self.graph[id2])) > 0:
                return "trusted"
        return "unverified"

    def feature3(self, id1, id2):
        """
            Check whether id1 and id2 are connected within 4 degrees
            of separation the graph. Trying to save some space by
            finding the common second degree vertices from both ends. 
            Return "trusted" if they are, "unverified" if not.
        """
        # check id1 might not be in the graph
        if id1 in self.graph:
            # find all vertices within 2 degree of separation from id1
            set1 = self.graph[id1]
            for i in copy.copy(set1):
                set1 = set1.union(self.graph[i])
        else:
            return "unverified"

        # check id2 might not be in the graph
        if id1 in self.graph:
            # find all vertices within 2 degree of separation from id2
            set2 = self.graph[id2]
            for i in copy.copy(set2):
                set2 = set2.union(self.graph[i])
        else:
            return "unverified"

        # check if there are common 2 second degree of separations
        if len(set1.intersection(set2)):
            return "trusted"
        return "unverified"

def main(batch_data, stream_data, output1, output2, output3):
    """
        This program will create a graph object from the batch_data.
        It will go though stream_data and export to output1, output2,
        and output3 line by line. All the arguments are assumed to be
        vaild file path. 
    """
    pass

    



if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
