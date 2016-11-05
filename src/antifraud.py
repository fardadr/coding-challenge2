# Written by Gene Der Su for Insight Coding Challenge.

import sys

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
                    if id2 not in self.graph[id1]:
                        self.graph[id1].add(id2)
                else:
                    self.graph[id1] = set([id2])
                if id2 in self.graph:
                    if id1 not in self.graph[id2]:
                        self.graph[id2].add(id1)
                else:
                    self.graph[id2] = set([id1])
                    
    def feature1(id1, id2):
        pass

    def feature2(id1, id2):
        pass

    def feature3(id1, id2):
        pass
                
        

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
