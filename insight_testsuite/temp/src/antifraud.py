# Written by Gene Der Su for Insight Coding Challenge.

import sys
import copy
import time

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
        with open(batch_data, 'r') as batch:
            next(batch)
            for row in batch:
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
        
        self.graph2 = {}
        for i in self.graph:
            self.graph2[i] = self.graph[i]
            for j in self.graph[i]:
                self.graph2[i].union(self.graph[j])
            self.graph2[i].add(i)

                    
    def feature1(self, id1, id2):
        """
            Check whether id1 and id2 are connected in the graph.
            Return True if they are, False if not.
        """
        if id1 in self.graph:
            if id2 in self.graph[id1]:
                return True
        return False

        
    def feature2(self, id1, id2):
        """
            Check whether id1 and id2 has common connection the graph.
            Return "trusted" if they are, "unverified" if not.
        """
##        # check for the first degree connection
##        if self.feature1(id1, id2) == "trusted\n":
##            return "trusted\n"
##    
##        # id1 and id2 might not be in the graph
##        if id1 in self.graph and id2 in self.graph:
##            if self.check_intersection(self.graph[id1],self.graph[id2]):
##                return "trusted\n"
##        return "unverified\n"
    
        """
            Check whether id2 is within 2 degree of separations
            of id1 using per-computed second degree graph. Return
            True if they are, False if not.
        """
        # id1 might not be in the graph
        if id1 in self.graph2:
            if id2 in self.graph2[id1]:
                return True
        return False
        

    def check_intersection(self, set1, set2):
        """
            Given 2 sets, return True if there is at least one
            interection, else return False.
        """
        for i in set1:
            if i in set2:
                return True
        return False


    def feature3(self, id1, id2):
        """
            Check whether id1 and id2 are connected within 4 degrees
            of separation the graph. Trying to save some space by
            finding the common second degree vertices from both ends. 
            Return "trusted" if they are, "unverified" if not.
        """
        
##        # check for the first degree of separation
##        if self.feature1(id1, id2) == "trusted\n":
##            return "trusted\n"
##        
##        # check id1 might not be in the graph
##        if id1 in self.graph:
##            # find all vertices within 2 degree of separation from id1
##            set1 = self.graph[id1]
##            for i in copy.copy(set1):
##                set1 = set1.union(self.graph[i])
##            set1.add(id1)
##        else:
##            return "unverified\n"
##
##        # check if id2 in second degree of separations
##        if id2 in set1:
##            return "trusted\n"
##
##        # check id2 might not be in the graph
##        if id2 in self.graph:
##            # find all vertices within 2 degree of separation from id2
##            set2 = self.graph[id2]
##            for i in copy.copy(set2):
##                set2 = set2.union(self.graph[i])
##            set2.add(id2)
##        else:
##            return "unverified\n"
##        # check if there are common 2 second degree of separations
##        if self.check_intersection(set1, set2):
##            return "trusted\n"
##        return "unverified\n"
        
        if self.check_intersection(self.graph2[id1], self.graph2[id2]):
            return True
        return False

        


def main(batch_data, stream_data, output1, output2, output3):
    """
        This program will create a graph object from the batch_data.
        It will go though stream_data and export to output1, output2,
        and output3 line by line. All the arguments are assumed to be
        vaild file path. 
    """
    t0 = time.time()
    batch_graph = Graph(batch_data)
    print "\nTime used to create graph: %ss\n"%(time.time() - t0)

    t0 = time.time()
    counter = 0
    with open(stream_data,'r') as stream, \
         open(output1,'w') as f1,\
         open(output2,'w') as f2,\
         open(output3,'w') as f3:
        next(stream)
        
        t0 = time.time()
        
        for row in stream:
            row_split = row.strip().split(',')
            id1 = row_split[1]
            id2 = row_split[2]
            current_feature1 = batch_graph.feature1(id1,id2)
            if batch_graph.feature1(id1,id2):
                f1.write("trusted\n")
                f2.write("trusted\n")
                f3.write("trusted\n")
            else:
                f1.write("unverified\n")
                if batch_graph.feature2(id1,id2):
                    f2.write("trusted\n")
                    f3.write("trusted\n")
                else:
                    f2.write("unverified\n")
                    if batch_graph.feature3(id1,id2):
                        f3.write("trusted\n")
                    else:
                        f3.write("unverified\n")
            counter += 1
            if counter % 100000 == 0:
                print "Row count:", counter

    print "\nAll feature completed, total %s rows, average time %s s/row"%(counter, (time.time() - t0)/counter)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
