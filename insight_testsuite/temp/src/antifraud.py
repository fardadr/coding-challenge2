# Written by Gene Der Su for Insight Coding Challenge.

import sys
import copy
import time

class Graph:
    """
        This class compiles a dictionary of first degree connections
        and another dictionary for second degree connections for all
        vercites with the given batch_data. Each vertex has a key in
        the directionary. All the satisfying vertices are stored as a
        set as values. There are also three class methods, one for
        each feature. Also there is a helper function for feature 3
        to early termination the intersection checking process.

        The code is optmized for runtime. The overhead for a large
        file might be high as to pre-computer all the second degree
        of separations, but it will be fast for each stream action.

        On a mid 2015 MacBook Pro with 2.8 GHz Intel Core i7, it will
        take around 3 minutes for the preprocessing of a file with
        close to 4 million lines in the batch_payment.txt. The runtime
        for each stream_payment is around 1.1e-05 seconds.
    """
    def __init__(self, batch_data):
        """
            Initialize the object with the given batch_data.
            Store dictionary of sets for each transaction.
            Compute second degree of separations for each node.
        """
        # genearte dictionary of first degree of separations
        t = time.time()
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

        # report status
        num_nodes = len(self.graph)
        print "First degree graph generated, total %s nodes, time used: %ss"%(num_nodes, time.time() - t)
        
        # compute second degree of separations
        count = 0
        self.graph2 = {}
        for i in self.graph:
            self.graph2[i] = self.graph[i]
            for j in self.graph[i]:
                self.graph2[i] = self.graph2[i].union(self.graph[j])
            self.graph2[i].add(i)
            
            # increment count and print the process so the user know
            # the program is actually running
            count += 1
            if count % 1000 == 0:
                print "Processing second degree graph: %s/%s"%(count, num_nodes)

                    
    def feature1(self, id1, id2):
        """
            Check whether id1 and id2 are connected in the graph.
            Return True if they are, False if not.
        """
        # id1 might not be in the graph
        if id1 in self.graph:
            if id2 in self.graph[id1]:
                return True
        return False

        
    def feature2(self, id1, id2):
        """
            Check whether id2 is within second degree of separations
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
            Helper function for early termination of check any
            intersection nodes for feature3. Given 2 sets, return
            True if there is at least one interection node, else
            return False.
        """
        for i in set1:
            if i in set2:
                return True
        return False


    def feature3(self, id1, id2):
        """
            Check whether id1 and id2 are connected within 4 degrees
            of separations the graph. Check by finding whether there
            are interection second degree of separations from both
            nodes. Return True if they are, False if not.
        """
        # id1 and id2 might not be in the graph
        if id1 in self.graph2 and id2 in self.graph2:
            if self.check_intersection(self.graph2[id1], self.graph2[id2]):
                return True
        return False


def main(batch_data, stream_data, output1, output2, output3):
    """
        This program will create a graph object from the batch_data.
        It will go through stream_data and export to output1, output2,
        and output3 line by line. All the arguments are assumed to be
        vaild file paths with correct formatting. 
    """
    # create the graph object and also calculate the time.
    # took around 3 minutes on my computer for a file close
    # to 4 million lines
    t0 = time.time()
    batch_graph = Graph(batch_data)
    print "\nTime used to create graphs: %ss\n"%(time.time() - t0)

    with open(stream_data, 'r') as stream, \
         open(output1, 'w') as f1,\
         open(output2, 'w') as f2,\
         open(output3, 'w') as f3:
        
        next(stream)
        counter = 0
        t0 = time.time()
        for row in stream:
            row_split = row.strip().split(',')
            id1 = row_split[1]
            id2 = row_split[2]
            
            # compute feature1, if feature 1 passes, all shold pass
            current_feature1 = batch_graph.feature1(id1, id2)
            if batch_graph.feature1(id1, id2):
                f1.write("trusted\n")
                f2.write("trusted\n")
                f3.write("trusted\n")
            else:
                
                # when feature 1 did not pass, check feature 2
                f1.write("unverified\n")
                
                # if feature 2 passes, both feature 2 and 3 are passed
                if batch_graph.feature2(id1,id2):
                    f2.write("trusted\n")
                    f3.write("trusted\n")
                else:

                    # when feature 2 also did not pass, check feature 3
                    f2.write("unverified\n")
                    
                    # assign the result depend on the feature3 outcome
                    if batch_graph.feature3(id1,id2):
                        f3.write("trusted\n")
                    else:
                        f3.write("unverified\n")
                        
            # increment counter and print the count so the user know
            # the program is actually running
            counter += 1
            if counter % 200000 == 0:
                print "Stream payments count:", counter

    # report the number of payment processed and the performance
    print "\nAll feature completed, total %s stream payments, average time %s s/payment\n"%(counter, (time.time() - t0)/counter)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
