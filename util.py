import math
from operator import itemgetter
import time
import random
random.seed(1)

MULTIPLIER = 0.5 # 0.5, 1.0, 4.0

# find max item in an arbitary depth of list of lists
def get_max(my_list):
    m = None
    for item in my_list:
        if isinstance(item, list):
            item = get_max(item)
        if not m or m < item:
            m = item
    return m

# Generate list for number of outgoing edges per node
def genOutgoing(data, numVertices):
    numOutgoing = []
    for i in range(numVertices):
        numOutgoing.append(0)
        
    for e in data:
        numOutgoing[e[0]] += 1
        
    #print(numOutgoing)
    return numOutgoing

# Generate sample graph using Adjacency List representation
# outgoing: outgoing edges per vertex
# incoming: incoming edges per vertex
def sampleGraph(name="small", size_chunk=100, randomize=False):

    if name == "natural":
        data = []
        numIncoming = []
        for destination in range(size_chunk):
            #print("destination: {}".format(destination))
            numIncoming.append(0)
            isFull = False
            while True:
                # select a source edge
                while True:
                    if numIncoming[destination] == size_chunk:
                        #print("FULL -- numincoming = {}".format(numIncoming[destination]))
                        isFull = True
                        break
                    source = random.randint(0, size_chunk-1)
                    if ([source, destination] not in data) and (source != destination):
                        #print("ADDED [{}, {}]".format(source, destination))
                        data.append([source, destination])
                        numIncoming[destination] += 1
                        break
                    
                if isFull:
                    break
                # check if power-law degree distribution rule does not check out
                if (numIncoming[destination] / (destination+1)) > ((numIncoming[destination] ** (-2)) * MULTIPLIER):
                    #print("POWER LAW DONE -- {} > {}".format((numIncoming[destination] / (destination+1)), (numIncoming[destination] ** (-2))))
                    break
    
    elif name == "large":
        data = []
        
        # chunk 1
        for i in range(1, size_chunk):
            data.append([i, 0])
            data.append([i, (i+1)%size_chunk])
            data.append([0, 0+1])
            data.append([0, size_chunk])
            
        # chunk 2
        for i in range(size_chunk+1, size_chunk*2):
            data.append([i, size_chunk])
            data.append([i, ((i+1)%size_chunk)+size_chunk])
            data.append([size_chunk, size_chunk+1])
            data.append([size_chunk, size_chunk*2])
            
        # chunk 3
        for i in range((size_chunk*2)+1, size_chunk*3):
            data.append([i, size_chunk*2])
            data.append([i, ((i+1)%size_chunk)+(size_chunk*2)])
            data.append([size_chunk*2, (size_chunk*2)+1])
            data.append([size_chunk*2, 0])
            
    elif name == "count":
        data = []
        for i in range(size_chunk):
            for j in range(i):
                data.append([i, j])
                data.append([i, (i+1)%size_chunk])
                
    elif name == "v2":
        data = []
        for i in range(1, size_chunk):
            data.append([i, 0])
            if i < size_chunk-1:
                data.append([i, i+1])
            else:
                data.append([i,1])
                data.append([0, 1])

    elif name == "v3":
        data = []
        # create 1 high-degree vertex
        for i in range(1, size_chunk):
            data.append([i,0])
        # randomly connect many more edges
        while len(data) < size_chunk*10:
            s = random.randint(0, size_chunk-1)
            d = random.randint(0, size_chunk-1)
            e = [s, d]
            if (s == d) or (e in data):
                continue
            data.append(e)
                
    elif name == "small":
        data = [[1,0],
                [2,0],
                [3,0],
                [4,0],
                [5,0],
                [6,0],
                [7,0],
                [8,0],
                [9,0],
                [0,10],
                
                [11,10],
                [12,10],
                [13,10],
                [14,10],
                [15,10],
                [16,10],
                [17,10],
                [18,10],
                [19,10],
                [10,20],
                
                [21,20],
                [22,20],
                [23,20],
                [24,20],
                [25,20],
                [26,20],
                [27,20],
                [28,20],
                [29,20],
                [20,0]]
    else:
        data = [[1,0],
                [2,0],
                [3,0],
                [4,0],
                [5,0],
                [6,0],
                [7,0],
                [8,0],
                [0,1],
                [1,2],
                [2,3],
                [3,4],
                [4,5],
                [5,6],
                [6,7],
                [7,8],
                [8,1]]

    if not randomize:
        data = sorted(data, key=itemgetter(1))
    else:
        random.shuffle(data)

    # -- Determine number of vertices in graph --
    #numVertices = 30
    #if name == "large":
    #    numVertices = size_chunk*3
    #else:
    numVertices = get_max(data) + 1
    
    return data, numVertices

def main():
    graph, numVertices = sampleGraph("natural", 1000)
    print(graph)
    
if __name__ == "__main__":
    main()
