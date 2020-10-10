# File:    gas.py
# Author:  Edward Hanson (eth20@duke.edu)
# Desc.:   Gather-apply-scatter graph decomposition. Generate traces to be used for PowerGraph analysis

import math
from operator import itemgetter

# Generate sample graph using Adjacency List representation
# outgoing: outgoing edges per vertex
# incoming: incoming edges per vertex
def sampleGraph():
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

    data = sorted(data, key=itemgetter(1))

    numVertices = 9
    
    return data, numVertices

# Generate list for number of outgoing edges per node
def genOutgoing(data, numVertices):
    numOutgoing = []
    for i in range(numVertices):
        numOutgoing.append(0)

    for e in data:
        numOutgoing[e[0]] += 1

    print(numOutgoing)
    return numOutgoing

# Partition given graph across 3 nodes: main, remote, device
# Arguments [main, remote, device] must be floats between [0, 1] representing fraction of data they will hold
def partitionGraph(data, numVertices, main, remote, device):
    # Assuming 1 high-degree vertex for now...

    assert ((main + remote + device) < 1.1) and ((main + remote + device) > 0.9), "Main + remote + device must sum to 1.0"

    lenMain = int(math.ceil(main * len(data)))
    lenRemote = int(math.ceil(remote * len(data)))
    lenDevice = int(math.ceil(len(data) - (lenMain + lenRemote)))
    
    mainCut   = data[0 : lenMain]
    remoteCut = data[lenMain : lenMain + lenRemote]
    deviceCut = data[lenMain + lenRemote : lenMain + lenRemote + lenDevice]

    MR_shared = []
    RD_shared = []
    MD_shared = []
    for v in range(numVertices):
        if any(v in sublist for sublist in mainCut) and any(v in sublist for sublist in remoteCut):
            MR_shared.append(1)
        else:
            MR_shared.append(0)
        if any(v in sublist for sublist in remoteCut) and any(v in sublist for sublist in deviceCut):
            RD_shared.append(1)
        else:
            RD_shared.append(0)
        if any(v in sublist for sublist in mainCut) and any(v in sublist for sublist in deviceCut):
            MD_shared.append(1)
        else:
            MD_shared.append(0)

    """
    MR_shared = []
    RM_shared = []
    RD_shared = []
    DR_shared = []
    MD_shared = []
    DM_shared = []
    for v in range(numVertices):
        if (v in [row[0] for row in remoteCut]) and (v in [row[1] for row in mainCut]):
            MR_shared.append(1)
        else:
            MR_shared.append(0)
        if (v in [row[0] for row in mainCut]) and (v in [row[1] for row in remoteCut]):
            RM_shared.append(1)
        else:
            RM_shared.append(0)
        if (v in [row[0] for row in deviceCut]) and (v in [row[1] for row in remoteCut]):
            RD_shared.append(1)
        else:
            RD_shared.append(0)
        if (v in [row[0] for row in remoteCut]) and (v in [row[1] for row in deviceCut]):
            DR_shared.append(1)
        else:
            DR_shared.append(0)
        if (v in [row[0] for row in deviceCut]) and (v in [row[1] for row in mainCut]):
            MD_shared.append(1)
        else:
            MD_shared.append(0)
        if (v in [row[0] for row in mainCut]) and (v in [row[1] for row in deviceCut]):
            DM_shared.append(1)
        else:
            DM_shared.append(0)
    """
    
    print(MR_shared)
    print(RD_shared)
    print(MD_shared)
    
    return mainCut, remoteCut, deviceCut, MR_shared, RD_shared, MD_shared

# recursive portion of pagerank algorithm
def PR(graph, startIdx, numOutgoing, sharedList1, sharedList2, ranks, done, dirty):
    curr = graph[startIdx][1]
    tot = 0
    idx = startIdx
    print("Accumulating for vertex {}".format(curr))
    while(idx < len(graph) and graph[idx][1] == curr):
        if (sharedList1[graph[idx][0]] and dirty[graph[idx][0]]):
            print("Receive vertex {} with sharedList1".format(graph[idx][0]))
            dirty[graph[idx][0]] = False                               ######## COULD BE ERRONEOUS
        if (sharedList2[graph[idx][0]] and dirty[graph[idx][0]]):
            print("Receive vertex {} with sharedList2".format(graph[idx][0]))
            dirty[graph[idx][0]] = False                               ######## COULD BE ERRONEOUS
        
        tot += ranks[graph[idx][0]] / numOutgoing[graph[idx][0]]
        idx += 1
        
    # check if need to receive data
    if (sharedList1[curr] and dirty[curr]):
        print("SYNC vertex {} with sharedList1".format(curr))
        dirty[curr] = False                                            ######## COULD BE ERRONEOUS
    if (sharedList2[curr] and dirty[curr]):
        print("SYNC vertex {} with sharedList2".format(curr))
        dirty[curr] = False                                            ######## COULD BE ERRONEOUS
        
    print("Update rank of vertex {}".format(curr))
    ranks[curr] = tot
    done[curr] = True
    dirty[curr] = True

    for out in range(idx,len(graph)):
        if graph[out][0] == curr and not done[graph[out][1]]:
            print("Scatter execution to vertex {}".format(graph[out][1]))
            ranks, done = PR(graph, out, numOutgoing, sharedList1, sharedList2, ranks, done, dirty)

    return ranks, done

# apply pagerank algorithm
def PageRank(numVertices, ranks, done, dirty, graph, numOutgoing, sharedList1, sharedList2, maxIter):

    # check for empty graph
    if len(graph) == 0:
        return ranks, done
    
    # assuming graph is sorted by DESTINATION ...
    startIdx = 0
    ranks, done = PR(graph, startIdx, numOutgoing, sharedList1, sharedList2, ranks, done, dirty)

    return ranks, done

def main():
    graph, numVertices = sampleGraph()
    numOutgoing = genOutgoing(graph, numVertices)
    mainCut, remoteCut, deviceCut, MR_shared, RD_shared, MD_shared = partitionGraph(graph, numVertices, 1.0/3.0, 1.0/3.0, 1.0/3.0)

    # initial ranks
    initRank = 1.0 / numVertices
    ranks = []
    for i in range(numVertices):
        ranks.append(initRank)

    # dirty bits
    dirty = []
    for i in range(numVertices):
        dirty.append(False)
        
    numIter = 1
    for x in range(numIter):
        done = []
        for i in range(numVertices):
            done.append(False)

        print("--- Host ---")
        ranks, done = PageRank(numVertices, ranks, done, dirty, mainCut, numOutgoing, MR_shared, MD_shared, 1)
        print("--- Remote ---")
        ranks, done = PageRank(numVertices, ranks, done, dirty, remoteCut, numOutgoing, MR_shared, RD_shared, 1)
        print("--- Device ---")
        ranks, done = PageRank(numVertices, ranks, done, dirty, deviceCut, numOutgoing, MD_shared, RD_shared, 1)

    print(ranks)
    print(done)

    #"""
    print(mainCut)
    print(remoteCut)
    print(deviceCut)
    print(MR_shared)
    print(RD_shared)
    print(MD_shared)
    #"""

if __name__ == "__main__":
    main()
    
####### NOTES:
# Gather should be done across all edges IN PARALLEL -> maximize CPU performance
