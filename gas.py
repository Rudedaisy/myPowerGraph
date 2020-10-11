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

    """
            [0,1],
            [1,2],
            [2,3],
            [3,4],
            [4,5],
            [5,6],
            [6,7],
            [7,8],
            [8,1]]
    """
    data = sorted(data, key=itemgetter(1))

    numVertices = 30
    
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
def PR(graph, startIdx, numOutgoing, ranks, done, working, cached_host, cached_remote, cached_device, nodeType):
    curr = graph[startIdx][1]
    tot = 0
    idx = startIdx
    
    print("Accumulating for vertex {}".format(curr))
    while(idx < len(graph) and graph[idx][1] == curr):
        if nodeType == "host":
            if cached_host[graph[idx][0]] == 'I':
                if cached_device[graph[idx][0]] != 'I': # only send coherence signals for SHARED DATA
                    print("\tTransition vertex {} from 'I' to 'S' state. Send signal HOST->DEVICE".format(graph[idx][0])) ## force all device M to S
                else:
                    print("\tTransition vertex {} from 'I' to 'S' state.".format(graph[idx][0]))

                if cached_remote[graph[idx][0]] == 'M':
                    print("\tRemote now at 'S' state. Send data REMOTE->HOST")
                    cached_remote[graph[idx][0]] = 'S'
                if cached_device[graph[idx][0]] == 'M':
                    print("\tDevice now at 'S' state. Send data DEVICE->HOST")
                    cached_device[graph[idx][0]] = 'S'
                cached_host[graph[idx][0]] = 'S'
                
        elif nodeType == "remote":
            ## remote memory always ejected from host after computation --> SLIGHTLY INCORRECT BEHAVIOR
            if cached_remote[graph[idx][0]] == 'I':
                if cached_device[graph[idx][0]] != 'I': # only send coherence signals for SHARED DATA
                    print("\tTransition vertex {} from 'I' to 'S' state. Send signal HOST->DEVICE".format(graph[idx][0])) ## force all device M to S
                else:
                    print("\tTransition vertex {} from 'I' to 'S' state.".format(graph[idx][0]))

                if cached_host[graph[idx][0]] == 'M':
                    cached_host[graph[idx][0]] = 'S'
                elif cached_device[graph[idx][0]] == 'M':
                    print("\tDevice now at 'S' state. Send data DEVICE->HOST")
                    cached_device[graph[idx][0]] = 'S'
                else: # special case for remote
                    print("\tCaching vertex {} from remote memory. Send data REMOTE->HOST".format(graph[idx][0]))
                cached_remote[graph[idx][0]] = 'S'
            else: # special case for remote
                print("\tCaching vertex {} from remote memory. Send data REMOTE->HOST".format(graph[idx][0]))

        elif nodeType == "device":
            if cached_device[graph[idx][0]] == 'I':
                if cached_host[graph[idx][0]] != 'I' or cached_remote[graph[idx][0]] != 'I':
                    print("\tTransition vertex {} from 'I' to 'S' state. Send signal DEVICE->HOST".format(graph[idx][0])) ## force all other devices and host M to S
                else:
                    print("\tTransition vertex {} from 'I' to 'S' state.".format(graph[idx][0]))
                    
                if cached_host[graph[idx][0]] == 'M':
                    print("\tHost now at 'S' state. Send data HOST->DEVICE")
                    cached_host[graph[idx][0]] = 'S'
                if cached_remote[graph[idx][0]] == 'M':
                    print("\tRemote now at 'S' state. Send data REMOTE->DEVICE")
                    cached_remote[graph[idx][0]] = 'S'
                cached_device[graph[idx][0]] = 'S'

        tot += ranks[graph[idx][0]] / numOutgoing[graph[idx][0]]
        idx += 1
        
    # check if need to receive data
    if nodeType == "host":
        if cached_host[curr] != 'M':
            
            if cached_host[curr] == 'S':
                if cached_device[curr] == 'I':
                    print("\tTransition vertex {} from 'S' to 'M' state.".format(curr))
                else:
                    print("\tTransition vertex {} from 'S' to 'M' state. Send invalidate signal HOST->DEVICE".format(curr))
            else: # 'I' state
                print("\tTransition vertex {} from 'I' to 'M' state. Send signal HOST->DEVICE and wait for response".format(curr))
                if cached_remote[curr] == 'M':
                    print("\tRemote now at 'I' state. Send data REMOTE->HOST")
                elif cached_device[curr] == 'M':
                    print("\tDevice now at 'I' state. Send data DEVICE->HOST")
                elif cached_remote[curr] == 'S':
                    print("\tRemote now at 'I' state. Send data REMOTE->HOST")
                elif cached_device[curr] == 'S':
                    print("\tRemote now at 'I' state. Send data DEVICE->HOST")

            cached_host[curr] = 'M'
            cached_remote[curr] = 'I'
            cached_device[curr] = 'I'
            
    elif nodeType == "remote":
        if cached_remote[curr] != 'M':

            if cached_remote[curr] == 'S':
                if cached_device[curr] != 'I':
                    print("\tTransition vertex {} from 'S' to 'M' state.".format(curr))
                else:
                    print("\tTransition vertex {} from 'S' to 'M' state. Send invalidate signal HOST->DEVICE".format(curr))
            else: # 'I' state
                if cached_host[curr] == 'I':
                    print("\tTransition vertex {} from 'I' to 'M' state. Send signal HOST->DEVICE".format(curr))
                    print(cached_host)
                else:
                    print("\tTransition vertex {} from 'I' to 'M' state.".format(curr))
                    
                if cached_host[curr] == 'M':
                    print("\tHost now at 'I' state.")
                elif cached_device[curr] == 'M':
                    print("\tDevice now at 'I' state. Send data DEVICE->HOST")
                elif cached_host[curr] == 'S':
                    print("\tHost now at 'I' state.")
                elif cached_device[curr] == 'S':
                    print("\tRemote now at 'I' state. Send data DEVICE->HOST")
                else: # special state only for remote
                    print("\tRetrieve vertex {} from remote memory. Send data REMOTE->HOST".format(curr))

            cached_host[curr] = 'I'
            cached_remote[curr] = 'M'
            cached_device[curr] = 'I'

    elif nodeType == "device":
        if cached_device[curr] != 'M':

            if cached_device[curr] == 'S':
                if cached_host[curr] == 'I' and cached_remote[curr] == 'I':
                    print("\tTransition vertex {} from 'S' to 'M' state.".format(curr))
                else:
                    print("\tTransition vertex {} from 'S' to 'M' state. Send invalidate signal DEVICE->HOST".format(curr))
            else: # 'I' state
                print("\tTransition vertex {} from 'I' to 'M' state. Send signal DEVICE->HOST".format(curr))
                if cached_host[curr] == 'M':
                    print("\tHost now at 'I' state. Send data HOST->DEVICE") ### this and below are potential gridlock point
                elif cached_remote[curr] == 'M':
                    print("\tRemote now at 'I' state. Send data REMOTE->DEVICE") ### this and above are potential gridlock point
                elif cached_host[curr] == 'S':
                    print("\tHost now at 'I' state. Send data HOST->DEVICE") ### this and below are potential gridlock point
                elif cached_remote[curr] == 'S':
                    print("\tRemote now at 'I' state. Send data REMOTE->DEVICE") ### this and above are potential gridlock point

            cached_host[curr] = 'I'
            cached_remote[curr] = 'I'
            cached_device[curr] = 'M'
                    
    print("COMPUTE: Update rank of vertex {}".format(curr))
    if not working[curr]:
        ranks[curr] = tot
        working[curr] = True
    else:
        ranks[curr] += tot
    done[curr] = True

    for out in range(idx,len(graph)):
        if graph[out][0] == curr and not done[graph[out][1]]:
            print("Scatter execution to vertex {}".format(graph[out][1]))
            ranks, working, cached_host, cached_remote, cached_device = PR(graph, out, numOutgoing, ranks, done, working, cached_host, cached_remote, cached_device, nodeType)

    return ranks, working, cached_host, cached_remote, cached_device

# apply pagerank algorithm
def PageRank(ranks, numVertices, working, cached_host, cached_remote, cached_device, nodeType, graph, numOutgoing, maxIter):

    # check for empty graph
    if len(graph) == 0:
        return ranks, done

    # 'done' is local to each partition
    done = []
    for i in range(numVertices):
        done.append(False)
    
    # assuming graph is sorted by DESTINATION ...
    startIdx = 0
    ranks, working, cached_host, cached_remote, cached_device = PR(graph, startIdx, numOutgoing, ranks, done, working, cached_host, cached_remote, cached_device, nodeType)

    return ranks, working, cached_host, cached_remote, cached_device

def main():
    graph, numVertices = sampleGraph()
    numOutgoing = genOutgoing(graph, numVertices)
    mainCut, remoteCut, deviceCut, MR_shared, RD_shared, MD_shared = partitionGraph(graph, numVertices, 1.0/3.0, 1.0/3.0, 1.0/3.0)

    # initial ranks
    initRank = 1.0 / numVertices
    ranks = []
    for i in range(numVertices):
        ranks.append(initRank)

    # bits for MSI coherence tracking
    cached_host = []
    cached_remote = []
    cached_device = []
    for i in range(numVertices):
        cached_host.append('I')
        cached_remote.append('I')
        cached_device.append('I')
        
    numIter = 2
    for x in range(numIter):
        working = []
        for i in range(numVertices):
            working.append(False)

        print("--- Host ---")
        ranks, working, cached_host, cached_remote, cached_device = PageRank(ranks, numVertices, working, cached_host, cached_remote, cached_device, "host", mainCut, numOutgoing, 1)
        print("--- Remote ---")
        ranks, working, cached_host, cached_remote, cached_device = PageRank(ranks, numVertices, working, cached_host, cached_remote, cached_device, "remote", remoteCut, numOutgoing, 1)
        print("--- Device ---")
        ranks, working, cached_host, cached_remote, cached_device = PageRank(ranks, numVertices, working, cached_host, cached_remote, cached_device, "device", deviceCut, numOutgoing, 1)

    print(ranks)

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
