import math
import numpy as np
from operator import itemgetter
from util import *
import random
random.seed(1)

COMPUTE_ONCE = 7.152557373046875e-07

BETA_0_MULTIPLIER = 5.5 #3
BETA_1 = "NA" #"NA"
BETA_2_MULTIPLIER = 1 #1
LAMBDA = 0.3 #0.3

def genIncoming(graph, numVertices):
    numIncoming = []
    for i in range(numVertices):
        numIncoming.append(0)
        
    for e in graph:
        numIncoming[e[1]] += 1
        
    #print(numIncoming)
    return numIncoming

def checkDest(dest, hostCut, deviceCut):
    if dest in [edge[1] for edge in hostCut]:
        # destination already exists in Host
        destInHost = True
    else:
        destInHost = False
    if dest in [edge[1] for edge in deviceCut]:
        # destination already exists in Device
        destInDevice = True
    else:
        destInDevice = False
        
    return destInHost, destInDevice

def checkVertex(v, cut):
    if v in [edge[0] for edge in cut]:
        return True
    if v in [edge[1] for edge in cut]:
        return True
    return False

def checkBothVertices(e, cut):
    if checkVertex(e[0], cut) and checkVertex(e[1], cut):
        return True
    return False

def maxedges(hostCut, deviceCut):
    return max(len(hostCut), len(deviceCut))
def minedges(hostCut, deviceCut):
    return min(len(hostCut), len(deviceCut))

def balance(curr, hostCut, deviceCut):
    dividend = maxedges(hostCut, deviceCut) - len(curr)
    divisor = maxedges(hostCut, deviceCut) - minedges(hostCut, deviceCut) + 1
    return (float(dividend) / divisor)

def score_in_DegreeIO(e, curr, hostCut, deviceCut, numIncoming):
    i1 = int(checkVertex(e[0], curr))
    i2 = int(checkVertex(e[1], curr))
    i3 = int(checkVertex(e[0], curr) and ( numIncoming[e[0]] <= numIncoming[e[1]] ))
    i4 = int(checkVertex(e[1], curr) and ( numIncoming[e[1]] <= numIncoming[e[0]] ))
    i5 = balance(curr, hostCut, deviceCut)
    return (i1+i2+i3+i4+i5)
def score_DegreeIO(e, curr, hostCut, deviceCut, numIncoming, numOutgoing):
    i1 = int(checkVertex(e[0], curr))
    i2 = int(checkVertex(e[1], curr))
    i3 = int(checkVertex(e[0], curr) and ( (numIncoming[e[0]]+numOutgoing[e[0]]) <= (numIncoming[e[1]]+numOutgoing[e[1]]) ))
    i4 = int(checkVertex(e[1], curr) and ( (numIncoming[e[1]]+numOutgoing[e[1]]) <= (numIncoming[e[0]]+numOutgoing[e[0]]) ))
    i5 = balance(curr, hostCut, deviceCut)
    return (i1+i2+i3+i4+i5)

# S-PowerGraph's DegreeIO
def DegreeIO(graph, numVertices, numOutgoing):
    numIncoming = genIncoming(graph, numVertices)
    random.shuffle(graph)

    hostCut = []
    deviceCut = []

    for e in graph:
        if score_DegreeIO(e, hostCut, hostCut, deviceCut, numIncoming, numOutgoing) >= score_DegreeIO(e, deviceCut, hostCut, deviceCut, numIncoming, numOutgoing):
            hostCut.append(e)
        else:
            deviceCut.append(e)

    hostCut = sorted(hostCut, key=itemgetter(0))
    hostCut = sorted(hostCut, key=itemgetter(1))
    deviceCut = sorted(deviceCut, key=itemgetter(0))
    deviceCut = sorted(deviceCut, key=itemgetter(1))
    return hostCut, deviceCut

# original partition function used in PowerGraph
def greedyPartition(graph):
    random.shuffle(graph)
    
    hostCut = []
    deviceCut = []

    for e in graph:
        """ Case 1 """
        if checkBothVertices(e, hostCut) and checkBothVertices(e, deviceCut):
            if len(hostCut) < len(deviceCut):
                hostCut.append(e)
                continue
            else:
                deviceCut.append(e)
                continue
        elif checkBothVertices(e, hostCut):
            hostCut.append(e)
            continue
        elif checkBothVertices(e, deviceCut):
            deviceCut.append(e)
            continue

        """ Case 2 """
        if (checkVertex(e[0], hostCut) or checkVertex(e[0], deviceCut)) and (checkVertex(e[1], hostCut) or checkVertex(e[1], deviceCut)):
            if len(hostCut) < len(deviceCut):
                hostCut.append(e)
                continue
            else:
                deviceCut.append(e)
                continue

        """ Case 3 """
        if checkVertex(e[0], hostCut):
            if checkVertex(e[0], deviceCut):
                if len(hostCut) < len(deviceCut):
                    hostCut.append(e)
                    continue
                else:
                    deviceCut.append(e)
                    continue
            else:
                hostCut.append(e)
                continue
        elif checkVertex(e[1], hostCut):
            if checkVertex(e[1], deviceCut):
                if len(hostCut) < len(deviceCut):
                    hostCut.append(e)
                    continue
                else:
                    deviceCut.append(e)
                    continue
            else:
                deviceCut.append(e)
                continue

        """ Case 4 """
        if len(hostCut) < len(deviceCut):
            hostCut.append(e)
            continue
        else:
            deviceCut.append(e)
            continue

        assert False, "Impossible case encountered!"

        
    hostCut = sorted(hostCut, key=itemgetter(0))
    hostCut = sorted(hostCut, key=itemgetter(1))
    deviceCut = sorted(deviceCut, key=itemgetter(0))
    deviceCut = sorted(deviceCut, key=itemgetter(1))
    return hostCut, deviceCut

def smartPartition(graph, numVertices, numOutgoing, coherenceSymmetry="asym_dev", relativeDevComputeCapability=1.0):
    # sort by destination; with each destination sorted by source
    graph = sorted(graph, key=itemgetter(0))
    graph = sorted(graph, key=itemgetter(1))

    numIncoming = genIncoming(graph, numVertices)
    
    """
    -- identify high-degree vertices
    1) partition low-degree vertices first
    2) high-degree vertices act as the balancing agent (i.e. doesn't impact dests, but impacts #vertices, sharedVertices, and edges
    """

    # balance factor for compute capability between host and device
    deviceBeta = relativeDevComputeCapability / (relativeDevComputeCapability + 1.0)
    hostBeta = 1.0 - deviceBeta
    #OVERRIDE = 0.48 #0.47 #0.5 #0.45
    #hostBeta = OVERRIDE

    avgDegree = float(sum(numIncoming)) / len(numIncoming)
    #hostBeta = (COMPUTE_ONCE * avgDegree) / (2*COMPUTE_ONCE*avgDegree + 0.5e-6)
    if BETA_1 == "NA":
        """ beta_1 """
        hostBeta = (COMPUTE_ONCE * avgDegree + 0.5e-6) / (2*COMPUTE_ONCE*avgDegree + 0.5e-6 + 30e-9)
        hostBeta = 1 - hostBeta
    else:
        hostBeta = BETA_1
        hostBeta = 1 - hostBeta
    print("a_1 = {}".format(hostBeta))
    
    hostDests = 0
    deviceDests = 0
    hostCut = []
    deviceCut = []
    
    destInHost = False
    destInDevice = False
    
    # find the high-degree vertices
    #HIGH_DEGREE = 0.07
    mean = (avgDegree / len(graph)) * 1.0
    #HIGH_DEGREE = mean + ((np.std(graph) / len(graph)))*3
    HIGH_DEGREE = mean + (np.std(numIncoming) / len(numIncoming))*BETA_0_MULTIPLIER #3 """ beta_0 """
    print("HIGH_DEGREE = {}".format(HIGH_DEGREE))
    highDegrees = []
    lenGraph = len(graph)
    for e_idx in range(len(graph)-1,-1,-1):
        if numIncoming[graph[e_idx][1]] > HIGH_DEGREE*lenGraph:
            highDegrees.append(graph[e_idx])
            del graph[e_idx]

    # because graph is sorted, can simply form a big partition (with a tolerance equal to ~x definition of HIGH_DEGREE)
    """ beta_2 """
    BETA_2 = BETA_2_MULTIPLIER*HIGH_DEGREE
    lowerBound = int(len(graph) * (hostBeta - BETA_2)) 
    upperBound = int(math.ceil(len(graph) * (hostBeta + BETA_2)))
    hostCut = graph[:lowerBound]
    deviceCut = graph[upperBound:]

    destID = -1
    for e in hostCut:
        if destID != e[1]:
            hostDests += 1
            destID = e[1]
    destID = -1
    for e in deviceCut:
        if destID != e[1]:
            deviceDests += 1
            destID = e[1]
    
    # arbitrate the middle portion of edges
    for e in graph[lowerBound:upperBound]:
        destInHost, destInDevice = checkDest(e[1], hostCut, deviceCut)
        
        if abs(len(hostCut) - len(deviceCut)) < (LAMBDA * len(graph)):  ################ USED TO BE 0.3
            numHostCommon = checkVertex(e[0], hostCut) + int(destInHost)
            numDeviceCommon = checkVertex(e[0], deviceCut) + int(destInDevice)

            # reduce number of shared vertices
            if numHostCommon > numDeviceCommon:
                hostCut.append(e)
                if not destInHost:
                    hostDests += 1
            elif numDeviceCommon > numHostCommon:
                deviceCut.append(e)
                if not destInDevice:
                    deviceDests += 1
            else:
                # reduce number of unique destinations
                if destInDevice: # assuming dev is more compute_capable...
                    deviceCut.append(e)
                    if not destInDevice:
                        deviceDests += 1
                elif destInHost:
                    hostCut.append(e)
                    if not destInHost:
                        hostDests += 1
                else:
                    # assuming asym_dev...
                    deviceCut.append(e)
                    if not destInDevice:
                        deviceDests += 1
        else:
            if len(hostCut) < len(deviceCut):
                hostCut.append(e)
                if not destInHost:
                    hostDests += 1
            else:
                deviceCut.append(e)
                if not destInDevice:
                    deviceDests += 1
            
    for e in highDegrees:
        destInHost, destInDevice = checkDest(e[1], hostCut, deviceCut)
        
        if checkVertex(e[0], hostCut) and not checkVertex(e[0], deviceCut): # and (abs(len(hostCut) - len(deviceCut)) < 2):
            hostCut.append(e)
            if not destInHost:
                hostDests += 1
        elif checkVertex(e[0], deviceCut) and not checkVertex(e[0], hostCut): # and (abs(len(hostCut) - len(deviceCut)) < 2):
            deviceCut.append(e)
            if not destInDevice:
                deviceDests += 1
        else:
            if len(hostCut) < len(deviceCut):
                hostCut.append(e)
                if not destInHost:
                    hostDests += 1
            else:
                deviceCut.append(e)
                if not destInDevice:
                    deviceDests += 1


    hostCut = sorted(hostCut, key=itemgetter(0))
    hostCut = sorted(hostCut, key=itemgetter(1))
    deviceCut = sorted(deviceCut, key=itemgetter(0))
    deviceCut = sorted(deviceCut, key=itemgetter(1))
    return hostCut, deviceCut
    
def smartPartition_OLD3(graph, numVertices, numOutgoing, coherenceSymmetry="asym_dev", relativeDevComputeCapability=1.0):
    # sort by destination; with each destination sorted by source
    graph = sorted(graph, key=itemgetter(0))
    graph = sorted(graph, key=itemgetter(1))

    numIncoming = genIncoming(graph, numVertices)
    
    """
    More holistic approach
    -- consider the number of source edges leading to a destination edge
    -- it is OK to break-up high-degree vertices
    -- if not high-degree vertex, then better to send to partition with less communication overhead
    """

    hostDests = 0
    deviceDests = 0
    hostCut = []
    deviceCut = []
    
    destInHost = False
    destInDevice = False

    HIGH_DEGREE = 0.1
    
    for e_idx, e in enumerate(graph):
        destInHost, destInDevice = checkDest(e[1], hostCut, deviceCut)
        
        if numIncoming[e[1]] > HIGH_DEGREE*len(graph):
            # high degree vertex... good to spread across partitions
            if checkVertex(e[0], hostCut) and not checkVertex(e[0], deviceCut): # and (abs(len(hostCut) - len(deviceCut)) < 2):
                hostCut.append(e)
                if not destInHost:
                    hostDests += 1
            elif checkVertex(e[0], deviceCut) and not checkVertex(e[0], hostCut): # and (abs(len(hostCut) - len(deviceCut)) < 2):
                deviceCut.append(e)
                if not destInDevice:
                    deviceDests += 1
            else:
                if len(hostCut) < len(deviceCut):
                    hostCut.append(e)
                    if not destInHost:
                        hostDests += 1
                else:
                    deviceCut.append(e)
                    if not destInDevice:
                        deviceDests += 1
            continue

        if destInHost:
            hostCut.append(e)
            if not destInHost:
                hostDests += 1
        elif destInDevice:
            deviceCut.append(e)
            if not destInDevice:
                deviceDests += 1
        else:
            if coherenceSymmetry=="asym_dev":
                if deviceDests < 1.0*hostDests and checkVertex(e[0], deviceCut):
                    deviceCut.append(e)
                    if not destInDevice:
                        deviceDests += 1
                elif deviceDests > 1.0*hostDests and checkVertex(e[0], hostCut):
                    hostCut.append(e)
                    if not destInHost:
                        hostDests += 1
                elif deviceDests > 1.7*hostDests:
                    hostCut.append(e)
                    if not destInHost:
                        hostDests += 1
                else:
                    deviceCut.append(e)
                    if not destInDevice:
                        deviceDests += 1
            else:
                assert False, "state not supported yet"
                
    return hostCut, deviceCut
    
def smartPartition_OLD2(graph, numVertices, numOutgoing, coherenceSymmetry="sym", relativeDevComputeCapability=1.0):
    # sort by destination; with each destination sorted by source
    graph = sorted(graph, key=itemgetter(0))
    graph = sorted(graph, key=itemgetter(1))

    """
    Greedy algorithm...
    1) Prioritize minimizing shared vertices
    2) Place new unique destinations towards the biased-node
    3) Number of edges tuned to account for compute-capability
    """

    # balance factor for compute capability between host and device
    deviceBeta = relativeDevComputeCapability / (relativeDevComputeCapability + 1.0)
    hostBeta = 1.0 - deviceBeta

    hostDests = 0
    deviceDests = 0
    hostCut = []
    deviceCut = []
    
    destInHost = False
    destInDevice = False
    
    for e in graph:
        destInHost, destInDevice = checkDest(e[1], hostCut, deviceCut)

        numHostCommon = checkVertex(e[0], hostCut) + int(destInHost)
        numDeviceCommon = checkVertex(e[0], deviceCut) + int(destInDevice)

        if (abs(len(hostCut) - len(deviceCut)) < 20) and numHostCommon > numDeviceCommon:
            hostCut.append(e)
            if not destInHost:
                hostDests += 1
        elif (abs(len(hostCut) - len(deviceCut)) < 20) and numDeviceCommon > numHostCommon:
            deviceCut.append(e)
            if not destInDevice:
                deviceDests += 1
        else:
            # sharedVertices not impacted by adding to either partition
            # now minimize total number of unique destinations
            if (abs(len(hostCut) - len(deviceCut)) < 20) and destInHost and (not destInDevice):
                hostCut.append(e)
            elif (abs(len(hostCut) - len(deviceCut)) < 20) and destInDevice and (not destInHost):
                deviceCut.append(e)
            else:
                # number of unique destinations not impact by adding to either partition
                # now check for 2nd priority
                if (abs(len(hostCut) - len(deviceCut)) < 20) and coherenceSymmetry=="asym_host":
                    hostCut.append(e)
                    if not destInHost:
                        hostDests += 1
                elif (abs(len(hostCut) - len(deviceCut)) < 20) and coherenceSymmetry=="asym_dev":
                    deviceCut.append(e)
                    if not destInDevice:
                        deviceDests += 1
                else:
                    # symmetric coherence... try to balance number of unique destinations
                    if hostDests <= deviceDests:
                        hostCut.append(e)
                        if not destInHost:
                            hostDests += 1
                    else:
                        deviceCut.append(e)
                        if not destInDevice:
                            deviceDests += 1
                            
    return hostCut, deviceCut
    
def smartPartition_OLD1(graph, numVertices, numOutgoing, coherenceSymmetry="sym", relativeDevComputeCapability=1.0):
    # sort by destination; with each destination sorted by source
    graph = sorted(graph, key=itemgetter(0))
    graph = sorted(graph, key=itemgetter(1))

    """
    Greedy algorithm to:
    1) Balance number of unique destination vertices towards the coherence-biased
    2) Balance number of edges towards compute-capability
    3) Lower number of shared vertices ?? <- don't know how to optimize for this
    """

    # temporary tuning knob for balancing coherenceSymetry
    # alpha: ratio of Device's unique destination count relative to Host's
    if coherenceSymmetry == "sym":
        alpha = 1.0
    elif coherenceSymmetry == "asym_host":
        alpha = 0.75
    elif coherenceSymmetry == "asym_dev":
        alpha = 1.33

    # balance factor for compute capability between host and device
    deviceBeta = relativeDevComputeCapability / (relativeDevComputeCapability + 1.0)
    hostBeta = 1.0 - deviceBeta
    
    hostDests = 0
    deviceDests = 0
    hostCut = []
    deviceCut = []

    destInHost = False
    destInDevice = False
    for e in graph:
        destInHost, destInDevice = checkDest(e[1], hostCut, deviceCut)

        if (not destInHost) and (not destInDevice):
            # destination in neither host nor device; give to node with less edges; MAY WANT TO CHECK SHARED VERTICES COUNT
            if checkVertex(e[0], hostCut):
                if len(hostCut) <= hostBeta*len(graph):
                    hostCut.append(e)
                else:
                    deviceCut.append(e)
            elif checkVertex(e[0], deviceCut):
                if len(deviceCut) <= deviceBeta*len(graph):
                    deviceCut.append(e)
                else:
                    hostCut.append(e)
            else:
                if len(hostCut) <= relativeDevComputeCapability*float(len(deviceCut)):
                    hostCut.append(e)
                else:
                    deviceCut.append(e)
        elif destInHost and destInDevice:
            # destination in both host and device; give to node with less edges; MAY WANT TO CHECK SHARED VERTICES COUNT
            if checkVertex(e[0], hostCut):
                if len(hostCut) <= hostBeta*len(graph):
                    hostCut.append(e)
                else:
                    deviceCut.append(e)
            elif checkVertex(e[0], deviceCut):
                if len(deviceCut) <= deviceBeta*len(graph):
                    deviceCut.append(e)
                else:
                    hostCut.append(e)
            else:
                if len(hostCut) <= relativeDevComputeCapability*float(len(deviceCut)):
                    hostCut.append(e)
                else:
                    deviceCut.append(e)
        elif destInHost:
            # destination only in host; want to add edge to host IF number of edges balanced
            if len(hostCut) <= hostBeta*len(graph):
                hostCut.append(e)
            else:
                deviceCut.append(e)
        elif destInDevice:
            # destination only in device; want to add edge to device IF number of edges balanced
            if len(deviceCut) <= deviceBeta*len(graph):
                deviceCut.append(e)
            else:
                hostCut.append(e)

    return hostCut, deviceCut

"""
def main():
    graph, numVertices = sampleGraph("large", 100)
    numOutgoing = genOutgoing(graph, numVertices)
    numIncoming = genIncoming(graph, numVertices)
    
    smartPartition(graph, numVertices, numOutgoing, "sym", 1.0)
""" 
#main()
