# File:    gas.py
# Author:  Edward Hanson (eth20@duke.edu)
# Desc.:   Gather-apply-scatter graph decomposition. Generate traces to be used for PowerGraph analysis

import math
from operator import itemgetter
import time
import random
random.seed(3)

COMPUTE_ONCE = 7.152557373046875e-07
FILLER_COMPUTE = 1

# -- These strings are for generating .cpp code for simgrid --
maincode = ""
hostcode = ""
remotecode = ""
devicecode = ""

## IDEA: gather signals/data transfers from Gather and Apply phases individually, then merge into the respective codes
host_g_code = []
host_a_code = []
remote_g_code = []
remote_a_code = []
device_g_code = []
device_a_code = []

# -- Counters to manage get_async() fence prior to execution
hgetcount = 0
dgetcount = 0

prevhget = 0
prevdget = 0

# These counters are for priming the data sends and signal reads
numHSig = 0
#numRSig = 0
numDSig = 0
numHData = 0
numRData = 0
numDData = 0

# -- counters to manage proper code interleaving
hostrecurcount = []
remoterecurcount = []
devicerecurcount = []

# find max item in an arbitary depth of list of lists
def get_max(my_list):
    m = None
    for item in my_list:
        if isinstance(item, list):
            item = get_max(item)
        if not m or m < item:
            m = item
    return m

# Generate sample graph using Adjacency List representation
# outgoing: outgoing edges per vertex
# incoming: incoming edges per vertex
def sampleGraph(name="small", size_chunk=100):

    if name == "large":
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
        
    data = sorted(data, key=itemgetter(1))
    #random.shuffle(data)
    
    # -- Determine number of vertices in graph --
    #numVertices = 30
    #if name == "large":
    #    numVertices = size_chunk*3
    #else:
    numVertices = get_max(data) + 1

    return data, numVertices

# Generate list for number of outgoing edges per node
def genOutgoing(data, numVertices):
    numOutgoing = []
    for i in range(numVertices):
        numOutgoing.append(0)

    for e in data:
        numOutgoing[e[0]] += 1

    #print(numOutgoing)
    return numOutgoing

# Partition given graph across 3 nodes: main, remote, device
# Arguments [main, remote, device] must be floats between [0, 1] representing fraction of data they will hold
def partitionGraph(data, numVertices, main, remote, device):

    assert ((main + remote + device) < 1.1) and ((main + remote + device) > 0.9), "Main + remote + device must sum to 1.0"

    lenMain = int(math.ceil(main * len(data)))
    lenRemote = int(math.ceil(remote * len(data)))
    lenDevice = int(math.ceil(len(data) - (lenMain + lenRemote)))
    
    mainCut   = data[0 : lenMain]
    remoteCut = data[lenMain : lenMain + lenRemote]
    deviceCut = data[lenMain + lenRemote : lenMain + lenRemote + lenDevice]

    # remember to sort by destination! PR() depends on this
    mainCut = sorted(mainCut, key=itemgetter(1))
    remoteCut = sorted(remoteCut, key=itemgetter(1))
    deviceCut = sorted(deviceCut, key=itemgetter(1))
    
    return mainCut, remoteCut, deviceCut

def writeDataTransfers_g(HR_count, RH_count, HD_count, DH_count, DR_count, RD_count):
    global host_g_code
    global remote_g_code
    global device_g_code
    global hgetcount
    global dgetcount
    global prevhget
    global prevdget

    global numHData
    global numRData
    global numDData
    
    # -- Assign data transfers --
    while HR_count > 0:
        #data_size = max(66, HR_count*4)
        #HR_count = 0
        data_size = 66
        HR_count -= 1
        #host_g_code[-1] += "  host_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        numHData += 1
        remote_g_code[-1] += "  XBT_INFO(\"remote recieve " + str(data_size) + " bytes of data from host\");\n"
        remote_g_code[-1] += "  host_data_mailbox->get_async(&data);\n"
        print("WARNING: UNSUPPORTED STATE")
        #hgetcount += 1
    while RH_count > 0:
        #data_size = max(66, RH_count*4)
        #RH_count = 0
        data_size = 66
        RH_count -= 1
        #remote_g_code[-1] += "  remote_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        numRData += 1
        host_g_code[-1] += "  XBT_INFO(\"host receive " + str(data_size) + " bytes of data from remote\");\n"
        host_g_code[-1] += "  simgrid::s4u::CommPtr get" + str(hgetcount) + " = remote_data_mailbox->get_async(&data);\n"
        hgetcount += 1
    while HD_count > 0:
        #data_size = max(66, HD_count*4)
        #HD_count = 0
        data_size = 66
        HD_count -= 1
        #host_g_code[-1] += "  host_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        numHData += 1
        device_g_code[-1] += "  XBT_INFO(\"device receive " + str(data_size) + " bytes of data from host\");\n"
        device_g_code[-1] += "  simgrid::s4u::CommPtr get" + str(dgetcount) + " = host_data_mailbox->get_async(&data);\n"
        dgetcount += 1
    while DH_count > 0:
        #data_size = max(66, DH_count*4)
        #DH_count = 0
        data_size = 66
        DH_count -= 1
        #device_g_code[-1] += "  device_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        numDData += 1
        host_g_code[-1] += "  XBT_INFO(\"host receive " + str(data_size) + " bytes of data from device\");\n"
        host_g_code[-1] += "  simgrid::s4u::CommPtr get" + str(hgetcount) + " = device_data_mailbox->get_async(&data);\n"
        hgetcount += 1
    while DR_count > 0:
        #data_size = max(66, DR_count*4)
        #DR_count = 0
        data_size = 66
        DR_count -= 1
        #device_g_code[-1] += "  device_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        numDData += 1
        remote_g_code[-1] += "  XBT_INFO(\"remote recieve " + str(data_size) + " bytes of data from device\");\n"
        remote_g_code[-1] += "  device_data_mailbox->get_async(&data);\n"
        print("WARNING: UNSUPPORTED STATE")
        #hgetcount += 1
    while RD_count > 0:
        #data_size = max(66, RD_count*4)
        #RD_count = 0
        data_size = 66
        RD_count -= 1
        #remote_g_code[-1] += "  remote_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        numRData += 1
        device_g_code[-1] += "  XBT_INFO(\"device receive " + str(data_size) + " bytes of data from remote\");\n"
        device_g_code[-1] += "  simgrid::s4u::CommPtr get" + str(dgetcount) + " = remote_data_mailbox->get_async(&data);\n"
        dgetcount += 1

def writeDataTransfers_a(HR_count, RH_count, HD_count, DH_count, DR_count, RD_count):
    global host_a_code
    global remote_a_code
    global device_a_code
    global hgetcount
    global dgetcount
    global prevhget
    global prevdget
    
    # -- Assign data transfers --
    while HR_count > 0:
        #data_size = max(66, HR_count*4)
        #HR_count = 0
        data_size = 66
        HR_count -= 1
        host_a_code[-1] += "  host_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        remote_a_code[-1] += "  XBT_INFO(\"remote recieve " + str(data_size) + " bytes of data from host\");\n"
        remote_a_code[-1] += "  host_data_mailbox->get_async(&data);\n"
        print("WARNING: UNSUPPORTED STATE")
        #hgetcount += 1
    while RH_count > 0:
        #data_size = max(66, RH_count*4)
        #RH_count = 0
        data_size = 66
        RH_count -= 1
        remote_a_code[-1] += "  remote_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        host_a_code[-1] += "  XBT_INFO(\"host receive " + str(data_size) + " bytes of data from remote\");\n"
        host_a_code[-1] += "  simgrid::s4u::CommPtr get" + str(hgetcount) + " = remote_data_mailbox->get_async(&data);\n"
        hgetcount += 1
    while HD_count > 0:
        #data_size = max(66, HD_count*4)
        #HD_count = 0
        data_size = 66
        HD_count -= 1
        host_a_code[-1] += "  host_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        device_a_code[-1] += "  XBT_INFO(\"device receive " + str(data_size) + " bytes of data from host\");\n"
        device_a_code[-1] += "  simgrid::s4u::CommPtr get" + str(dgetcount) + " = host_data_mailbox->get_async(&data);\n"
        dgetcount += 1
    while DH_count > 0:
        #data_size = max(66, DH_count*4)
        #DH_count = 0
        data_size = 66
        DH_count -= 1
        device_a_code[-1] += "  device_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        host_a_code[-1] += "  XBT_INFO(\"host receive " + str(data_size) + " bytes of data from device\");\n"
        host_a_code[-1] += "  simgrid::s4u::CommPtr get" + str(hgetcount) + " = device_data_mailbox->get_async(&data);\n"
        hgetcount += 1
    while DR_count > 0:
        #data_size = max(66, DR_count*4)
        #DR_count = 0
        data_size = 66
        DR_count -= 1
        device_a_code[-1] += "  device_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        remote_a_code[-1] += "  XBT_INFO(\"remote recieve " + str(data_size) + " bytes of data from device\");\n"
        remote_a_code[-1] += "  device_data_mailbox->get_async(&data);\n"
        print("WARNING: UNSUPPORTED STATE")
        #hgetcount += 1
    while RD_count > 0:
        #data_size = max(66, RD_count*4)
        #RD_count = 0
        data_size = 66
        RD_count -= 1
        remote_a_code[-1] += "  remote_data_mailbox->put_async(dummy_data, " + str(data_size) + ");\n"
        device_a_code[-1] += "  XBT_INFO(\"device receive " + str(data_size) + " bytes of data from remote\");\n"
        device_a_code[-1] += "  simgrid::s4u::CommPtr get" + str(dgetcount) + " = remote_data_mailbox->get_async(&data);\n"
        dgetcount += 1

# recursive portion of pagerank algorithm
def PR(graph, startIdx, numOutgoing, ranks, done, working, cached_host, cached_remote, cached_device, nodeType, coherenceSymmetry, isRecursion):
    global host_g_code
    global host_a_code
    global remote_g_code
    global remote_a_code
    global device_g_code
    global device_a_code
    global hgetcount
    global dgetcount
    global prevhget
    global prevdget
    global hostrecurcount
    global remoterecurcount
    global devicerecurcount

    global numHSig
    #global numRSig
    global numDSig
    global numHData
    global numRData
    global numDData
    
    host_g_code.append("")
    host_a_code.append("")
    remote_g_code.append("")
    remote_a_code.append("")
    device_g_code.append("")
    device_a_code.append("")

    if nodeType == "host":
        hostrecurcount[-1] += 1
    elif nodeType == "remote":
        remoterecurcount[-1] += 1
    elif nodeType == "device":
        devicerecurcount[-1] += 1
        
    curr = graph[startIdx][1]
    tot = 0
    idx = startIdx

    HR_count = 0
    RH_count = 0
    HD_count = 0
    DH_count = 0
    DR_count = 0
    RD_count = 0

    compute_time = 0
    num_computes = 0
    
    print("Accumulating for vertex {}".format(curr))
    while(idx < len(graph) and graph[idx][1] == curr):
        HD_count = 0
        if nodeType == "host":
            if cached_host[graph[idx][0]] == 'I':
                if cached_device[graph[idx][0]] != 'I': # only send coherence signals for SHARED DATA
                    print("\tTransition vertex {} from 'I' to 'S' state. Send signal HOST->DEVICE".format(graph[idx][0])) ## force all device M to S
                    host_g_code[-1] += "  XBT_INFO(\"host send invalidates\");\n"
                    if coherenceSymmetry == "sym" or coherenceSymmetry == "asym_dev":
                        host_g_code[-1] += "  pending_comms.push_back(device_sig_mailbox->put_async(dummy_data, SNOOP_SIZE));\n"
                        host_g_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(FILLER_COMPUTE) + "); // to avoid segfault\n"
                        numDSig += 1

                else:
                    print("\tTransition vertex {} from 'I' to 'S' state.".format(graph[idx][0]))

                if cached_remote[graph[idx][0]] == 'M':
                    print("\tRemote now at 'S' state. Send data REMOTE->HOST")
                    RH_count += 1
                    cached_remote[graph[idx][0]] = 'S'
                    
                elif cached_device[graph[idx][0]] == 'M':
                    print("\tDevice now at 'S' state. Send data DEVICE->HOST")
                    cached_device[graph[idx][0]] = 'S'
                    
                cached_host[graph[idx][0]] = 'S'
                
        elif nodeType == "remote":
            ## remote memory always ejected from host after computation --> SLIGHTLY INCORRECT BEHAVIOR
            if cached_remote[graph[idx][0]] == 'I':
                if cached_device[graph[idx][0]] != 'I': # only send coherence signals for SHARED DATA
                    print("\tTransition vertex {} from 'I' to 'S' state. Send signal HOST->DEVICE".format(graph[idx][0])) ## force all device M to S
                    host_g_code[-1] += "  XBT_INFO(\"host send invalidates on remote's behalf\");\n"
                    if coherenceSymmetry == "sym" or coherenceSymmetry == "asym_dev":
                        host_g_code[-1] += "  pending_comms.push_back(device_sig_mailbox->put_async(dummy_data, SNOOP_SIZE));\n"
                        host_g_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(FILLER_COMPUTE) + "); // to avoid segfault\n"
                        numDSig += 1

                else:
                    print("\tTransition vertex {} from 'I' to 'S' state.".format(graph[idx][0]))

                if cached_host[graph[idx][0]] == 'M':
                    cached_host[graph[idx][0]] = 'S'
                elif cached_device[graph[idx][0]] == 'M':
                    print("\tDevice now at 'S' state. Send data DEVICE->HOST")
                    DH_count += 1
                    cached_device[graph[idx][0]] = 'S'
                else: # special case for remote
                    print("\tCaching vertex {} from remote memory. Send data REMOTE->HOST".format(graph[idx][0]))
                    RH_count += 1
                cached_remote[graph[idx][0]] = 'S'
            else: # special case for remote
                print("\tCaching vertex {} from remote memory. Send data REMOTE->HOST".format(graph[idx][0]))
                RH_count += 1

        elif nodeType == "device":
            if cached_device[graph[idx][0]] == 'I':
                if cached_host[graph[idx][0]] != 'I' or cached_remote[graph[idx][0]] != 'I':
                    print("\tTransition vertex {} from 'I' to 'S' state. Send signal DEVICE->HOST".format(graph[idx][0])) ## force all other devices and host M to S
                    device_g_code[-1] += "  XBT_INFO(\"device send invalidates\");\n"
                    if coherenceSymmetry == "sym" or coherenceSymmetry == "asym_host":
                        device_g_code[-1] += "  pending_comms.push_back(host_sig_mailbox->put_async(dummy_data, SNOOP_SIZE));\n"
                        device_g_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(FILLER_COMPUTE) + "); // to avoid segfault\n"
                        numHSig += 1

                else:
                    print("\tTransition vertex {} from 'I' to 'S' state.".format(graph[idx][0]))
                    
                if cached_host[graph[idx][0]] == 'M':
                    print("\tHost now at 'S' state. Send data HOST->DEVICE")
                    HD_count += 1
                    cached_host[graph[idx][0]] = 'S'
                if cached_remote[graph[idx][0]] == 'M':
                    print("\tRemote now at 'S' state. Send data REMOTE->DEVICE")
                    RD_count += 1
                    cached_remote[graph[idx][0]] = 'S'
                cached_device[graph[idx][0]] = 'S'

        startTime = time.time()
        tot += ranks[graph[idx][0]] / numOutgoing[graph[idx][0]]
        idx += 1
        compute_time += (time.time() - startTime)
        num_computes += 1
        
    # -- Assign data transfers --
    writeDataTransfers_g(HR_count, RH_count, HD_count, DH_count, DR_count, RD_count)
    HR_count = 0
    RH_count = 0
    HD_count = 0
    DH_count = 0
    DR_count = 0
    RD_count = 0
        
    # grab Modify permissions
    if nodeType == "host":
        if cached_host[curr] != 'M':
            
            if cached_host[curr] == 'S':
                if cached_device[curr] == 'I':
                    print("\tTransition vertex {} from 'S' to 'M' state.".format(curr))
                else:
                    print("\tTransition vertex {} from 'S' to 'M' state. Send invalidate signal HOST->DEVICE".format(curr))
                    host_g_code[-1] += "  XBT_INFO(\"host send invalidates\");\n"
                    if coherenceSymmetry == "sym" or coherenceSymmetry == "asym_dev":
                        host_g_code[-1] += "  pending_comms.push_back(device_sig_mailbox->put_async(dummy_data, SNOOP_SIZE));\n"
                        host_g_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(FILLER_COMPUTE) + "); // to avoid segfault\n"
                        numDSig += 1

            else: # 'I' state
                print("\tTransition vertex {} from 'I' to 'M' state. Send signal HOST->DEVICE and wait for response".format(curr))
                host_g_code[-1] += "  XBT_INFO(\"host send invalidates\");\n"
                if coherenceSymmetry == "sym" or coherenceSymmetry == "asym_dev":
                    host_g_code[-1] += "  pending_comms.push_back(device_sig_mailbox->put_async(dummy_data, SNOOP_SIZE));\n"
                    host_g_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(FILLER_COMPUTE) + "); // to avoid segfault\n"
                    numDSig += 1

                if cached_remote[curr] == 'M':
                    print("\tRemote now at 'I' state. Send data REMOTE->HOST")
                    RH_count += 1
                elif cached_device[curr] == 'M':
                    print("\tDevice now at 'I' state. Send data DEVICE->HOST")
                    DH_count += 1
                elif cached_remote[curr] == 'S':
                    print("\tRemote now at 'I' state. Send data REMOTE->HOST")
                    RH_count += 1
                elif cached_device[curr] == 'S':
                    print("\tRemote now at 'I' state. Send data DEVICE->HOST")
                    DH_count += 1

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
                    host_g_code[-1] += "  XBT_INFO(\"host send invalidates on remote's behalf\");\n"
                    if coherenceSymmetry == "sym" or coherenceSymmetry == "asym_dev":
                        host_g_code[-1] += "  pending_comms.push_back(device_sig_mailbox->put_async(dummy_data, SNOOP_SIZE));\n"
                        host_g_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(FILLER_COMPUTE) + "); // to avoid segfault\n"
                        numDSig += 1

            else: # 'I' state
                if cached_host[curr] == 'I':
                    print("\tTransition vertex {} from 'I' to 'M' state. Send signal HOST->DEVICE".format(curr))
                    host_g_code[-1] += "  XBT_INFO(\"host send invalidates on remote's behalf\");\n"
                    if coherenceSymmetry == "sym" or coherenceSymmetry == "asym_dev":
                        host_g_code[-1] += "  pending_comms.push_back(device_sig_mailbox->put_async(dummy_data, SNOOP_SIZE));\n"
                        host_g_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(FILLER_COMPUTE) + "); // to avoid segfault\n"
                        numDSig += 1

                    print(cached_host)
                else:
                    print("\tTransition vertex {} from 'I' to 'M' state.".format(curr))
                    
                if cached_host[curr] == 'M':
                    print("\tHost now at 'I' state.")
                elif cached_device[curr] == 'M':
                    print("\tDevice now at 'I' state. Send data DEVICE->HOST")
                    DH_count += 1
                elif cached_host[curr] == 'S':
                    print("\tHost now at 'I' state.")
                elif cached_device[curr] == 'S':
                    print("\tRemote now at 'I' state. Send data DEVICE->HOST")
                    DH_count += 1
                else: # special state only for remote
                    print("\tRetrieve vertex {} from remote memory. Send data REMOTE->HOST".format(curr))
                    RH_count += 1

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
                    device_g_code[-1] += "  XBT_INFO(\"device send invalidates\");\n"
                    if coherenceSymmetry == "sym" or coherenceSymmetry == "asym_host":
                        device_g_code[-1] += "  pending_comms.push_back(host_sig_mailbox->put_async(dummy_data, SNOOP_SIZE));\n"
                        device_g_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(FILLER_COMPUTE) + "); // to avoid segfault\n"
                        numHSig += 1

            else: # 'I' state
                print("\tTransition vertex {} from 'I' to 'M' state. Send signal DEVICE->HOST".format(curr))
                device_g_code[-1] += "  XBT_INFO(\"device send invalidates\");\n"
                if coherenceSymmetry == "sym" or coherenceSymmetry == "asym_host":
                    device_g_code[-1] += "  pending_comms.push_back(host_sig_mailbox->put_async(dummy_data, SNOOP_SIZE));\n"
                    device_g_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(FILLER_COMPUTE) + "); // to avoid segfault\n"
                    numHSig += 1

                if cached_host[curr] == 'M':
                    print("\tHost now at 'I' state. Send data HOST->DEVICE") ### this and below are potential gridlock point
                    HD_count += 1
                elif cached_remote[curr] == 'M':
                    print("\tRemote now at 'I' state. Send data REMOTE->DEVICE") ### this and above are potential gridlock point
                    RD_count += 1
                elif cached_host[curr] == 'S':
                    print("\tHost now at 'I' state. Send data HOST->DEVICE") ### this and below are potential gridlock point
                    HD_count += 1
                elif cached_remote[curr] == 'S':
                    print("\tRemote now at 'I' state. Send data REMOTE->DEVICE") ### this and above are potential gridlock point
                    RD_count += 1

            cached_host[curr] = 'I'
            cached_remote[curr] = 'I'
            cached_device[curr] = 'M'

    print("COMPUTE: Update rank of vertex {}".format(curr))
    startTime = time.time()
    if not working[curr]:
        ranks[curr] = tot
        working[curr] = True
    else:
        ranks[curr] += tot
    done[curr] = True
    compute_time += (time.time() - startTime)
    num_computes += 1
    
    # -- Assign data transfers and compute --
    writeDataTransfers_g(HR_count, RH_count, HD_count, DH_count, DR_count, RD_count)
    
    # -- Assign compute execution --
    #cost = compute_time * 10e9
    cost = num_computes * COMPUTE_ONCE * 10e9
    if nodeType == "host":
        host_a_code[-1] += "\n"
        for i in range(prevhget, hgetcount):
            host_a_code[-1] += "  get" + str(i) + "->wait();\n"
        prevhget = hgetcount;
        host_a_code[-1] += "  simgrid::s4u::Comm::wait_all(&pending_comms);\n"
        host_a_code[-1] += "  pending_comms.clear();\n"
        host_a_code[-1] += "\n  XBT_INFO(\"host perform computation\");\n"
        host_a_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(int(cost)) + ");\n\n"
    elif nodeType == "remote":
        host_a_code[-1] += "\n"
        for i in range(prevhget, hgetcount):
            host_a_code[-1] += "\n  get" + str(i) + "->wait();\n"
        prevhget = hgetcount;
        host_a_code[-1] += "  simgrid::s4u::Comm::wait_all(&pending_comms);\n"
        host_a_code[-1] += "  pending_comms.clear();\n"
        host_a_code[-1] += "\n  XBT_INFO(\"host perform computation on remote's behalf\");\n"
        host_a_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(int(cost)) + ");\n\n"
    elif nodeType == "device":
        device_a_code[-1] += "\n"
        for i in range(prevdget, dgetcount):
            device_a_code[-1] += "\n  get" + str(i) + "->wait();\n"
        prevdget = dgetcount;
        device_a_code[-1] += "  simgrid::s4u::Comm::wait_all(&pending_comms);\n"
        device_a_code[-1] += "  pending_comms.clear();\n"
        device_a_code[-1] += "\n  XBT_INFO(\"device perform computation\");\n"
        device_a_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(int(cost)) + ");\n\n"

    # prevent max recursion depth issue
    if isRecursion:
        return ranks, working, cached_host, cached_remote, cached_device
        
    #for out in range(idx,len(graph)):
    for out in range(0,len(graph)):
        if graph[out][0] == curr and not done[graph[out][1]]:
            print("Scatter execution to vertex {}".format(graph[out][1]))
            ranks, working, cached_host, cached_remote, cached_device = PR(graph, out, numOutgoing, ranks, done, working, cached_host, cached_remote, cached_device, nodeType, coherenceSymmetry, True)
    for out in range(0,len(graph)):
        if not done[graph[out][1]]:
             print("Scatter chain broken, searching for more unfinished vertices... {}".format(graph[out][1]))
             ranks, working, cached_host, cached_remote, cached_device = PR(graph, out, numOutgoing, ranks, done, working, cached_host, cached_remote, cached_device, nodeType, coherenceSymmetry, True)
            
    return ranks, working, cached_host, cached_remote, cached_device

# apply pagerank algorithm
def PageRank(ranks, numVertices, working, cached_host, cached_remote, cached_device, nodeType, coherenceSymmetry, graph, numOutgoing, maxIter):

    # check for empty graph
    if len(graph) == 0:
        return ranks, working, cached_host, cached_remote, cached_device

    # 'done' is local to each partition
    done = []
    for i in range(numVertices):
        done.append(False)
    
    # assuming graph is sorted by DESTINATION ...
    startIdx = 0
    ranks, working, cached_host, cached_remote, cached_device = PR(graph, startIdx, numOutgoing, ranks, done, working, cached_host, cached_remote, cached_device, nodeType, coherenceSymmetry, False)

    return ranks, working, cached_host, cached_remote, cached_device

def generatePrefix(coherenceSymmetry):
    global maincode
    global hostcode
    global remotecode
    global devicecode
    
    maincode += "/*AUTOMATICALLY GENERATED CODE*/\n/*Coherence symmetry mode: "
    maincode += coherenceSymmetry + "*/\n\n#include <simgrid/s4u.hpp>\n\nXBT_LOG_NEW_DEFAULT_CATEGORY(s4u_app_masterworker, \"Messages specific for this example\");\n\n"
    maincode += "#define FLIT_SIZE 64\n#define SNOOP_SIZE 64\n\n"
    maincode += "simgrid::s4u::BarrierPtr barrier = simgrid::s4u::Barrier::create(3);\n"
    maincode += "double dummy_cost = 10;\ndouble* dummy_data = &dummy_cost;\n\n"
    maincode += "void checkAndRec(simgrid::s4u::Mailbox* mailbox) {\n  if (!mailbox->empty()) {\n    double* signal = static_cast<double*>(mailbox->get());\n    delete signal;\n  }\n}\n\n"
    maincode += "void checkAndSend(simgrid::s4u::Mailbox* mailbox) {\n  if (!mailbox->empty()) {\n    mailbox->put_async(dummy_data, FLIT_SIZE);\n  }\n}\n\n"

    hostcode += "static void host(std::vector<std::string> args) {\n  xbt_assert(args.size() == 1, \"The host expects no argument.\");\n  simgrid::s4u::Mailbox* host_data_mailbox     = simgrid::s4u::Mailbox::by_name(\"host_data\");\n  simgrid::s4u::Mailbox* remote_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"remote_data\");\n  simgrid::s4u::Mailbox* device_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"device_data\");\n  simgrid::s4u::Mailbox* host_sig_mailbox      = simgrid::s4u::Mailbox::by_name(\"host_sig\");\n  simgrid::s4u::Mailbox* device_sig_mailbox    = simgrid::s4u::Mailbox::by_name(\"device_sig\");\n  std::vector<simgrid::s4u::CommPtr> pending_comms;\n  std::vector<simgrid::s4u::CommPtr> noncrit_comms;\n\n  void* data = NULL;\n  void* signal = NULL;\n\n"

    remotecode += "static void remote(std::vector<std::string> args) {\n  xbt_assert(args.size() == 1, \"The remote expects no argument.\");\n  simgrid::s4u::Mailbox* host_data_mailbox     = simgrid::s4u::Mailbox::by_name(\"host_data\");\n  simgrid::s4u::Mailbox* remote_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"remote_data\");\n  simgrid::s4u::Mailbox* device_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"device_data\");\n  simgrid::s4u::Mailbox* host_sig_mailbox      = simgrid::s4u::Mailbox::by_name(\"host_sig\");\n  simgrid::s4u::Mailbox* device_sig_mailbox    = simgrid::s4u::Mailbox::by_name(\"device_sig\");\n  std::vector<simgrid::s4u::CommPtr> pending_comms;\n  std::vector<simgrid::s4u::CommPtr> noncrit_comms;\n\n  void* data = NULL;\n  void* signal = NULL;\n\n"

    devicecode += "static void device(std::vector<std::string> args) {\n  xbt_assert(args.size() == 1, \"The device expects no argument.\");\n  simgrid::s4u::Mailbox* host_data_mailbox     = simgrid::s4u::Mailbox::by_name(\"host_data\");\n  simgrid::s4u::Mailbox* remote_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"remote_data\");\n  simgrid::s4u::Mailbox* device_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"device_data\");\n  simgrid::s4u::Mailbox* host_sig_mailbox      = simgrid::s4u::Mailbox::by_name(\"host_sig\");\n  simgrid::s4u::Mailbox* device_sig_mailbox    = simgrid::s4u::Mailbox::by_name(\"device_sig\");\n  std::vector<simgrid::s4u::CommPtr> pending_comms;\n  std::vector<simgrid::s4u::CommPtr> noncrit_comms;\n\n  void* data = NULL;\n  void* signal = NULL;\n\n"

def generateSuffix(coherenceSymmetry):
    global maincode
    global hostcode
    global remotecode
    global devicecode

    global host_g_code
    global host_a_code
    global remote_g_code
    global remote_a_code
    global device_g_code
    global device_a_code

    global hostrecurcount
    global remoterecurcount
    global devicerecurcount

    global numHSig
    #global numRSig
    global numDSig
    global numHData
    global numRData
    global numDData
    
    print("Counts: ")
    print(hostrecurcount)
    print(remoterecurcount)
    print(devicerecurcount)
    #print(host_a_code);
    #print(remote_a_code);
    #print(device_a_code);

    # Prime the signal reads and data sends
    data_size = 66
    for i in range(numHSig):
        hostcode += "  noncrit_comms.push_back(host_sig_mailbox->get_async(&signal));\n"
    for i in range(numHSig, numHData + numHSig):
        hostcode += "  noncrit_comms.push_back(host_data_mailbox->put_async(dummy_data, " + str(data_size) + "));\n"
    for i in range(numDSig):
        devicecode += "  noncrit_comms.push_back(device_sig_mailbox->get_async(&signal));\n"
    for i in range(numDSig, numDData + numDSig):
        devicecode += "  noncrit_comms.push_back(device_data_mailbox->put_async(dummy_data, " + str(data_size) + "));\n"
    for i in range(numRData):
        remotecode += "  noncrit_comms.push_back(remote_data_mailbox->put_async(dummy_data, " + str(data_size) + "));\n"
        
    for iteration in range(len(hostrecurcount)):
        if iteration == 0:
            offset = 0
        else:
            offset = sum(hostrecurcount[:iteration]) + sum(remoterecurcount[:iteration]) + sum(devicerecurcount[:iteration])

        if coherenceSymmetry == "asym_dev":
            for i in range(max([hostrecurcount[iteration], remoterecurcount[iteration], devicerecurcount[iteration]])):
                if i < devicerecurcount[iteration]:
                    hostcode += host_g_code[i + 0 + offset]
                    remotecode += remote_g_code[i + 0 + offset]
                    devicecode += device_g_code[i + 0 + offset]
                    #print(i + offset)
                if i < hostrecurcount[iteration]:
                    hostcode += host_g_code[i + devicerecurcount[iteration] + offset]
                    remotecode += remote_g_code[i + devicerecurcount[iteration] + offset]
                    devicecode += device_g_code[i + devicerecurcount[iteration] + offset]
                    #print(i+devicerecurcount[iteration] + offset)
                if i < remoterecurcount[iteration]:
                    hostcode += host_g_code[i + hostrecurcount[iteration] + devicerecurcount[iteration] + offset]
                    remotecode += remote_g_code[i + hostrecurcount[iteration] + devicerecurcount[iteration] + offset]
                    devicecode += device_g_code[i + hostrecurcount[iteration] + devicecurcount[iteration] + offset]
                    #print(i+hostrecurcount[iteration]+remoterecurcount[iteration] + offset)
                
                if i < devicerecurcount[iteration]:
                    devicecode += device_a_code[i + 0 + offset]
                if i < hostrecurcount[iteration]:
                    hostcode += host_a_code[i + devicerecurcount[iteration] + offset]
                if i < remoterecurcount[iteration]:
                    remotecode += remote_a_code[i + devicerecurcount[iteration] + hostrecurcount[iteration] + offset]

        else:
            for i in range(max([hostrecurcount[iteration], remoterecurcount[iteration], devicerecurcount[iteration]])):
                if i < hostrecurcount[iteration]:
                    hostcode += host_g_code[i + 0 + offset]
                    remotecode += remote_g_code[i + 0 + offset]
                    devicecode += device_g_code[i + 0 + offset]
                    #print(i + offset)
                if i < remoterecurcount[iteration]:
                    hostcode += host_g_code[i + hostrecurcount[iteration] + offset]
                    remotecode += remote_g_code[i + hostrecurcount[iteration] + offset]
                    devicecode += device_g_code[i + hostrecurcount[iteration] + offset]
                    #print(i+hostrecurcount[iteration] + offset)
                if i < devicerecurcount[iteration]:
                    hostcode += host_g_code[i + hostrecurcount[iteration] + remoterecurcount[iteration] + offset]
                    remotecode += remote_g_code[i + hostrecurcount[iteration] + remoterecurcount[iteration] + offset]
                    devicecode += device_g_code[i + hostrecurcount[iteration] + remoterecurcount[iteration] + offset]
                    #print(i+hostrecurcount[iteration]+remoterecurcount[iteration] + offset)
                    
                if i < hostrecurcount[iteration]:
                    hostcode += host_a_code[i + 0 + offset]
                if i < remoterecurcount[iteration]:
                    remotecode += remote_a_code[i + hostrecurcount[iteration] + offset]
                if i < devicerecurcount[iteration]:
                    devicecode += device_a_code[i + hostrecurcount[iteration] + remoterecurcount[iteration] + offset]
    
    hostcode += "\n  XBT_INFO(\"Host waiting on the barrier\");\n  simgrid::s4u::Comm::wait_all(&noncrit_comms);\n  barrier->wait();\n"
    hostcode += "  XBT_INFO(\"Host exiting.\");\n}"
    remotecode += "\n  XBT_INFO(\"Remote waiting on the barrier\");\n  simgrid::s4u::Comm::wait_all(&noncrit_comms);\n  barrier->wait();\n"
    remotecode += "  XBT_INFO(\"Remote exiting.\");\n}"
    devicecode += "\n  XBT_INFO(\"Device waiting on the barrier\");\n  simgrid::s4u::Comm::wait_all(&noncrit_comms);\n  barrier->wait();\n"
    devicecode += "  XBT_INFO(\"Device exiting.\");\n}"

    maincode += "\n"
    maincode += hostcode
    maincode += "\n\n"
    maincode += remotecode
    maincode += "\n\n"
    maincode += devicecode
    maincode += "\n\nint main(int argc, char* argv[]) {\n  simgrid::s4u::Engine e(&argc, argv);\n  xbt_assert(argc > 2, \"Usage: %s platform_file deployment_file\", argv[0]);\n\n  /* Register the functions representing the actors */\n  e.register_function(\"host\", &host);\n  e.register_function(\"remote\", &remote);\n  e.register_function(\"device\", &device);\n\n  /* Load the platform description and then deploy the application */\n  e.load_platform(argv[1]);\n  e.load_deployment(argv[2]);\n\n  /* Run the simulation */\n  // Functions automatically end after completing tasks\n  e.run();\n\n  XBT_INFO(\"Simulation is over\");\n\n  return 0;\n}"

    f = open("/mnt/1tb/projects/simgrid-v3.25/examples/s4u/app-masterworkers/s4u-app-masterworkers-fun.cpp","w")
    f.write(maincode)
    f.close()
    
def main():
    global hostrecurcount
    global remoterecurcount
    global devicerecurcount

    #coherenceSymmetry = "none"
    #coherenceSymmetry = "sym"
    #coherenceSymmetry = "asym_host"
    coherenceSymmetry = "asym_dev"
    
    # start generating .cpp file
    generatePrefix(coherenceSymmetry)
    
    graph, numVertices = sampleGraph("large", 100)
    #print(graph)
    numOutgoing = genOutgoing(graph, numVertices)

    # -- Different workload cuts: evenly distributed, all host, all remote, all device --
    #mainCut, remoteCut, deviceCut = partitionGraph(graph, numVertices, 1.0/3.0, 1.0/3.0, 1.0/3.0)
    mainCut, remoteCut, deviceCut = partitionGraph(graph, numVertices, 1.0/2.0, 0.0, 1.0/2.0)
    #mainCut, remoteCut, deviceCut = partitionGraph(graph, numVertices, 1.0, 0, 0)
    #mainCut, remoteCut, deviceCut = partitionGraph(graph, numVertices, 0, 1.0, 0)
    #mainCut, remoteCut, deviceCut = partitionGraph(graph, numVertices, 0, 0, 1.0)

    print(mainCut)
    
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
        
    numIter = 1
    for x in range(numIter):
        hostrecurcount.append(0)
        remoterecurcount.append(0)
        devicerecurcount.append(0)
        working = []
        for i in range(numVertices):
            working.append(False)

        if coherenceSymmetry == "asym_dev":
            print("--- Device ---")
            ranks, working, cached_host, cached_remote, cached_device = PageRank(ranks, numVertices, working, cached_host, cached_remote, cached_device, "device", coherenceSymmetry, deviceCut, numOutgoing, 1)
            
        print("--- Host ---")
        ranks, working, cached_host, cached_remote, cached_device = PageRank(ranks, numVertices, working, cached_host, cached_remote, cached_device, "host", coherenceSymmetry, mainCut, numOutgoing, 1)
        print("--- Remote ---")
        ranks, working, cached_host, cached_remote, cached_device = PageRank(ranks, numVertices, working, cached_host, cached_remote, cached_device, "remote", coherenceSymmetry, remoteCut, numOutgoing, 1)
        if coherenceSymmetry != "asym_dev":
            print("--- Device ---")
            ranks, working, cached_host, cached_remote, cached_device = PageRank(ranks, numVertices, working, cached_host, cached_remote, cached_device, "device", coherenceSymmetry, deviceCut, numOutgoing, 1)
        
        #print(ranks)

    """
    print(mainCut)
    print(remoteCut)
    print(deviceCut)
    #"""

    # Complete .cpp file generation
    generateSuffix(coherenceSymmetry)

    # --- number of distinct vertices on each cut ---
    mainSet = set(i for j in mainCut for i in j)
    deviceSet = set(i for j in deviceCut for i in j)
    print("Number of distinct vertices in Host: {}".format(len(mainSet)))
    print("Number of distinct vertices in Device: {}".format(len(deviceSet)))

    intersectS = mainSet.intersection(deviceSet)
    unionS = mainSet.union(deviceSet)
    print("Ratio of shared vertices between Host and Device: {}".format(float(len(intersectS)) / len(unionS)))

    # --- number of distinct destinations on each cut ---
    mainDest = set([i[1] for i in mainCut])
    deviceDest = set([i[1] for i in deviceCut])
    print("Number of distinct destinations in Host: {}".format(len(mainDest)))
    print("Number of distinct destinations in Device: {}".format(len(deviceDest)))

    intersectD = mainDest.intersection(deviceDest)
    unionD = mainDest.union(deviceDest)
    print("Ratio of shared destinations between Host and Device:{}".format(float(len(intersectD)) / len(unionD)))

    print("Number of edges in Host: {}".format(len(mainCut)))
    print("Number of edges in Device: {}".format(len(deviceCut)))
    
if __name__ == "__main__":
    main()
    
####### NOTES:
# Gather should be done across all edges IN PARALLEL -> maximize CPU performance
