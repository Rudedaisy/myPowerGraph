# File:    gas.py
# Author:  Edward Hanson (eth20@duke.edu)
# Desc.:   Gather-apply-scatter graph decomposition. Generate traces to be used for PowerGraph analysis

import math
from operator import itemgetter
import time

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

def writeDataTransfers_g(HR_count, RH_count, HD_count, DH_count, DR_count, RD_count):
    global host_g_code
    global remote_g_code
    global device_g_code

    # -- Assign data transfers --
    while HR_count > 0:
        data_size = max(66, HR_count*4)
        HR_count = 0
        host_g_code[-1] += str("  host_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        remote_g_code[-1] += str("  XBT_INFO(\"remote recieve " + str(data_size) + " bytes of data from host\");\n")
        remote_g_code[-1] += str("  data = static_cast<double*>(host_data_mailbox->get());\n")
        remote_g_code[-1] += str("  delete data;\n")
    while RH_count > 0:
        data_size = max(66, RH_count*4)
        RH_count = 0
        remote_g_code[-1] += str("  remote_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        host_g_code[-1] += str("  XBT_INFO(\"host receive " + str(data_size) + " bytes of data from remote\");\n")
        host_g_code[-1] += str("  data = static_cast<double*>(remote_data_mailbox->get());\n")
        host_g_code[-1] += str("  delete data;\n")
    while HD_count > 0:
        data_size = max(66, HD_count*4)
        HD_count = 0
        host_g_code[-1] += str("  host_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        device_g_code[-1] += str("  XBT_INFO(\"device receive " + str(data_size) + " bytes of data from host\");\n")
        device_g_code[-1] += str("  data = static_cast<double*>(host_data_mailbox->get());\n")
        device_g_code[-1] += str("  delete data;\n")
    while DH_count > 0:
        data_size = max(66, DH_count*4)
        DH_count = 0
        device_g_code[-1] += str("  device_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        host_g_code[-1] += str("  XBT_INFO(\"host receive " + str(data_size) + " bytes of data from device\");\n")
        host_g_code[-1] += str("  data = static_cast<double*>(device_data_mailbox->get());\n")
        host_g_code[-1] += str("  delete data;\n")
    while DR_count > 0:
        data_size = max(66, DR_count*4)
        DR_count = 0
        device_g_code[-1] += str("  device_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        remote_g_code[-1] += str("  XBT_INFO(\"remote recieve " + str(data_size) + " bytes of data from device\");\n")
        remote_g_code[-1] += str("  data = static_cast<double*>(device_data_mailbox->get());\n")
        remote_g_code[-1] += str("  delete data;\n")
    while RD_count > 0:
        data_size = max(66, RD_count*4)
        RD_count = 0
        remote_g_code[-1] += str("  remote_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        device_g_code[-1] += str("  XBT_INFO(\"device receive " + str(data_size) + " bytes of data from remote\");\n")
        device_g_code[-1] += str("  data = static_cast<double*>(remote_data_mailbox->get());\n")
        device_g_code[-1] += str("  delete data;\n")

def writeDataTransfers_a(HR_count, RH_count, HD_count, DH_count, DR_count, RD_count):
    global host_a_code
    global remote_a_code
    global device_a_code
    
    # -- Assign data transfers --
    while HR_count > 0:
        data_size = max(66, HR_count*4)
        HR_count = 0
        host_a_code[-1] += str("  host_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        remote_a_code[-1] += str("  XBT_INFO(\"remote recieve " + str(data_size) + " bytes of data from host\");\n")
        remote_a_code[-1] += str("  data = static_cast<double*>(host_data_mailbox->get());\n")
        remote_a_code[-1] += str("  delete data;\n")
    while RH_count > 0:
        data_size = max(66, RH_count*4)
        RH_count = 0
        remote_a_code[-1] += str("  remote_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        host_a_code[-1] += str("  XBT_INFO(\"host receive " + str(data_size) + " bytes of data from remote\");\n")
        host_a_code[-1] += str("  data = static_cast<double*>(remote_data_mailbox->get());\n")
        host_a_code[-1] += str("  delete data;\n")
    while HD_count > 0:
        data_size = max(66, HD_count*4)
        HD_count = 0
        host_a_code[-1] += str("  host_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        device_a_code[-1] += str("  XBT_INFO(\"device receive " + str(data_size) + " bytes of data from host\");\n")
        device_a_code[-1] += str("  data = static_cast<double*>(host_data_mailbox->get());\n")
        device_a_code[-1] += str("  delete data;\n")
    while DH_count > 0:
        data_size = max(66, DH_count*4)
        DH_count = 0
        device_a_code[-1] += str("  device_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        host_a_code[-1] += str("  XBT_INFO(\"host receive " + str(data_size) + " bytes of data from device\");\n")
        host_a_code[-1] += str("  data = static_cast<double*>(device_data_mailbox->get());\n")
        host_a_code[-1] += str("  delete data;\n")
    while DR_count > 0:
        data_size = max(66, DR_count*4)
        DR_count = 0
        device_a_code[-1] += str("  device_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        remote_a_code[-1] += str("  XBT_INFO(\"remote recieve " + str(data_size) + " bytes of data from device\");\n")
        remote_a_code[-1] += str("  data = static_cast<double*>(device_data_mailbox->get());\n")
        remote_a_code[-1] += str("  delete data;\n")
    while RD_count > 0:
        data_size = max(66, RD_count*4)
        RD_count = 0
        remote_a_code[-1] += str("  remote_data_mailbox->put(new double(dummy_cost), " + str(data_size) + ");\n")
        device_a_code[-1] += str("  XBT_INFO(\"device receive " + str(data_size) + " bytes of data from remote\");\n")
        device_a_code[-1] += str("  data = static_cast<double*>(remote_data_mailbox->get());\n")
        device_a_code[-1] += str("  delete data;\n")

# recursive portion of pagerank algorithm
def PR(graph, startIdx, numOutgoing, ranks, done, working, cached_host, cached_remote, cached_device, nodeType):
    global host_g_code
    global host_a_code
    global remote_g_code
    global remote_a_code
    global device_g_code
    global device_a_code

    host_g_code.append("")
    host_a_code.append("")
    remote_g_code.append("")
    remote_a_code.append("")
    device_g_code.append("")
    device_a_code.append("")
    
    
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
    
    print("Accumulating for vertex {}".format(curr))
    while(idx < len(graph) and graph[idx][1] == curr):
        HD_count = 0
        if nodeType == "host":
            if cached_host[graph[idx][0]] == 'I':
                if cached_device[graph[idx][0]] != 'I': # only send coherence signals for SHARED DATA
                    print("\tTransition vertex {} from 'I' to 'S' state. Send signal HOST->DEVICE".format(graph[idx][0])) ## force all device M to S
                    host_g_code[-1] += "  XBT_INFO(\"host send invalidates\");\n"
                    host_g_code[-1] += "  device_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);\n"
                    device_g_code[-1] += "  signal = static_cast<double*>(device_sig_mailbox->get()); //1\n"
                    device_g_code[-1] += "  delete signal;\n"
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
                    host_g_code[-1] += "  device_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);\n"
                    device_g_code[-1] += "  signal = static_cast<double*>(device_sig_mailbox->get()) //2;\n"
                    device_g_code[-1] += "  delete signal;\n"
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
                    device_g_code[-1] += "  host_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);\n"
                    host_g_code[-1] += "  signal = static_cast<double*>(host_sig_mailbox->get());\n"
                    host_g_code[-1] += "  delete signal;\n"
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
                    host_a_code[-1] += "  XBT_INFO(\"host send invalidates\");\n"
                    host_a_code[-1] += "  device_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);\n"
                    device_a_code[-1] += "  signal = static_cast<double*>(device_sig_mailbox->get()); //3\n"
                    device_a_code[-1] += "  delete signal;\n"
            else: # 'I' state
                print("\tTransition vertex {} from 'I' to 'M' state. Send signal HOST->DEVICE and wait for response".format(curr))
                host_a_code[-1] += "  XBT_INFO(\"host send invalidates\");\n"
                host_a_code[-1] += "  device_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);\n"
                device_a_code[-1] += "  signal = static_cast<double*>(device_sig_mailbox->get()); //4\n"
                device_a_code[-1] += "  delete signal;\n"
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
                    host_a_code[-1] += "  XBT_INFO(\"host send invalidates on remote's behalf\");\n"
                    host_a_code[-1] += "  device_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);\n"
                    device_a_code[-1] += "  signal = static_cast<double*>(device_sig_mailbox->get()); //5\n"
                    device_a_code[-1] += "  delete signal;\n"
            else: # 'I' state
                if cached_host[curr] == 'I':
                    print("\tTransition vertex {} from 'I' to 'M' state. Send signal HOST->DEVICE".format(curr))
                    host_a_code[-1] += "  XBT_INFO(\"host send invalidates on remote's behalf\");\n"
                    host_a_code[-1] += "  device_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);\n"
                    device_a_code[-1] += "  signal = static_cast<double*>(device_sig_mailbox->get()); //6\n"
                    device_a_code[-1] += "  delete signal;\n"
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
                    device_a_code[-1] += "  XBT_INFO(\"device send invalidates\");\n"
                    device_a_code[-1] += "  host_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);\n"
                    host_a_code[-1] += "  signal = static_cast<double*>(host_sig_mailbox->get());\n"
                    host_a_code[-1] += "  delete signal;\n"
            else: # 'I' state
                print("\tTransition vertex {} from 'I' to 'M' state. Send signal DEVICE->HOST".format(curr))
                device_a_code[-1] += "  XBT_INFO(\"device send invalidates\");\n"
                device_a_code[-1] += "  host_sig_mailbox->put(new double(dummy_cost), SNOOP_SIZE);\n"
                host_a_code[-1] += "  signal = static_cast<double*>(host_sig_mailbox->get());\n"
                host_a_code[-1] += "  delete signal;\n"
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

    # -- Assign compute execution --
    cost = compute_time * 10e9
    if nodeType == "host":
        host_a_code[-1] += "\n  XBT_INFO(\"host perform computation\");\n"
        host_a_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(cost) + ");\n\n"
    elif nodeType == "remote":
        host_a_code[-1] += "\n  XBT_INFO(\"host perform computation on remote's behalf\");\n"
        host_a_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(cost) + ");\n\n"
    elif nodeType == "device":
        device_a_code[-1] += "\n  XBT_INFO(\"device perform computation\");\n"
        device_a_code[-1] += "  simgrid::s4u::this_actor::execute(" + str(cost) + ");\n\n"

    # -- Assign data transfers and compute --
    writeDataTransfers_a(HR_count, RH_count, HD_count, DH_count, DR_count, RD_count)
    
    for out in range(idx,len(graph)):
        if graph[out][0] == curr and not done[graph[out][1]]:
            print("Scatter execution to vertex {}".format(graph[out][1]))
            ranks, working, cached_host, cached_remote, cached_device = PR(graph, out, numOutgoing, ranks, done, working, cached_host, cached_remote, cached_device, nodeType)

    return ranks, working, cached_host, cached_remote, cached_device

# apply pagerank algorithm
def PageRank(ranks, numVertices, working, cached_host, cached_remote, cached_device, nodeType, graph, numOutgoing, maxIter):

    # check for empty graph
    if len(graph) == 0:
        return ranks, working, cached_host, cached_remote, cached_device

    # 'done' is local to each partition
    done = []
    for i in range(numVertices):
        done.append(False)
    
    # assuming graph is sorted by DESTINATION ...
    startIdx = 0
    ranks, working, cached_host, cached_remote, cached_device = PR(graph, startIdx, numOutgoing, ranks, done, working, cached_host, cached_remote, cached_device, nodeType)

    return ranks, working, cached_host, cached_remote, cached_device

def generatePrefix():
    global maincode
    global hostcode
    global remotecode
    global devicecode
    
    maincode += "/*AUTOMATICALLY GENERATED CODE*/\n\n#include <simgrid/s4u.hpp>\n\nXBT_LOG_NEW_DEFAULT_CATEGORY(s4u_app_masterworker, \"Messages specific for this example\");\n\n"
    maincode += "#define FLIT_SIZE 64\n#define SNOOP_SIZE 64\n#define dummy_cost 10\n\n"
    maincode += "void checkAndRec(simgrid::s4u::Mailbox* mailbox) {\n  if (!mailbox->empty()) {\n    double* signal = static_cast<double*>(mailbox->get());\n    delete signal;\n  }\n}\n\n"
    maincode += "void checkAndSend(simgrid::s4u::Mailbox* mailbox) {\n  if (!mailbox->empty()) {\n    mailbox->put(new double(dummy_cost), FLIT_SIZE);\n  }\n}\n\n"

    hostcode += "static void host(std::vector<std::string> args) {\n  xbt_assert(args.size() == 1, \"The host expects no argument.\");\n  simgrid::s4u::Mailbox* host_data_mailbox     = simgrid::s4u::Mailbox::by_name(\"host_data\");\n  simgrid::s4u::Mailbox* remote_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"remote_data\");\n  simgrid::s4u::Mailbox* device_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"device_data\");\n  simgrid::s4u::Mailbox* host_sig_mailbox      = simgrid::s4u::Mailbox::by_name(\"host_sig\");\n  simgrid::s4u::Mailbox* device_sig_mailbox    = simgrid::s4u::Mailbox::by_name(\"device_sig\");\n\ndouble* data = NULL;\ndouble* signal = NULL;\n\n"

    remotecode += "static void remote(std::vector<std::string> args) {\n  xbt_assert(args.size() == 1, \"The remote expects no argument.\");\n  simgrid::s4u::Mailbox* host_data_mailbox     = simgrid::s4u::Mailbox::by_name(\"host_data\");\n  simgrid::s4u::Mailbox* remote_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"remote_data\");\n  simgrid::s4u::Mailbox* device_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"device_data\");\n  simgrid::s4u::Mailbox* host_sig_mailbox      = simgrid::s4u::Mailbox::by_name(\"host_sig\");\n  simgrid::s4u::Mailbox* device_sig_mailbox    = simgrid::s4u::Mailbox::by_name(\"device_sig\");\n\ndouble* data = NULL;\ndouble* signal = NULL;\n\n"

    devicecode += "static void device(std::vector<std::string> args) {\n  xbt_assert(args.size() == 1, \"The device expects no argument.\");\n  simgrid::s4u::Mailbox* host_data_mailbox     = simgrid::s4u::Mailbox::by_name(\"host_data\");\n  simgrid::s4u::Mailbox* remote_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"remote_data\");\n  simgrid::s4u::Mailbox* device_data_mailbox   = simgrid::s4u::Mailbox::by_name(\"device_data\");\n  simgrid::s4u::Mailbox* host_sig_mailbox      = simgrid::s4u::Mailbox::by_name(\"host_sig\");\n  simgrid::s4u::Mailbox* device_sig_mailbox    = simgrid::s4u::Mailbox::by_name(\"device_sig\");\n\ndouble* data = NULL;\ndouble* signal = NULL;\n\n"

def generateSuffix():
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

    for i in range(len(host_g_code)):
        hostcode += host_g_code[i]
        hostcode += host_a_code[i]
        remotecode += remote_g_code[i]
        remotecode += remote_a_code[i]
        devicecode += device_g_code[i]
        devicecode += device_a_code[i]

    hostcode += "  XBT_INFO(\"Host exiting.\");\n}"
    remotecode += "  XBT_INFO(\"Remote exiting.\");\n}"
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

    # start generating .cpp file
    generatePrefix()
    
    graph, numVertices = sampleGraph()
    numOutgoing = genOutgoing(graph, numVertices)

    # -- Different workload cuts: evenly distributed, all host, all remote, all device --
    #mainCut, remoteCut, deviceCut, MR_shared, RD_shared, MD_shared = partitionGraph(graph, numVertices, 1.0/3.0, 1.0/3.0, 1.0/3.0)
    #mainCut, remoteCut, deviceCut, MR_shared, RD_shared, MD_shared = partitionGraph(graph, numVertices, 1.0, 0, 0)
    #mainCut, remoteCut, deviceCut, MR_shared, RD_shared, MD_shared = partitionGraph(graph, numVertices, 0, 1.0, 0)
    mainCut, remoteCut, deviceCut, MR_shared, RD_shared, MD_shared = partitionGraph(graph, numVertices, 0, 0, 1.0)
    
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

    # Complete .cpp file generation
    generateSuffix()

if __name__ == "__main__":
    main()
    
####### NOTES:
# Gather should be done across all edges IN PARALLEL -> maximize CPU performance
