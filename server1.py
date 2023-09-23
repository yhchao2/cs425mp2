import socket
import threading
import time
import json
import sys
import random

T_GOSSIP = 0.5
FAILURE_THRESHOLD = 8
T_CLEANUP = 8

def log_event(message, filename):
    with open(filename, 'a') as f:
        message = message + "\n"
        f.write(message)

def command_line_interface():
    global status
    global suspicion
    while True:
        cmd = input("Enter your command (type 'exit' to quit, 'leave' to set status to leave, 'online' to set status to online): ")
        if cmd == 'exit':
            print("Exiting command line interface.")
            break
        elif cmd == 'leave':
            status = 'failed'
            membership_list[node_name]["status"] = status  # Update local membership status
            print("Node set to 'leave' status.")
        elif cmd == 'join':
            status = 'online'
            suspicion = False
            lock.acquire()
            membership_list[node_name]["status"] = status  # Update local membership status
            membership_list[node_name]["version_id"] += 1
            lock.release()
            print("Node set to 'online' status.")
        elif cmd == 'disable suspicion':
            suspicion = False
            print("disable gossip s")
        elif cmd == 'enable suspicion':
            suspicion = True
            print("enable gossip s")
        elif cmd == 'list_mem':
            print(membership_list)
        elif cmd == 'list_self':
            print(membership_list[node_name])
        else:
            print(f"Unknown command: {cmd}")

def receiver(name, s):
    while True:
        # Receive data
        
        if status == 'online':
            
            if suspicion == False:
                try:
                    data, addr = s.recvfrom(4096)
                    received_list = json.loads(data.decode())
                    lock.acquire()
                    first_key = list(received_list.keys())[0]
                    if node_name == "node1" and received_list[first_key]["status"] == "joining":
                        target_ip, target_port = NODES[first_key] 
                        s.sendto(json.dumps(membership_list).encode(), (target_ip, target_port))
                    else: 
                        for node, node_data in received_list.items():
                            if node not in membership_list and node_data["status"] != "failed":
                                membership_list[node] = {"heartbeat_counter":0,"local_clock": 0, "timestamp": 0, "version_id": 0, "status": "online", "incarnation":0}
                                output = node + " joined"
                                print(output)
                                log_event(output, filename)
                            # Update if the received heartbeat_counter is newer
                            if node_data["status"] != "failed":
                                if node_data["heartbeat_counter"] > membership_list[node]["heartbeat_counter"]:
                                    membership_list[node] = node_data
                                    membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]
                    lock.release()
                except socket.timeout:
                    pass
            else: 
                try:
                    data, addr = s.recvfrom(4096)
                    received_list = json.loads(data.decode())
                    lock.acquire()
                    first_key = list(received_list.keys())[0]
                    if node_name == "node1" and received_list[first_key]["status"] == "joining":
                        target_ip, target_port = NODES[first_key] 
                        s.sendto(json.dumps(membership_list).encode(), (target_ip, target_port))
                    for node, node_data in received_list.items():
                        if node not in membership_list and node_data["status"] != "failed":
                            membership_list[node] = {"heartbeat_counter":0,"local_clock": 0, "timestamp": 0, "version_id": 0, "status": "online", "incarnation":0}
                            output = node + " joined"
                            print(output)
                            log_event(output, filename)
                            #print(output)
                        # Update if the received heartbeat_counter is newer    
                        elif node in membership_list:
                            """
                            if node_data["status"] == "online" and node_data["heartbeat_counter"] > membership_list[node]["heartbeat_counter"]:
                                membership_list[node] = node_data
                                membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]

                            elif node_data["status"] == "suspect" and node_data["incarnation"] > membership_list[node]["incarnation"]:
                                membership_list[node] = node_data
                                membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]
                                membership_list[node]["status"] = "online"
                            """
                            if membership_list[node]["status"] == 'online' and node_data["status"] == "suspect":
                                if membership_list[node]['incarnation'] < node_data["incarnation"]:
                                    membership_list[node] = node_data
                                    membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]
                                    membership_list[node]["status"] = "suspect"
                            elif membership_list[node]["status"] == 'suspect' and node_data["status"] == "online":
                                if membership_list[node]['incarnation'] < node_data["incarnation"]:
                                    membership_list[node] = node_data
                                    membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]
                                    membership_list[node]["status"] = "online"
                            elif membership_list[node]["status"] == 'suspect' and node_data["status"] == "suspect":
                                if membership_list[node]['incarnation'] < node_data["incarnation"]:
                                    membership_list[node] = node_data
                                    membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]
                                    membership_list[node]["status"] = "suspect"
                            elif membership_list[node]["status"] == 'online' and node_data["status"] == "online":
                                if node_data["heartbeat_counter"] > membership_list[node]["heartbeat_counter"]:
                                    membership_list[node] = node_data
                                    membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]

                        if node == node_name and node_data["status"] == "suspect":
                            membership_list[node]["status"] = "online"
                            membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]
                            membership_list[node]["incarnation"] += 1
                        
                    lock.release()
                except socket.timeout:
                    pass

def failure_detector(node_name):
    while True:
        #if status == 'online':
            if suspicion == False:
                current_time = time.time()
                lock.acquire()
                for node, node_data in membership_list.items():
                    if node != node_name and membership_list[node_name]["local_clock"] - node_data["local_clock"] >= FAILURE_THRESHOLD and node_data["status"] == 'online':
                        print(f"{node_name} : {node} has failed!")
                        output = node + " has failed!"
                        log_event(output, filename)
                        membership_list[node]["status"] = "failed"
                        failed_nodes[node] = membership_list[node_name]["local_clock"]

                # Check if any suspected node has surpassed the T_CLEANUP interval
                nodes_to_remove = [node for node, suspect_time in failed_nodes.items() if membership_list[node_name]["local_clock"] - suspect_time > T_CLEANUP]

                # Remove nodes that have surpassed the T_CLEANUP from the membership list and suspected_nodes list
                for node in nodes_to_remove:
                    output = "Removing " + node + " from membership list after T_cleanup."
                    print(output)
                    log_event(output, filename)
                    if node in membership_list:
                        del membership_list[node]
                    del failed_nodes[node]
                    #for node in suspected_nodes:
                    #    del membership_list[node]
                lock.release()
            else:
                current_time = time.time()
                lock.acquire()
                for node, node_data in membership_list.items():
                    if node_data["status"] == "online" and membership_list[node_name]["local_clock"] - node_data["local_clock"] <= T_CLEANUP and node in suspected_nodes:
                        print(f"{node} is now active again.")
                        output = node + " is now active again."
                        log_event(output, filename)
                        del suspected_nodes[node]

                    elif node != node_name and membership_list[node_name]["local_clock"] - node_data["local_clock"] > FAILURE_THRESHOLD and node_data["status"] == 'online':
                        print(f"{node_name} suspects {node} has failed!")
                        output = "suspect " + node + " has failed!"
                        log_event(output, filename)
                        membership_list[node]["status"] = "suspect"
                        suspected_nodes[node] = membership_list[node_name]["local_clock"]

                # Check if any suspected node has surpassed the FAILURE_THRESHOLD interval
                nodes_that_failed = [node for node, suspect_time in suspected_nodes.items() if membership_list[node_name]["local_clock"] - suspect_time > FAILURE_THRESHOLD]
                
                # Remove nodes that have surpassed the FAILURE_THRESHOLD from the membership list and suspected_nodes list
                for node in nodes_that_failed:
                    output = node + " failed"
                    print(output)
                    log_event(output, filename)
                    if node in membership_list:
                        membership_list[node]["status"] = "failed"
                        readytoremove_nodes[node] = membership_list[node_name]["local_clock"]
                    del suspected_nodes[node]
                    #for node in suspected_nodes:
                    #    del membership_list[node]
                nodes_to_remove = [node for node, suspect_time in readytoremove_nodes.items() if membership_list[node_name]["local_clock"] - suspect_time > T_CLEANUP]

                # Remove nodes that have surpassed the FAILURE_THRESHOLD from the membership list and suspected_nodes list
                for node in nodes_to_remove:
                    print(f"Removing {node} from membership list after T_cleanup.")
                    output = "Removing " + node + " from membership list after T_cleanup."
                    log_event(output, filename)
                    if node in membership_list:
                        del membership_list[node]
                    del readytoremove_nodes[node]
                    #for node in suspected_nodes:
                    #    del membership_list[node]
                lock.release()

            time.sleep(T_GOSSIP)

def gossip(node_name):
    ip, port = NODES[node_name]
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((ip, port))
    if node_name != "node1":  # If it's not node1, then request to join
        introducer_ip, introducer_port = NODES["node1"]
        msg = {node_name: {"status": "joining"}}
        s.sendto(json.dumps(msg).encode(), (introducer_ip, introducer_port))
        data, _ = s.recvfrom(4096)
        received_list = json.loads(data.decode())
        #print(f"printing {received_list}")
        #print(f"printing {membership_list}")
        membership_list.update(received_list)
        #print(f"printing {membership_list}")
        for node, node_data in membership_list.items():
            if node != node_name:
                output = node + " joined"
                print(output)
                log_event(output, filename)
            
    # Start the receiver in a separate thread
    receiver_thread = threading.Thread(target=receiver, args=(node_name,s))
    receiver_thread.start()

    failure_detector_thread = threading.Thread(target=failure_detector, args=(node_name,))
    failure_detector_thread.start()
    
    while True:
        # Update and send own data only if the node is online
        
        lock.acquire()
        
            #if suspicion == False:
        membership_list[node_name]["timestamp"] = time.time()
        #membership_list[name]["version_id"] += 1
        membership_list[node_name]["local_clock"] += 1
        membership_list[node_name]["heartbeat_counter"] += 1
        if status == 'online':
                # Send membership list to all other nodes
                i = 0
                nodeList = list(NODES.keys())
                while i < 4:
                    target_node = random.choice(nodeList)
                    target_ip, target_port = NODES[target_node]
                    if target_node != node_name:
                        nodeList.remove(target_node)
                        try: 
                            s.sendto(json.dumps(membership_list).encode(), (target_ip, target_port))
                            i+=1
                        except Exception as e:
                            print ("Error sending data: %s" % e) 
                            print(target_node)
                            print(target_ip, target_port)

                #for key, value in membership_list.items():
                #    output = key + ":" + value["status"]
                #   print(output)
            
        lock.release()
        time.sleep(T_GOSSIP)


    receiver_thread.join()
    failure_detector_thread.join()

if __name__ == "__main__":

    host = socket.gethostname() # local host name
    port = 8011  # port number
    # Node configurations (used to know about peers)
    NODES = {
        'node1': ("127.0.0.1", 8011),
        'node2': ("127.0.0.1", 8012),
        'node3': ("127.0.0.1", 8013),
        'node4': ("127.0.0.1", 8014),
        'node5': ("127.0.0.1", 8015),
        'node6': ("127.0.0.1", 8016),
        'node7': ("127.0.0.1", 8017),
        'node8': ("127.0.0.1", 8018),
        'node9': ("127.0.0.1", 8019),
        'node10': ("127.0.0.1", 8020)
    }
    
    ip_list = ['fa23-cs425-7601.cs.illinois.edu','fa23-cs425-7602.cs.illinois.edu'
    ,'fa23-cs425-7603.cs.illinois.edu','fa23-cs425-7604.cs.illinois.edu'
    ,'fa23-cs425-7605.cs.illinois.edu','fa23-cs425-7606.cs.illinois.edu'
    ,'fa23-cs425-7607.cs.illinois.edu','fa23-cs425-7608.cs.illinois.edu'
    ,'fa23-cs425-7609.cs.illinois.edu','fa23-cs425-7610.cs.illinois.edu']

    
    for i, key in enumerate(NODES.keys()):
        port = NODES[key][1]
        NODES[key] = (ip_list[i], port)
    
    # Node status (online/leave)
    status = 'online'
    suspicion = True
    lock = threading.Lock()


    if len(sys.argv) < 2:
        print("Usage: python script_name.py node_name")
        sys.exit(1)
    
    node_name = sys.argv[1]
    # Membership list initialization
    initial_data = {"heartbeat_counter":0,"local_clock": 0, "timestamp": 0, "version_id": 0, "status": "online", "incarnation":0}
    membership_list = {node_name: initial_data}
    failed_nodes = {}  # To keep track of when nodes were first failed
    suspected_nodes = {}
    readytoremove_nodes = {}
    filename = node_name+"log.txt"
    f = open(filename, "w")
    f.write(f"{node_name} joined\n")
    f.close()
    if node_name not in NODES:
        print(f"Unknown node name. Choose from: {', '.join(NODES.keys())}")
        sys.exit(1)

    cli_thread = threading.Thread(target=command_line_interface)
    cli_thread.start()

    gossip(node_name)

    cli_thread.join()
    print("Program has terminated.")