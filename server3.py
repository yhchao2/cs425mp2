import socket
import threading
import time
import json
import sys
import random

T_GOSSIP = 0.5
FAILURE_THRESHOLD = 10
T_CLEANUP = 10

def log_event(message, filename):
    with open(filename, 'a') as f:
        message = message + "\n"
        f.write(message)

def command_line_interface():
    global status
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
        elif cmd == 'enable suspuscion +s':
            suspicion = True
            print("enable gossip s")
        elif cmd == 'list_mem':
            print(membership_list)
        elif cmd == 'list_self':
            print(membership_list[node_name])
        else:
            print(f"Unknown command: {cmd}")

def receiver(name, sock):
    while True:
        # Receive data
        
        if status == 'online':
            if suspicion == False:
                try:
                    data, addr = sock.recvfrom(4096)
                    received_list = json.loads(data.decode())
                    lock.acquire()
                    for node, node_data in received_list.items():
                        if node not in membership_list and node_data["status"] != "failed":
                            membership_list[node] = {"heartbeat_counter":0,"local_clock": 0, "timestamp": 0, "version_id": 0, "status": "online", "incarnation":0}
                            output = node + " joined"
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
                    data, addr = sock.recvfrom(4096)
                    received_list = json.loads(data.decode())
                    lock.acquire()
                    for node, node_data in received_list.items():
        
                        if node not in membership_list:
                            membership_list[node] = {"heartbeat_counter":0,"local_clock": 0, "timestamp": 0, "version_id": 0, "status": "online", "incarnation":0}
                            output = node + " joined"
                            log_event(output, filename)
                            #print(output)
                        # Update if the received heartbeat_counter is newer    
                        if membership_list[node]["status"] == "suspect":
                            if node_data["incarnation"] > membership_list[node]["incarnation"]:
                                membership_list[node] = node_data
                                membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]
                                membership_list[node]["status"] = "online"
                        elif node_data["heartbeat_counter"] > membership_list[node]["heartbeat_counter"]:
                            membership_list[node] = node_data
                            membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]
                        if node == node_name and node_data["status"] == "suspect":
                            membership_list[node]["status"] = "online"
                            membership_list[node]["incarnation"] += 1
                        
                    lock.release()
                except socket.timeout:
                    pass

def failure_detector(node_name):
    while True:
        if status == 'online':
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
                    print(f"Removing {node} from membership list due to prolonged inactivity.")
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
                nodes_to_remove = [node for node, suspect_time in suspected_nodes.items() if membership_list[node_name]["local_clock"] - suspect_time > FAILURE_THRESHOLD]

                # Remove nodes that have surpassed the FAILURE_THRESHOLD from the membership list and suspected_nodes list
                for node in nodes_to_remove:
                    print(f"Removing {node} from membership list due to prolonged inactivity.")
                    output = "Removing " + node + " from membership list due to prolonged inactivity."
                    log_event(output, filename)
                    if node in membership_list:
                        del membership_list[node]
                    del suspected_nodes[node]
                    #for node in suspected_nodes:
                    #    del membership_list[node]
                lock.release()

            time.sleep(T_GOSSIP)

def gossip(node_name):
    ip, port = NODES[node_name]
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((ip, port))
    
    # Start the receiver in a separate thread
    receiver_thread = threading.Thread(target=receiver, args=(node_name,s))
    receiver_thread.start()

    failure_detector_thread = threading.Thread(target=failure_detector, args=(node_name,))
    failure_detector_thread.start()
    
    while True:
        # Update and send own data only if the node is online
        
        lock.acquire()
        if status == 'online':
            #if suspicion == False:
                membership_list[node_name]["timestamp"] = time.time()
                #membership_list[name]["version_id"] += 1
                membership_list[node_name]["local_clock"] += 1
                membership_list[node_name]["heartbeat_counter"] += 1

                # Send membership list to all other nodes
                i = 0
                nodeList = list(NODES.keys())
                while i < 3:
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
        'node8': ("127.0.0.1", 8018)
    }

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
    filename = node_name+"log.txt"
    f = open(filename, "w")
    f.write("")
    f.close()
    if node_name not in NODES:
        print(f"Unknown node name. Choose from: {', '.join(NODES.keys())}")
        sys.exit(1)

    cli_thread = threading.Thread(target=command_line_interface)
    cli_thread.start()

    gossip(node_name)

    cli_thread.join()
    print("Program has terminated.")