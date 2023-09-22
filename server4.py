import socket
import threading
import time
import json
import sys
import random

T_GOSSIP = 0.5
FAILURE_THRESHOLD = 6
T_CLEANUP = 6

def command_line_interface():
    global status
    while True:
        cmd = input("Enter your command (type 'exit' to quit, 'leave' to set status to leave, 'online' to set status to online): ")
        if cmd == 'exit':
            print("Exiting command line interface.")
            break
        elif cmd == 'leave':
            status = 'leave'
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
        elif cmd == 'gossip+s':
            suspicion = False
            print("activate gossip s")
        elif cmd == 'gossip+s':
            suspicion = True
            print("activate gossip s")
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
                        if node not in membership_list and node_data["status"] != "leave":
                            membership_list[node] = {"heartbeat_counter":0,"local_clock": 0, "timestamp": 0, "version_id": 0, "status": "leave", "incarnation":0}
                            output = node + " joined"
                            print(output)
                        # Update if the received version_id is newer
                        if node_data["status"] != "leave":
                            if node_data["heartbeat_counter"] > membership_list[node]["heartbeat_counter"]:
                                membership_list[node] = node_data
                                membership_list[node]["local_clock"] = membership_list[node_name]["local_clock"]
                    lock.release()
                except socket.timeout:
                    pass

def failure_detector(name):
    while True:
        if status == 'online':
            if suspicion == False:
                current_time = time.time()
                #suspected_nodes = []
                lock.acquire()
                for node, node_data in membership_list.items():
                    if node != name and membership_list[node_name]["local_clock"] - node_data["local_clock"] > FAILURE_THRESHOLD and node_data["status"] == 'online':
                        print(f"{name} suspects {node} has failed!")
                        membership_list[node]["status"] = "leave"
                        #suspected_nodes.append(node)
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
                time.sleep(T_GOSSIP)

def gossip(name):
    ip, port = NODES[name]
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((ip, port))
    
    # Start the receiver in a separate thread
    receiver_thread = threading.Thread(target=receiver, args=(name,s))
    receiver_thread.start()

    failure_detector_thread = threading.Thread(target=failure_detector, args=(name,))
    failure_detector_thread.start()

    while True:
        # Update and send own data only if the node is online
        lock.acquire()
        if status == 'online':
            if suspicion == False:
                membership_list[node_name]["timestamp"] = time.time()
                #membership_list[name]["version_id"] += 1
                membership_list[node_name]["local_clock"] += 1
                membership_list[node_name]["heartbeat_counter"] += 1

                # Send membership list to all other nodes
                i = 0
                while i < 3:
                    target_node = random.choice(list(NODES.keys()))
                    target_ip, target_port = NODES[target_node]
                    if target_node != node_name:
                        try: 
                            s.sendto(json.dumps(membership_list).encode(), (target_ip, target_port))
                            i+=1
                            #print("success")
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
        'node4': ("127.0.0.1", 8014)
    }

    # Node status (online/leave)
    status = 'online'
    suspicion = False

    lock = threading.Lock()

    if len(sys.argv) < 2:
        print("Usage: python script_name.py node_name")
        sys.exit(1)
    
    node_name = sys.argv[1]
    # Membership list initialization
    initial_data = {"heartbeat_counter":0,"local_clock": 0, "timestamp": 0, "version_id": 0, "status": "leave", "incarnation":0}
    membership_list = {node_name: initial_data}
    failed_nodes = {}  # To keep track of when nodes were first suspected
    #print(membership_list)
    if node_name not in NODES:
        print(f"Unknown node name. Choose from: {', '.join(NODES.keys())}")
        sys.exit(1)

    cli_thread = threading.Thread(target=command_line_interface)
    cli_thread.start()

    gossip(node_name)

    cli_thread.join()
    print("Program has terminated.")