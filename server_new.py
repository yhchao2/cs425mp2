import socket
import threading
import time
import json
import sys
import random
import argparse
import uuid

from util import Logger, clamp, if_then_else, with_default
from enum import Enum
from typing import TypedDict, Literal, cast

# GLOBAL CONSTANTS

DEFAULT_T_GOSSIP = 0.5
DEFAULT_T_SUSPECT = 1.0   # After 2 * T_GOSSIP, suspect
DEFAULT_T_FAIL = 2.0      # After 4 * T_GOSSIP, fail
DEFAULT_T_CLEANUP = 2.0   # When failed, cleanup after 2.0 seconds

DEFAULT_SELF_LOCAL = ("localhost", 8000)
DEFAULT_SELF_REMOTE = (socket.gethostbyname(socket.gethostname()), 80)
DEFAULT_SELF_ID = str(uuid.uuid4())

DEFAULT_INTRODUCER_LOCAL = ("localhost", 8000)
DEFAULT_INTRODUCER_REMOTE = ("fa23-cs425-7601.cs.illinois.edu", 80)
DEFAULT_MESSAGE_DROP_RATE = 0.0  # APPLIED AT RECEIVER SIDE

DEFAULT_VERBOSITY = 1

T_GOSSIP = DEFAULT_T_GOSSIP
T_SUSPECT = DEFAULT_T_SUSPECT
T_FAIL = DEFAULT_T_FAIL
T_CLEANUP = DEFAULT_T_CLEANUP
T_UPDATE = 0.1

INTRODUCER_ADDRESS = DEFAULT_INTRODUCER_REMOTE

CONNECTION_BUFFER_SIZE = 1024
MESSAGE_DROP_RATE = DEFAULT_MESSAGE_DROP_RATE
VERBOSITY = 1
LOGGER = Logger(VERBOSITY)

# ENUMS AND TYPED DICTS


class Command(Enum):
    JOIN = 0
    LEAVE = 1
    GOSSIP = 2


class Status(Enum):
    ALIVE = 0
    SUSPECTED = 1
    FAILED = 2


class Member(TypedDict):
    id: str
    address: tuple[str, int]
    heartbeat: int
    time: float
    status: tuple[int, Status]  # (incarnation, status)
    failed_time: float          # time when failed


class JoinMessageData(TypedDict):
    id: str
    host: str
    port: int


class LeaveMessageData(TypedDict):
    id: str


class LeaveMessage(TypedDict):
    command: Literal[Command.LEAVE]
    data: LeaveMessageData


class JoinMessage(TypedDict):
    command: Literal[Command.JOIN]
    data: JoinMessageData


class GossipMessage(TypedDict):
    command: Literal[Command.GOSSIP]
    data: dict[str, Member]


class Message(TypedDict):
    command: Command
    data: dict[str, Member] | JoinMessageData | LeaveMessageData


class SelfNodeType(TypedDict):
    Id: str
    Address: tuple[str, int]
    IsOnline: bool
    Incarnation: int


# GLOBAL VARIABLES


MemberList: dict[str, Member] = {}
MemberListLock = threading.Lock()
EnableSuspicionStrategy = False
SelfNode = SelfNodeType(
    Id=DEFAULT_SELF_ID,
    Address=DEFAULT_SELF_LOCAL,
    IsOnline=True,
    Incarnation=0,
)


def de_suspect(status: tuple[int, Status]) -> tuple[int, Status]:
    return (status[0], if_then_else(
        status[1] == Status.SUSPECTED and EnableSuspicionStrategy,
        Status.ALIVE,
        status[1]
    ))


def mark_as_failed(member: Member) -> None:
    """
    Mark a member as failed, must be called with lock acquired

    Args:
        member (Member): the member to be marked as failed
    """
    MemberList[member["id"]]["status"] = (
        member["status"][0],
        Status.FAILED
    )
    MemberList[member["id"]]["failed_time"] = time.time()


def merge_to_member_list(gossip_member_list: dict[str, Member]) -> None:

    MemberListLock.acquire()

    for member_id, new_member in gossip_member_list.items():
        if member_id not in MemberList:
            # new member

            MemberList[member_id] = Member(
                id=new_member["id"],
                address=new_member["address"],
                heartbeat=new_member["heartbeat"],
                time=time.time(),
                status=de_suspect(new_member["status"]),
                failed_time=0.0
            )
        else:
            old_member = MemberList[member_id]

            # heartbeat rule
            if new_member["heartbeat"] > old_member["heartbeat"]:
                MemberList[member_id]["heartbeat"] = new_member["heartbeat"]
                MemberList[member_id]["time"] = time.time()

            # incarnation rule
            new_incarnation, new_status = de_suspect(new_member["status"])
            old_incarnation, _ = old_member["status"]

            if new_status == Status.FAILED:
                MemberList[member_id]["status"] = (new_incarnation, new_status)
            elif new_incarnation > old_incarnation:
                MemberList[member_id]["status"] = (new_incarnation, new_status)
            elif new_incarnation == old_incarnation and new_status == Status.SUSPECTED:
                MemberList[member_id]["status"] = (new_incarnation, new_status)

    MemberListLock.release()


def initialize_node() -> socket.socket:

    try:
        self_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  # UDP
        self_socket.bind(SelfNode["Address"])
        self_socket.settimeout(1.0)
    except socket.error:
        LOGGER.log("Failed to bind socket")
        sys.exit(1)

    # remember to put yourself in the member list
    MemberList[SelfNode["Id"]] = Member(
        id=SelfNode["Id"],
        address=SelfNode["Address"],
        heartbeat=0,
        time=time.time(),
        status=(0, Status.ALIVE),
        failed_time=0.0
    )

    # if self is introducer, do nothing
    if SelfNode["Address"] == INTRODUCER_ADDRESS:
        return self_socket

    try:
        # otherwise connect to introducer
        self_socket.sendto(json.dumps(JoinMessage(
            command=Command.JOIN,
            data=JoinMessageData(
                id=SelfNode["Id"],
                host=SelfNode["Address"][0],
                port=SelfNode["Address"][1],
            )
        )).encode(), INTRODUCER_ADDRESS)
    except socket.error:
        LOGGER.log("Failed to send message to introducer")
        sys.exit(1)

    try:
        data, _ = self_socket.recvfrom(CONNECTION_BUFFER_SIZE)
        message: GossipMessage = json.loads(data.decode())

        merge_to_member_list(message["data"])

        return self_socket
    except socket.timeout:
        LOGGER.log("Introducer is not responding. Exiting...")
        sys.exit(1)


def handle_updating_member_list() -> None:
    while True:

        if not SelfNode["IsOnline"]:
            time.sleep(T_UPDATE)
            continue

        MemberListLock.acquire()

        for member_id, member in MemberList.items():
            if member_id == SelfNode["Id"]:
                member["heartbeat"] += 1
                member["time"] = time.time()
                member["status"] = (
                    member["status"][0] + 1,
                    Status.ALIVE
                )
                continue

            if (
                member["status"][1] == Status.ALIVE and
                time.time() - member["time"] > T_SUSPECT and
                EnableSuspicionStrategy
            ):
                MemberList[member_id]["status"] = (
                    member["status"][0],
                    Status.SUSPECTED
                )
            elif (
                member["status"][1] == Status.SUSPECTED and
                time.time() - member["time"] > T_FAIL
            ):
                mark_as_failed(member)
            elif (
                member["status"][1] == Status.FAILED and
                time.time() - member["failed_time"] > T_CLEANUP
            ):
                del MemberList[member_id]

        MemberListLock.release()

        time.sleep(T_UPDATE)


def handle_join_message(
    self_socket: socket.socket,
    join_data: JoinMessageData
) -> None:
    if join_data["id"] in MemberList:
        # already in the member list (should not happen)
        LOGGER.log(
            f"Member {join_data['id']} already in the member list",
            verbosity=2
        )
        return
    elif SelfNode["Address"] != INTRODUCER_ADDRESS:
        # not the introducer, you should only receive
        # join message from introducer
        LOGGER.log(
            f"{join_data['id']} attempts to join from"
            f"{SelfNode['Id']} but not the introducer",
            verbosity=2
        )
        return
    else:
        # add to member list
        merge_to_member_list({
            join_data["id"]: Member(
                id=join_data["id"],
                address=(join_data["host"], join_data["port"]),
                heartbeat=0,
                time=time.time(),
                status=(0, Status.ALIVE),
                failed_time=0.0
            )
        })

        # send back the member list
        try:
            self_socket.sendto(json.dumps(GossipMessage(
                command=Command.GOSSIP,
                data=MemberList
            )).encode(), (join_data["host"], join_data["port"]))
        except socket.error:
            LOGGER.log(f"Failed to send message to joiner {join_data['id']}")


def handle_leave_message(
    leave_data: LeaveMessageData
) -> None:
    if leave_data["id"] not in MemberList:
        # not in the member list (should not happen)
        LOGGER.log(
            f"Member {leave_data['id']} not in the member list",
            verbosity=2
        )
        return
    else:
        # mark that member as failed
        mark_as_failed(MemberList[leave_data["id"]])


def handle_receiving_message(self_socket: socket.socket) -> None:
    while True:
        if not SelfNode["IsOnline"]:
            time.sleep(T_UPDATE)
            continue

        try:
            data, address = self_socket.recvfrom(CONNECTION_BUFFER_SIZE)

            if random.random() < MESSAGE_DROP_RATE:
                LOGGER.log(f"Simulated Data Drop From {address}", verbosity=3)
                continue

            message: Message = json.loads(data.decode())

            match message["command"]:
                case Command.JOIN:
                    handle_join_message(
                        self_socket,
                        cast(JoinMessage, message)["data"]
                    )
                case Command.LEAVE:
                    handle_leave_message(
                        cast(LeaveMessage, message)["data"]
                    )
                case Command.GOSSIP:
                    merge_to_member_list(cast(GossipMessage, message)["data"])

        except socket.timeout:
            LOGGER.log("Socket timeout", verbosity=2)
            continue

        time.sleep(T_UPDATE)


def handle_sending_gossip(self_socket: socket.socket) -> None:
    while True:
        if not SelfNode["IsOnline"]:
            time.sleep(T_GOSSIP)
            continue

        MemberListLock.acquire()

        select_count = min(4, len(MemberList))

        selected_members = random.sample(
            list(MemberList.items()),
            select_count
        )

        for member_id, member in selected_members:
            if member_id == SelfNode["Id"]:
                continue

            if member["status"][1] == Status.FAILED:
                continue

            try:
                self_socket.sendto(json.dumps(GossipMessage(
                    command=Command.GOSSIP,
                    data=MemberList
                )).encode(), member["address"])
            except socket.error:
                LOGGER.log("Failed to send message to introducer", verbosity=2)
                sys.exit(1)

        MemberListLock.release()

        time.sleep(T_GOSSIP)


def main():
    """ The main Function """

    global T_GOSSIP, T_SUSPECT, T_FAIL, T_CLEANUP
    global MESSAGE_DROP_RATE, VERBOSITY, LOGGER
    global INTRODUCER_ADDRESS, SelfNode
    global EnableSuspicionStrategy

    # Parse Arguments

    parser = argparse.ArgumentParser(
        description="Gossip-based Membership Protocol for CS425 MP2"
    )

    parser.add_argument(
        "-u", "--id", dest="id", type=str,
        help="Server ID", default=DEFAULT_SELF_ID
    )

    parser.add_argument(
        "-p", "--port", dest="port", type=int,
        help="Server Port Number"
    )

    parser.add_argument(
        "-iH", "--introducer-host", dest="introducer_host", type=str,
        help="Introducer Hostname"
    )

    parser.add_argument(
        "-iP", "--introducer-port", dest="introducer_port", type=int,
        help="Introducer Port Number"
    )

    parser.add_argument(
        "-tG", "--t-gossip", dest="t_gossip", type=float,
        help="Time Interval for Gossiping", default=DEFAULT_T_CLEANUP
    )

    parser.add_argument(
        "-tC", "--t-cleanup", dest="t_cleanup", type=float,
        help="Time Interval for Cleanup", default=DEFAULT_T_CLEANUP
    )

    parser.add_argument(
        "-tF", "--t-fail", dest="t_fail", type=float,
        help="Time Interval for Failure Detection", default=DEFAULT_T_FAIL
    )

    parser.add_argument(
        "-tS", "--t-suspect", dest="t_suspect", type=float,
        help="Time Interval for Suspect Detection", default=DEFAULT_T_SUSPECT
    )

    parser.add_argument(
        "-d", "--message-drop-rate", dest="message_drop_rate", type=float,
        help="Message Drop Rate", default=DEFAULT_MESSAGE_DROP_RATE
    )

    parser.add_argument(
        "-s", "--enable-suspicion-strategy", dest="enable_suspicion_strategy",
        action="store_true",
        help="Enable Suspicion Strategy"
    )

    parser.add_argument(
        "-l", "--local", dest="local", action="store_true",
        help="Enable Local Mode"
    )

    parser.add_argument(
        "-v", "--verbosity", dest="verbosity", type=int,
        help="Log Verbosity Level", default=DEFAULT_VERBOSITY
    )

    args = parser.parse_args()

    # Set Global Variables

    default_self = if_then_else(
        args.local,
        DEFAULT_SELF_LOCAL,
        DEFAULT_SELF_REMOTE
    )
    default_introducer = if_then_else(
        args.local,
        DEFAULT_INTRODUCER_LOCAL,
        DEFAULT_INTRODUCER_REMOTE
    )

    SelfNode["Address"] = (
        default_self[0],
        with_default(args.port, default_self[1])
    )
    SelfNode["Id"] = with_default(args.id, DEFAULT_SELF_ID)

    EnableSuspicionStrategy = args.enable_suspicion_strategy
    T_GOSSIP = args.t_gossip
    T_SUSPECT = args.t_suspect
    T_FAIL = args.t_fail
    T_CLEANUP = args.t_cleanup
    VERBOSITY = args.verbosity
    LOGGER = Logger(VERBOSITY)

    MESSAGE_DROP_RATE = clamp(args.message_drop_rate, 0.0, 1.0)

    introducer_host = with_default(args.introducer_host, default_introducer[0])
    introducer_port = with_default(args.introducer_port, default_introducer[1])

    INTRODUCER_ADDRESS = (introducer_host, introducer_port)

    # Initialize Node

    self_socket = initialize_node()

    # Start Threads
    try:
        thread_receive = threading.Thread(
            target=handle_receiving_message,
            args=((self_socket, ))
        )
        thread_receive.start()

        thread_gossip = threading.Thread(
            target=handle_sending_gossip,
            args=((self_socket, ))
        )
        thread_gossip.start()

        thread_update = threading.Thread(
            target=handle_updating_member_list,
        )
        thread_update.start()

    except Exception as e:
        LOGGER.log(e)
        sys.exit(1)
    except KeyboardInterrupt:
        LOGGER.log("Keyboard Interrupt. Exiting...")
        self_socket.close()
        sys.exit(0)


if __name__ == '__main__':
    main()
