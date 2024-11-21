import requests, time
from threading import Thread, Lock
from utils.timer import ResettableTimer
from utils.states import FOLLOWER, LEADER, CANDIDATE

class Node():
    def __init__(self, index, config, ip, port, max_num_votes):
        self.lock = Lock()                                                    # Lock to access node variables

        # When servers start up, they begin as followers
        self.state = FOLLOWER                                                   # State of the node, initially follower
        self.term = 0                                                           # Current term of the node
        self.voted_for = None                                                   # Voted for in the current term
        self.vote_count = 0                                                     # Vote count for the current term

        self.id = index                                                         # Node ID
        self.config = config                                                    # Details of other nodes in cluster
        self.ip = ip                                                            # Node's IP
        self.port = port                                                        # Node's Port
        self.majority_votes = max_num_votes // 2 + 1                            # Number of votes needed for majority
        
        self.message_topics = {}                                                # Message topics-queue

        self.log = []                                                           # (command, term), indexed from 1
        self.commitIndex = 0                                                    # highest log entry committed
        self.lastApplied = 0                                                    # highest log entry applied
        # TODO
        self.nextIndex = {}                                                     # list of next log entries to send
        self.match_index = {}                                                   # list of highest log entries replicated
        self.commit_count = 0                                                   # Number of threads that have committed
        self.commit_applied = False                                             # Has the current commit been applied

        self.endpoint_result = None                                             # For get message client endpoint

        # If a follower receives no communication over a period of time called the election timeout, then it assumes
        # there is no viable leader and begins an election to choose a new leader
        self.election_timer = ResettableTimer(function=self.election,
            interval_lb=500,
            interval_ub=1000,
            node_id=self.id,
            timer_type="election"
        )
        # Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries) to all followers in order to
        # maintain their authority.
        self.heartbeat_timer = ResettableTimer(function=self.heartbeat,
            interval_lb=100,
            interval_ub=200,
            node_id=self.id,
            timer_type="heartbeat"
        )

        print(f"Node {self.id}: Starting election timer")
        self.election_timer.run()


    # If a client contacts a follower, the follower redirects it to the leader
    def is_leader(self):
        with self.lock:                                                         # Obtain the lock
            val = self.state == LEADER
        print(f"Node {self.id}: Current state == leader -> {val}")
        return val                                                              # Return whether node is leader
        
    
    # Election timeout expired, elect a new leader
    def election(self):
        # Candidates attempts to become leader
        # If a candidate wins the election, it serves as leader for the rest of the term
        # The term may end with no leader; a new term (with a new election) will begin

        print(f"Node {self.id}: Starting new election")
        with self.lock:
            # To begin an election, a follower increments its current term and transitions to candidate state
            self.term += 1
            self.state = CANDIDATE
            # It then votes for itself
            self.voted_for = self.id
            self.vote_count += 1

            # Votes could be split so that no candidate obtains a majority. When this happens, each candidate will time
            # out and start a new election by incrementing its term and initiating another round of Request-Vote RPCs.
            self.election_timer.reset()
        print(f"Node {self.id}: Reset election timer after starting new election")
        self.check_win()
        
        # and issues RequestVote RPCs in parallel to each of the other servers in the cluster
        for component in self.config:
            if component['port'] != self.port:
                t = Thread(target=self.ask_for_vote, args=(component,))
                t.start()
        

    def check_win(self):
        self.lock.acquire()
        if self.state == CANDIDATE and self.vote_count >= self.majority_votes:
            self.lock.release()
            print(f"Node {self.id}: Received a majority of votes")
            # it wins the election
            # Once a candidate wins an election, it becomes leader
            self.become_leader()
        else:
            self.lock.release()

    
    def increment_vote_count_and_check_win(self):
        print(f"Node {self.id}: Incrementing vote count")
        with self.lock:
            self.vote_count += 1
        self.check_win()

    def ask_for_vote(self, component):
        with self.lock:
            term = None
            if self.log:
                term = self.log[-1]['term']
            request = {
                'term': self.term,
                'candidateId': self.id,
                'lastLogIndex': len(self.log),                                  # Log Index starts from 1
                'lastLogTerm': term                                             # Term of the last log
            }
        print(f"Node {self.id}: Asking for a vote from node {component['port']} with request {request}")
        response = requests.put(
            "http://" + component['ip'] + ":" + str(component['port']) + "/request_vote",
            json=request
        )
        data = response.json()
        print(f"Node {self.id}: Received a vote from node {component['port']} with response {data}")
        term_check_res = self.term_check(data.get('term'))
        # If vote received
        if term_check_res and data.get('voteGranted'):
            self.increment_vote_count_and_check_win()
        

    def heartbeat(self, called_by_thread=True, prevLogIndex=0, entries=[]):
        print(f"Node {self.id}: Initialised heartbeat")
        heartbeat_threads = []
        for component in self.config:
            if component['port'] != self.port:
                if called_by_thread:
                    print(f"Node {self.id}: Waiting for lock")
                    with self.lock:
                        prevLogIndex = len(self.log)
                    print(f"Node {self.id}: Done with lock")
                t = Thread(target=self.send_internal_message, args=(component,prevLogIndex,entries,))
                heartbeat_threads.append(t)
                t.start()
        print(f"Node {self.id}: Resetting heartbeat timer")
        with self.lock:
            self.heartbeat_timer.reset()
        return heartbeat_threads


    def send_internal_message(self, component, prevLogIndex, entries):
        with self.lock:
            print(f"Node {self.id}: Updating prev log index and entries to send heartbeat")
            if prevLogIndex < 1:
                prevLogTerm = None
            else:
                if len(self.log) >= self.nextIndex[component['port']]:
                    prevLogIndex = self.nextIndex[component['port']] - 1
                    entries = self.log[prevLogIndex:]
                    if prevLogIndex >= 0:
                        prevLogTerm = self.log[prevLogIndex]['term']
                    else:
                        prevLogTerm = None
                else:
                    prevLogTerm = self.log[prevLogIndex-1]['term']

            print(f"Node {self.id}: Checking if commit index needs to be updated")
            commitIndex = self.commitIndex
            N = commitIndex + 1
            while N < len(self.log):
                print(f"Node {self.id}: Checking for N = {N}")
                count = sum(1 for i in self.match_index if i >= N)
                if count > self.majority_votes and self.log[N-1]['term'] == self.term:
                    commitIndex = N
                else:
                    break
                N += 1

            request = {
                'term': self.term,
                'leaderId': self.id,
                'prevLogIndex': prevLogIndex,
                'prevLogTerm': prevLogTerm,
                'entries': entries,
                'leaderCommit': commitIndex
            }
            
            if self.nextIndex[component['port']] < 1:
                return

        print(f"Node {self.id}: Sending heartbeat message to node {component['port']} with request {request}")
        response = requests.put(
            "http://" + component['ip'] + ":" + str(component['port']) + "/append_entries",
            json=request
        )
        data = response.json()
        if data.get('success'):
            print(f"Node {self.id}: Received success heartbeat response")
            with self.lock:
                self.commit_count += 1
                self.nextIndex[component['port']] = len(self.log) + 1
                self.match_index[component['port']] = len(self.log)
            # self.check_and_apply_log()
        else:
            print(f"Node {self.id}: Received failure heartbeat response")
            self.nextIndex[component['port']] -= 1
            #print(f"Node {self.id}: Retrying sending message")
            #self.send_internal_message(component, prevLogIndex, entries)
        print(f"Node {self.id}: Received a response from node {component['port']}: {data}")
        self.term_check(data.get('term'))
        if not data.get('success'):
            self.become_follower()


    def become_follower(self):
        print(f"Node {self.id}: Becoming a follower")
        print(f"Node {self.id}: Stopping heartbeat timer")
        with self.lock:
            self.heartbeat_timer.stop()
            self.state = FOLLOWER
            self.nextIndex = {}
            self.match_index = {}
            self.election_timer.reset()
        print(f"Node {self.id}: Started election timer")


    def become_leader(self):
        print(f"Node {self.id}: Becoming leader")
        # It then sends heartbeat messages to all of the other servers to establish its authority and prevent new
        # elections
        print(f"Node {self.id}: Stopping election timer")
        with self.lock:
            self.state = LEADER
            for component in self.config:
                self.nextIndex[component['port']] = len(self.log)+1
                self.match_index[component['port']] = 0
            self.election_timer.stop()
        print(f"Node {self.id}: Calling heartbeat function")
        self.heartbeat()


    def check_if_candidate(self):
        self.lock.acquire()
        if self.state == CANDIDATE:
            self.lock.release()
            print(f"Node {self.id}: Is a candidate, becoming follower")
            self.become_follower()
        else:
            self.lock.release()


    def term_check(self, term):
        self.lock.acquire()
        # Current terms are exchanged whenever servers communicate
        # If one server’s current term is smaller than the other’s, then it updates its current term to the larger
        # value
        if self.term < term:
            self.lock.release()
            print(f"Node {self.id}: New term is larger than existing term")
            self.set_term(term)
            # If a candidate or leader discovers that its term is out of date, it immediately reverts to follower
            # state
            self.lock.acquire()
            if self.state == CANDIDATE or self.state == LEADER:
                self.lock.release()
                print(f"Node {self.id}: Becoming follower due to larger term received")
                self.become_follower()
            else:
                self.lock.release()
            return True
        elif self.term == term:
            self.lock.release()
            print(f"Node {self.id}: New term is the same as existing term")
            return True
        #If a server receives a request with a stale term number, it rejects the request
        else:                                                                   # self.term > term
            self.lock.release()
            print(f"Node {self.id}: New term is smaller than existing term")
            return False
    

    def set_term(self, term):
        print(f"Node {self.id}: Setting term to {term}")
        with self.lock:
            self.term = term
            self.voted_for = None
            self.vote_count = 0
    

    def log_at_least_up_to_date(self, request):
        if (not request['lastLogTerm'] and self.log):                           # If node has log and leader does not
            print(f"Node {self.id}: Receiver log has long but sender does not")
            return False
        else:
            # If both have logs, neither have logs, or sender has log but receiver does not
            if self.log and request['lastLogTerm'] < self.log[-1]['term']:      # Sender's log less than receiver's
                print(f"Node {self.id}: Sender log has a lower term than receiver log")
                return False
            elif (self.log and request['lastLogTerm'] == self.log[-1]['term']) or not self.log:
                # Sender's log is equal to receiver's
                if len(self.log) > request['lastLogIndex']:                     # Receiver's log is longer
                    print(f"Node {self.id}: Receiver log is longer than sender log")
                    return False
        return True
    

    def vote_if_can(self, request):
        self.lock.acquire()
        # Each server will vote for at most one candidate in a given term, on a first-come-first-served basis
        if (not self.voted_for or self.voted_for == request['candidateId']) and self.log_at_least_up_to_date(request):
            self.lock.release()
            print(f"Node {self.id}: Voting for {request['candidateId']}")
            self.set_term(request['term'])
            self.lock.acquire()
            self.voted_for = id
            self.lock.release()
            return True
        else:
            self.lock.release()
            print(f"Node {self.id}: Cannot vote")
            return False
    

    def check_prev_log_match(self, data):
        with self.lock:
            if len(self.log) > data.get('prevLogIndex') and self.log[data.get('prevLogIndex')-1]['term'] != data.get('prevLogTerm'):
                # If log has prevLogIndex but term does not match
                print(f"Node {self.id}: Sender log has prev log index but term does not match")
                self.log = self.log[:data.get('prevLogIndex')-1]
                return False
            print(f"Node {self.id}: Sender log matches")
            self.log += data.get('entries')
            if data.get('leaderCommit') > self.commitIndex:
                print(f"Node {self.id}: Updating commit index")
                self.commitIndex = min(data.get('leaderCommit'), len(self.log))
            return True
        
    
    def apply(self, logEntry):
        res = None
        if logEntry['command'] == "ADD_TOPIC":
            with self.lock:
                self.message_topics[logEntry['topic']] = []
        elif logEntry['command'] == "GET_TOPICS":
            with self.lock:
                self.endpoint_result = list(self.message_topics.keys())
                res = self.endpoint_result
        elif logEntry['command'] == "PUT_MESSAGE":
            with self.lock:
                self.message_topics[logEntry['topic']].append(logEntry['message'])
        elif logEntry['command'] == "GET_MESSAGE":
            with self.lock:
                self.endpoint_result = self.message_topics[logEntry['topic']].pop(0)
                res = self.endpoint_result
        return res
    

    def add_details_based_on_command(self, data, command):
        print(f"Node {self.id}: Adding details to log for command {command}")
        logEntry = {'command': command}
        if logEntry['command'] == "ADD_TOPIC":
            with self.lock:
                logEntry['topic'] = data.get('topic')
                logEntry['term'] = self.term
        elif logEntry['command'] == "GET_TOPICS":
            with self.lock:
                logEntry['term'] = self.term
        elif logEntry['command'] == "PUT_MESSAGE":
            with self.lock:
                logEntry['topic'] = data.get('topic')
                logEntry['message'] = data.get('message')
                logEntry['term'] = self.term
        elif logEntry['command'] == "GET_MESSAGE":
            with self.lock:
                logEntry['topic'] = data.get('topic')
                logEntry['term'] = self.term
        return logEntry
        
    
    def apply_pending_logs(self):
        res = None
        print(f"Node {self.id}: Applying all pending logs, commit index={self.commitIndex} and last applied={self.lastApplied}")
        self.lock.acquire()
        if self.commitIndex > self.lastApplied and self.lastApplied < len(self.log):
            print(f"Node {self.id}: Incrementing last applied")
            self.lastApplied += 1
            logEntry = self.log[self.lastApplied-1]
            self.lock.release()
            res = self.apply(logEntry)
            self.lock.acquire()
            self.commit_applied = True
            self.lock.release()
        else:
            self.lock.release()
        print(f"Node {self.id}: Applied all pending logs with result {res}")
        return res
    

    def commit_to_log(self, data, command):
        if not command:
            print(f"Node {self.id}: Just a heartbeat, do not commit to log")
            return False, None, None
        print(f"Node {self.id}: Add to log")
        logEntry = self.add_details_based_on_command(data, command)
        with self.lock:
            self.log.append(logEntry)
            prevLogIndex = len(self.log)-1
            entries = self.log[prevLogIndex:]
            self.commitIndex += 1
            self.commit_count = 1
            self.commit_applied = False
        #self.heartbeat(called_by_thread=False, prevLogIndex=prevLogIndex, entries=entries)
        return prevLogIndex, entries


    def check_and_apply_log(self):
        self.lock.acquire()
        while self.commit_count < self.majority_votes:
            self.lock.release()
            time.sleep(0.001)
            self.lock.acquire()
        self.lock.release()
        print(f"Node {self.id}: Majority nodes have accepted commit, apply")
        return self.apply_pending_logs()
        #heartbeat_threads = node.heartbeat()
        #for thread in heartbeat_threads:
            #thread.join()
