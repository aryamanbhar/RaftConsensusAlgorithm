#!/usr/bin/env python3

import argparse, socket, time, json, select, struct, sys, math, os
import random

# Implement responses

BROADCAST = "FFFF"
T = 150

APPEND_ENTRIES_RESPONSE = "APPEND_ENTRIES_RESPONSE"
APPEND_ENTRIES          = "APPEND_ENTRIES"
REQUEST_VOTE            = "REQUEST_VOTE"
GIVE_VOTE               = "GIVE_VOTE"
CANDIDATE               = "CANDIDATE"
FOLLOWER                = "FOLLOWER"
LEADER                  = "LEADER"
COMMIT                  = "COMMIT"
COMMITTED               = "COMMITTED"
PUT                     = "put"
GET                     = "get"

class Replica:
    def __init__(self, port, id, others):
        
        self.port   = port
        self.id     = id
        self.others = others
        self.status = FOLLOWER # Every replica starts as a FOLLOWER
        
        self.currentTerm = 0          # Initially, it is the first term.
        self.votedFor    = None       # Id of the candidate voted for during an election.
        self.leader      = BROADCAST  # Initially, leader is BROADCAST ("FFFF") as it is unknown.
        self.log         = []         # Log to maintain entries
        self.datastore   = {}         # State machine (data-store) where logs will be applied
        self.voteCount   = []          # Keeps track of the votes if Replica is a Candidate during an election
        
        
        # Volatile state on leaders
        # Below needs to be re-initialized after every election.
        self.nextIndex  = {}
        self.matchIndex = {}
        
        # We initialize all to 0 for all replicas
        for replicaId in others:
            self.nextIndex[replicaId]  = 0
            self.matchIndex[replicaId] = -1
        
        # Volatile State on all servers
        self.commitIndex = -1 # Index of the highest log entry known to be committed.
        self.lastApplied = -1 # Index of the highest log entry applied on state machine.
        
        self.electionTimeout = random.uniform(T, 2*T)  # Election timeout, if expired then election is called.
        self.timerStart      = time.time() * 1000      # Timer to keep track of timeouts
        self.heartBeat       = 100   # Heart Beat timeout to keep track of 
        
        self.clientBuffer = []
        
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('localhost', 0))
        self.socket.setblocking(False)

        print("Replica %s starting up" % self.id, flush=True)
        hello = { "src": self.id, "dst": BROADCAST, "leader": BROADCAST, "type": "hello" }
        self.send(hello)
        print("Sent hello message: %s" % hello, flush=True)


    def send(self, message):
        self.socket.sendto(json.dumps(message).encode('utf-8'), ('localhost', self.port))

    
    # startElection() calls the election and requests votes from other replicas.
    def startElection(self):
        
        self.status       = CANDIDATE  # Becomes a Candidate
        self.currentTerm  = self.currentTerm + 1 # Increments its current term
        self.votedFor     = self.id    # votedFor itself
        self.voteCount    = [self.id]  # has one vote initially
        self.leader       = BROADCAST
        
        lastLogTerm  = 0
        if self.log:
            lastLogTerm  = self.log[-1]['term']
            
        requestVoteRPC = {
            "src": self.id,
            "dst": BROADCAST,
            "leader": self.leader, 
            "type": REQUEST_VOTE,
            "cid": self.id,
            'MID': str(random.randint(1000000, 9999999)),
            "term": self.currentTerm,
            "lastLogIndex": len(self.log) - 1,
            "lastLogTerm": lastLogTerm,
            "logSize": len(self.log)}
        
        self.send(requestVoteRPC)
        
        self.timerStart = time.time() * 1000 # Timer reset
        
    
    
    # When other servers that are still followers give their vote
    def handleRequestVoteRPC(self, msg):
        
        if msg['term'] > self.currentTerm:
            self.currentTerm = msg['term']
            self.status      = FOLLOWER
            self.votedFor    = None
            self.voteCount   = []
            # self.leader      = msg['leader']
        
        if self.status == FOLLOWER and msg['cid'] != self.id:
            # Checking uptodate
            # voteGranted = self.currentTerm <= msg['term']
            
            lastLogTerm = 0 if not self.log else self.log[-1]['term']
            
            logOK = (msg['lastLogTerm'] > lastLogTerm) or (msg['lastLogTerm'] == lastLogTerm and msg['logSize'] >= len(self.log))
            
            voteGranted = self.currentTerm and logOK and self.votedFor in [msg['cid'], None]
            
            # if not self.votedFor: # This basicallly checks if voted has been casted already
            if voteGranted:
                self.votedFor = msg['cid']
                self.timerStart = time.time() * 1000 # We reset the timer
            
            vote = {
                "src": self.id,
                "dst": msg['cid'],
                "leader": self.leader,
                "type": GIVE_VOTE,
                "term": self.currentTerm,
                "voteGranted": voteGranted}
                
            self.send(vote)
        
        
    
    # This function handles the vote if given
    def handleVote(self, msg):
        
        if self.status == CANDIDATE and msg['term'] == self.currentTerm and msg['voteGranted']:
            
            if msg['src'] not in self.voteCount:
                self.voteCount += [msg['src']]
            
            if len(self.voteCount) >= 2:
                self.status = LEADER
                self.leader = self.id
                self.timerStart = time.time()
                for id in self.others:
                    self.nextIndex[id]  = len(self.log)
                    self.matchIndex[id] = -1
                self.voteCount = []
                    
                self.appendEntries(BROADCAST, len(self.log) - 1, [])
        
        elif msg['term'] > self.currentTerm:
            self.currentTerm = msg['term']
            self.status      = FOLLOWER
            self.votedFor    = None
            self.voteCount   = []
            self.timerStart  = time.time()


    # Function to send an AppendEntries RPCs
    def appendEntries(self, dst, prevLogIndex, entries):
        
        if self.status != LEADER or self.leader != self.id:
            return
        
        self.timerStart = time.time() * 1000 # We are sending an append entry, it may be a heartbeat or otherwise,
                                             # but it is more efficient to restart timer to avoid extra heartbeats

        prevLogTerm = 0
        if self.log and prevLogIndex >= 0:
            prevLogTerm = self.log[prevLogIndex]['term']
        
        appendEntriesRPC = {
            "src": self.id,
            "dst": dst,
            "type": APPEND_ENTRIES,
            "term": self.currentTerm,
            "leader": self.leader,
            'MID': str(random.randint(1000000, 9999999)), 
            "entries": entries, # This passes all entries after lastLogIndex
            "prevLogIndex": prevLogIndex,
            "prevLogTerm": prevLogTerm, # term of prevLogIndex Entry
            "leaderCommit": self.commitIndex}
        
        self.send(appendEntriesRPC)
            
    def handleAppendEntries(self, msg):
        
        self.timerStart = time.time() * 1000
        
        '''
        1. Reply false if term < currentTerm (§5.1)
        2. Reply false if log doesn’t contain an entry at prevLogIndex
        whose term matches prevLogTerm
        '''
        # if msg['prevLogIndex'] >= 0:
        if msg['term'] < self.currentTerm \
            or msg['prevLogIndex'] >= len(self.log):
            # We return False if
            # 1. term < self.currentTerm
            # 2. our lastLogIndex < leader's prevLogIndex
            # 3. lastlogIndex = prevLogIndex of leader, but term is not the same.
            
            self.check_term(msg)
            
            response = {
                'src': self.id,
                'dst': msg['src'],
                'leader': self.leader,
                'type': APPEND_ENTRIES_RESPONSE,
                'term': self.currentTerm,
                'success': False,
                'replicaCommit': self.commitIndex
            }
            self.send(response)
            return
        
        if msg['prevLogIndex'] >= 0 and self.log[msg['prevLogIndex']]['term'] != msg['prevLogTerm']:
            response = {
                'src': self.id,
                'dst': msg['leader'],
                'leader': msg['leader'],
                'type': APPEND_ENTRIES_RESPONSE,
                'term': self.currentTerm,
                'success': False,
                'replicaCommit': self.commitIndex
            }
            self.send(response)
            return
    
        self.check_term(msg)
        '''
        3. If an existing entry conflicts with a new one (same index
        but different terms), delete the existing entry and all that
        follow it (§5.3)
        '''
        i, j = msg['prevLogIndex'] + 1, 0
        
        while i < len(self.log) and j < len(msg['entries']):
            
            print(f"LOG SIZE: {len(self.log)}, i: {i}, ENTRIES SIZE: {len(msg['entries'])}, j: {j}", flush=True)
            if self.log[i]['term'] != msg['entries'][j]['term']:
                break
            
            i += 1
            j += 1
        
        self.log = self.log[:i]
        
        '''
        4. Append any new entries not already in the log.
        '''
        self.log += msg['entries'][j:]
        
        '''
        5. If leaderCommit > commitIndex, set commitIndex =
        min(leaderCommit, index of last new entry)
        '''
        
        if msg['leaderCommit'] > self.commitIndex:
            self.commitIndex = min(msg['leaderCommit'], len(self.log) - 1)
            
        self.leader = msg['leader']
        
        if msg['entries']:
            response = {
                'src': self.id,
                'dst': msg['leader'],
                'leader': self.leader,
                'type': APPEND_ENTRIES_RESPONSE,
                'term': self.currentTerm,
                'success': True,
                'upto': len(self.log)
            }
            self.send(response)
        else:
            print(f"HEART BEAT RECEIVED BY REPLICA: {self.id}, entries: {msg['entries']}")
        
    def handleAppendEntriesResponse(self, msg):
        
        # print("RE")
        if msg['term'] == self.currentTerm and self.status == LEADER:
            if msg['success'] and msg['upto'] >= self.matchIndex[msg['src']]:
                self.nextIndex[msg['src']]  = msg['upto'] + 1
                self.matchIndex[msg['src']] = msg['upto']
                # self.commitLogEntries()
                
            elif self.nextIndex[msg['src']] > 0:
                self.nextIndex[msg['src']] = self.nextIndex[msg['src']] - 1

        self.check_term(msg)
        
     
    def redirect(self, msg):
        
        temp = msg.copy()
        temp['src']  = self.id
        temp['dst']  = msg['src']
        temp['type'] = 'redirect'
        temp['leader'] = self.leader
        
        self.send(temp)

    # Simply handles failed message
    def failed_msg(self, msg):
        
        temp = msg.copy()
        temp['src'] = self.id
        temp['dst'] = msg['src']
        temp['leader'] = self.leader
        temp['type'] = 'fail'
        
        self.send(temp)
    
    def check_term(self, msg):
        
        if msg['term'] > self.currentTerm:
            self.status      = FOLLOWER
            self.currentTerm = msg['term']
            self.voteCount   = []
            self.votedFor    = None
            self.leader      = msg['leader']
        
    def commitNow(self):
        
        while self.commitIndex > self.lastApplied:
            
            self.lastApplied += 1
            
            operation = self.log[self.lastApplied]
            
            if operation['type'] == GET:
                if self.status == LEADER and self.leader == self.id:
                    key = self.log[self.lastApplied]['key']
                    MID = self.log[self.lastApplied]['MID']
                    src = self.log[self.lastApplied]['src']
                    
                    response = {
                        "src":   self.id,
                        "dst":  src,
                        "leader": self.leader,
                        "type": "ok",
                        "key": key,
                        "value": self.datastore.get(key, ""),
                        "MID": MID
                    }
                    
                    self.send(response)
                    
            elif operation['type'] == PUT:
                key = self.log[self.lastApplied]['key']
                
                # Committing a PUT Request
                val = self.log[self.lastApplied]['val']
                self.datastore[key] = val # Commit

                if self.status == LEADER:
                    MID = self.log[self.lastApplied]['MID']
                    src = self.log[self.lastApplied]['src']
                    
                    response = {
                        "src":    self.id,
                        "dst":    src,
                        "leader": self.leader,
                        "type":   "ok",
                        "MID":    MID}
                    
                    self.send(response)
        
    def updateCommitIndex(self):
        
        if self.status == LEADER:
            for N in range(self.commitIndex + 1, len(self.log)):
                count = sum([1 if matchIndex > self.commitIndex else 0 for matchIndex in self.matchIndex.values()])
                if self.log[N]['term'] == self.currentTerm and \
                    count > (len(self.others) + 1) / 2:
                        self.commitIndex = N
                else:
                    break
    
        
    def handle_msg(self, msg):
        
        if msg["type"] == REQUEST_VOTE: # Status can be CANDIDATE in case there is a situation where two nodes become leaders
            # self.check_term(msg)
            self.handleRequestVoteRPC(msg)
            
            print(f"Replica {self.id}: I have voted for {self.votedFor}, but vote for me guys!", flush=True)        
        
        # Function for determining if that candidate won the election
        elif msg["type"] == GIVE_VOTE and self.status == CANDIDATE:
            
            # self.check_term(msg)
            self.handleVote(msg)
            
        elif msg['type'] == APPEND_ENTRIES:
            
            self.handleAppendEntries(msg)
            
        elif msg['type'] == APPEND_ENTRIES_RESPONSE:
            
            # self.check_term(msg)
            self.handleAppendEntriesResponse(msg)
        
        elif msg['type'] == PUT:
            
            # self.check_term(msg)
            
            if self.leader == BROADCAST:
                self.clientBuffer.append(msg)
            elif self.status != LEADER:
                self.redirect(msg)
            else:
                entry = {
                    'key': msg['key'],
                    'val': msg['value'],
                    'MID': msg['MID'],
                    'type': PUT,
                    'term': self.currentTerm,
                    'src': msg['src']
                }
                self.log.append(entry)
                
                for id in self.others:
                    if self.log and len(self.log) > self.nextIndex[id]:
                        
                        entries = self.log[self.nextIndex[id]:min(len(self.log), self.nextIndex[id] + 10)]
                        self.appendEntries(id, self.nextIndex[id] - 1, entries)
            
            # self.handle_put(msg)
            
        elif msg['type'] == GET:
            
            if self.leader == BROADCAST:
                self.clientBuffer.append(msg)
            elif self.status == FOLLOWER:
                self.redirect(msg)
            else:
                entry = {
                    'key': msg['key'],
                    'MID': msg['MID'],
                    'type': GET,
                    'term': self.currentTerm,
                    'src': msg['src']
                }
                self.log.append(entry)
                
                for id in self.others:
                    if self.log and len(self.log) > self.nextIndex[id]:
                        
                        entries = self.log[self.nextIndex[id]:min(len(self.log), self.nextIndex[id] + 10)]

    def run(self):
        
        # Try 3 things:
        # 1. Upto Log Index in success
        # 2. Heartbeats optimization
        # 3. 
        while True:
            self.commitNow()
            try:  
                
                if self.status == LEADER:
                    self.updateCommitIndex()

                    if time.time() * 1000 >= self.timerStart + self.heartBeat:
                        self.appendEntries(BROADCAST, len(self.log) - 1, [])
                
                # CANDIDATES AND FOLLOWERS only can start new elections
                if time.time() * 1000 >= self.timerStart + self.electionTimeout and self.status != LEADER:
                    print(f"ELECTION STARTED BY REPLICA {self.id}")
                    self.startElection()
                
                msg = None
                
                if self.leader != BROADCAST and self.clientBuffer:
                    msg = self.clientBuffer.pop(0)
                    print(f"POP FROM BUFFER: {msg}", flush=True)
                    
                    self.handle_msg(msg)
                else:
                    
                    # data, addr = self.socket.recvfrom(65535)
                    # msg = data.decode('utf-8')
                    # msg = json.loads(msg)
                    replicas, _, _ = select.select([self.socket], [], [], 0.1)
                    for r in replicas:
                        data, addr = r.recvfrom(65535)
                        full_message = data.decode('utf-8')
                        msg = json.loads(full_message)
                        
                        
                        print("Received message '%s'" % (msg,), flush=True)
                        self.handle_msg(msg)
                    
                print(f"LOG LENGTH: {len(self.log)} COMMITED: {self.commitIndex}")
                
            
            except BlockingIOError:
                continue
            
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='run a key-value store')
    parser.add_argument('port', type=int, help="Port number to communicate")
    parser.add_argument('id', type=str, help="ID of this replica")
    parser.add_argument('others', metavar='others', type=str, nargs='+', help="IDs of other replicas")
    args = parser.parse_args()
    replica = Replica(args.port, args.id, args.others)
    replica.run()