High-Level Approach:

Our approach for implementing the RAFT consensus algorithm involved structuring our Replica class to handle multiple responsibilities:

Leader Election: On startup or leader failure, replicas can initiate elections to choose a new leader. 
Log Replication: Leaders replicate logs to followers to maintain consistent state across all replicas.
Client Interaction: Replicas handle put and get requests from clients, redirecting if not the leader 
Heartbeat Mechanism: Leaders regularly send heartbeat messages to maintain authority and prevent unnecessary elections periodically

Challenges Faced:

Leader Election Stability: Ensuring that only one leader is elected per term and not having many elections was the biggest challenge. This involved debugging our election timeout and managing vote requests and responses properly. 
Log Consistency: The solution involved implementing significant checks before log entries are committed and applying entries to the state machine only after ensuring they are replicated on a majority of servers. It required a significant amount of time with handling the AppendEntries related calls. 

Good Design Features:
Modular Design: The codebase is structured into functions and methods that handle specific tasks such as voting, appending entries, and responding to client requests, which simplifies understanding and maintenance. 
Robust Error Handling: The system is designed to handle errors gracefully, ensuring the continuity of service even when network issues occur or nodes fail.
State Machine Safety: Ensures that the state machine (key-value store) is only updated upon commitment of entries, preventing stale or incorrect reads.

Testing Strategy:
Unit Testing: Each major component (election, log replication, client interaction) was unit tested to ensure it performs as expected in isolation.
Performance Measurement: Used the performance test analysis provided at the end of the simulation to help improve latency and to not oversend too many messages between the replicas
Edge Cases: Special attention was given to edge cases such as leadership changes during a client request and simultaneous leader elections. 


