Flotilla is a consensus, embedded and programmable database.

Consensus -- All changes to a Flotilla database are replicated in a consistent order across all machines in the Flotilla cluster.

Embedded -- All read transactions are from local storage.  Doesn't expose any external network APIs.

Programmable -- All write modifications are done via user-defined functions.  This is done to process the same state machine in the same order on all machines.  There are example user-defined functions in lib.go and the examples on this page.


