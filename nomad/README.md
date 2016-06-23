# Server Notes

If all server restart the quorum is broken, to fix it:

1. Shutdown the mayority of the servers.

2. Overwrite the file ```/tmp/nomad/server/raft/peers.json``` adding all the nomad servers, example:

  ```json
  ["10.0.150.105:4647","10.0.150.103:4647","10.0.150.104:4647"]
  ```
  
3. Start all the servers again.

