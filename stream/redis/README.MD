1. use `XAUTOCLAIM` to read pending message which was not ack after 10min (600000)
  - handle the messages pending for more than 10 minutes
  - use `XACK` to remove messages from pending entries list
2. use `XREADGROUP` to consume the latest message
  - hadnle the new messages
  - use `XACK` to remove messages from pending entries list
3. TODO: deploy regular job (eg. 1 day) to clean the ack-ed messages in queue 
  - use `XPENDING` to check the pending messages id from stream
  - if there is pending messages and oldest pending messages stay > regular period
    - log the message id and trim messages stay longer than regular job period 
  - use `XTRIM` to trim the message stay longer then regular job period
