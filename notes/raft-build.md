- actor pattern seems like a lot of extra effort
- forgetting to initialize channels is a silly mistake
- channels seem to be difficult to debug

bugs i encountered
- Forgetting to send on some channels for the actor model, also in case of failure of rpc
- Leaders handling replies from followers in older term, after re-election
- Logic bugs for voting, and who is more up-to-date
- When follower is inconsistent, clearing the logs before having new ones to replace them with
    - resulted in a rare bug when requests arrive out of order. 1/50 runs of Figure8 test failed
