- actor pattern seems like a lot of extra effort
- forgetting to initialize channels is a silly mistake
- channels seem to be difficult to debug

bugs i encountered
- Forgetting to send on some channels for the actor model, also in case of failure of rpc
- Leaders handling replies to requests sent in older term, after re-election
- Candidates accepting votes sent in previous elections
- Logic bugs for voting, and who is more up-to-date
- Committing log entries from previous terms
- When follower is inconsistent, clearing the logs before having new ones to replace them with
    - resulted in a rare bug when requests arrive out of order. 1/50 runs of Figure8 test failed
- Spent more time figuring out why i can't send to some channel i didn't initialize than i case to admit
