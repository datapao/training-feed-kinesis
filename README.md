# training-feed-kinesis
Manage Multiple Kinesis agents associated with multiple accounts and feed them with synthesized bank transaction data

## Installation

Launch a [Kinesis Agent compatible instance](https://docs.aws.amazon.com/streams/latest/dev/writing-with-agents.html#prereqs).

1. Set up and activate virtualenv
2. `pip install -r requirements.txt`
3. `sqlite3 app.db < dbschema.sql`
4. Start `generate_transactions.py > /tmp/transactions.log`. This is generating the card transaction data.
5. Start `status_watcher.py`. This component will keep the db up-to-date and manage the streams with AWS.
6. Start the web UI `service.py`. 

