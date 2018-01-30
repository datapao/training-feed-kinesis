import sys, os
from datetime import datetime, timedelta
import boto3, sqlite3

sqlite_db = None


def connect_db():
    """Connects to the specific database."""
    rv = sqlite3.connect('app.db')
    rv.row_factory = sqlite3.Row
    return rv


def get_db():
    global sqlite_db
    if sqlite_db == None:
        sqlite_db = connect_db()
    return sqlite_db


def row_dict(row):
    return dict(zip(row.keys(), row))


def get_db_streams():
    stream_curr = get_db().execute(
        'select c.*, s.* FROM streams s INNER JOIN credentials c USING (access_key) ORDER BY name, arn')
    entries = stream_curr.fetchall()
    streams = {}
    for e in entries:
        e_dict = row_dict(e)
        streams[e_dict["arn"]] = e_dict

    return streams


def start_feed(stream):
    pass


def stop_feed(stream):
    pass


def new_expiry_time():
    return datetime.now() + timedelta(hours=7.5)


if __name__ == '__main__':
    db = get_db()

    cur = db.execute('select * FROM credentials ORDER BY name')
    entries = cur.fetchall()

    db_streams = get_db_streams()
    changed_streams = {}
    aws_arns = {}

    for e in entries:
        e = row_dict(e)
        print("Fetching data for {} {}".format(e["name"], e["access_key"]))
        client = boto3.client(
            'kinesis',
            aws_access_key_id=e["access_key"],
            aws_secret_access_key=e["secret_key"],
            region_name='eu-west-1'
        )

        streams_dict = client.list_streams()
        stream_names = streams_dict["StreamNames"]

        for s in stream_names:
            stream_description = client.describe_stream_summary(StreamName=s)
            #print(stream_description)
            aws_status = stream_description['StreamDescriptionSummary']['StreamStatus']
            arn = stream_description['StreamDescriptionSummary']['StreamARN']
            aws_arns[arn] = True

            # If this stream isn't in the DB, add it.
            if arn not in db_streams:
                db_streams[arn] = {
                    'arn': arn,
                    'access_key': e['access_key'],
                    'status': 'NEW',
                    'expiry_time': new_expiry_time(),
                    'state_change': datetime.now(),
                    'name': '==NEW_STREAM=='
                }
                changed_streams[arn] = db_streams[arn]

            db_stream = db_streams[arn]
            old_db_status = db_stream['status']
            if db_stream['status'] == 'NEW':
                if aws_status == 'CREATING':
                    pass
                elif aws_status == 'ACTIVE':
                    start_feed(db_stream)
                    db_stream['status'] = 'RUNNING'
                    changed_streams[arn] = db_stream
                elif aws_status == 'DELETING':
                    stop_feed(db_stream)
                    db_stream['status'] = 'STOPPED'
                    changed_streams[arn] = db_stream
                elif aws_status == 'UPDATING':
                    pass
                else:
                    print('Unexpected statce change. ARN: {}, AWS status: {}, DB Status: {}'.format(
                        arn, aws_status, db_stream['status']),
                        file=sys.stderr)

            elif db_stream['status'] == 'RUNNING':
                if aws_status == 'CREATING':
                    db_stream['status'] = 'NEW'
                    changed_streams[arn] = db_stream
                elif aws_status == "ACTIVE":
                    pass
                elif aws_status == "DELETING":
                    stop_feed(db_stream)
                    db_stream['status'] = 'STOPPED'
                    changed_streams[arn] = db_stream
                elif aws_status == 'UPDATING':
                    pass
                else:
                    print('Unexpected statce change. ARN: {}, AWS status: {}, DB Status: {}'.format(
                        arn, aws_status, db_stream['status']),
                        file=sys.stderr)

            elif db_stream['status'] == 'STOPPED':
                if aws_status == 'CREATING':
                    db_stream['status'] = 'NEW'
                    changed_streams[arn] = db_stream
                elif aws_status == "ACTIVE":
                    start_feed(db_stream)
                    db_stream['status'] = 'RUNNING'
                    changed_streams[arn] = db_stream
                elif aws_status == "DELETING":
                    pass
                elif aws_status == 'UPDATING':
                    pass
                else:
                    print('Unexpected statce change. ARN: {}, AWS status: {}, DB Status: {}'.format(
                        arn, aws_status, db_stream['status']),
                        file=sys.stderr)

            if arn in changed_streams:
                print("{}/{}: AWS:{} OLD:{} NEW:{}".format(db_stream['name'], arn, aws_status, old_db_status, changed_streams[arn]['status']))

        # Stop streams that got removed from AWS manually.
        for arn in db_streams:
            db_stream = db_streams[arn]
            if (db_stream['status'] != 'STOPPED') and (arn not in aws_arns):
                stop_feed(db_streams[arn])
                db_stream['status'] = 'STOPPED'
                changed_streams[arn] = db_stream
                print("{}/{}: {} -> {}".format(db_stream['name'], arn, "N/A", changed_streams[arn]['status']))


    # Update DB
    for arn in changed_streams:
        s = changed_streams[arn]
        db.execute('REPLACE INTO STREAMS (arn, access_key, status, state_change, expiry_time) values (?, ?, ?, ?, ?)',
                   [s['arn'], s['access_key'], s['status'], datetime.now(), s['expiry_time']]);

    db.commit()
