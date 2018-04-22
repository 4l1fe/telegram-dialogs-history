import sqlite3


DB = 'data'
PT_USER = 'user'
PT_CHAT = 'chat'
PT_CHANNEL = 'channel'


def connect(func):

    def wrapped(*args, **kwargs):
        with sqlite3.connect(DB) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            args = args + (cursor, )
            result = func(*args, **kwargs)
            return result

    return wrapped


@connect
def create_tables(cursor):
    cursor.executescript("""CREATE TABLE IF NOT EXISTS dialogs(
                                    id INT8,
                                    type TEXT,
                                    name TEXT NOT NULL,
                                    bin_data BLOB,
                                    PRIMARY KEY (id, type)
                              ) WITHOUT ROWID;
                              CREATE TABLE IF NOT EXISTS messages(
                                    id INT8,
                                    dialog_id INT8,
                                    bin_data BLOB NOT NULL,
                                    PRIMARY KEY (id, dialog_id)
                                    FOREIGN KEY (dialog_id) REFERENCES dialogs(id)
                              ) WITHOUT ROWID;
                              """)


@connect
def save_dialogs(dialogs, cursor):

    def _get_query_values(dialogs):
        for type_, id_, name, d in dialogs:
            bin_data = d.write()
            yield type_, id_, name, sqlite3.Binary(bin_data)

    cursor.executemany("""INSERT INTO dialogs(type, id, name, bin_data)
                          VALUES (?,?,?,?);""", _get_query_values(dialogs))


@connect
def get_dialogs(cursor):
    cursor.execute("""SELECT * FROM dialogs""")
    dialogs = cursor.fetchall()
    return dialogs


@connect
def get_messages(cursor, dialog_ids=None):
    query = "SELECT * FROM messages"
    if dialog_ids:
        if not isinstance(dialog_ids, (list,tuple)):
            dialog_ids = [dialog_ids]
        query += " WHERE dialog_id IN ({})".format(*dialog_ids)
    cursor.execute(query)
    messages = cursor.fetchall()
    return messages


@connect
def save_history(messages, dialog_id, cursor):

    def _get_query_values(messages):
        for message in messages:
            yield message.id, dialog_id, sqlite3.Binary(message.write())

    cursor.executemany("""INSERT INTO messages(id, dialog_id, bin_data) 
                      VALUES (?,?,?)""", _get_query_values(messages))


if __name__ == '__main__':
    create_tables()
    print('created')
