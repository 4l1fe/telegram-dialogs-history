import sqlite3
from peewee import *
from pyrogram.api import Object
from io import BytesIO


DB = 'data'
db = SqliteDatabase(DB)


class Dialog(Model):

    id = AutoField(primary_key=True)
    dialog_id = IntegerField()
    type = TextField()
    name = TextField()
    bin_data = BlobField()

    class Meta:
        database = db
        tablename = 'dialogs'
        constraints = [SQL('UNIQUE (dialog_id, type)')]


class Message(Model):

    id = IntegerField()
    dialog = ForeignKeyField(Dialog, backref='messages')
    bin_data = BlobField()

    class Meta:
        database = db
        tablename = 'messages'
        primary_key = CompositeKey('id', 'dialog')

    @property
    def message(self):
        msg = Object.read(BytesIO(self.bin_data))
        return msg


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
def get_messages(cursor, dialog_ids=None):
    query = "SELECT * FROM messages"
    if dialog_ids:
        if not isinstance(dialog_ids, (list,tuple)):
            dialog_ids = [dialog_ids]
        query += " WHERE dialog_id IN ({})".format(*dialog_ids)
    cursor.execute(query)
    messages = cursor.fetchall()
    return messages


if __name__ == '__main__':
    db.connect()
    db.create_tables([Dialog, Message])
    print('created')
