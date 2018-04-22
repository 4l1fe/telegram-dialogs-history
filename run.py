#!venv/bin/python
import logging
import db
from io import BytesIO
from pyrogram import Client
from pyrogram.api import functions, types, Object
from db import PT_USER, PT_CHAT, PT_CHANNEL


logger = logging.getLogger('historian')
API_ID = os.environ['API_ID']
API_HASH = os.environ['API_HASH']


#todo file saving


def show_dialogs():
    dialogs = db.get_dialogs()
    for d in dialogs:
        print(d['type'], d['id'], d['name'])


def show_messages(dialog_ids=None):
    messages = db.get_messages(dialog_ids=dialog_ids)
    for m in messages:
        b = BytesIO(m['bin_data'])
        message = Object.read(b)
        if isinstance(message, types.message.Message):
            print(message.message)
        # else:
        #     print(message)


def get_dialog_type_id(dialog):
    if isinstance(dialog.peer, types.PeerUser):
        type_ = PT_USER
        id_ = dialog.peer.user_id
    elif isinstance(dialog.peer, types.PeerChat):
        type_ = PT_CHAT
        id_ = dialog.peer.chat_id
    elif isinstance(dialog.peer, types.PeerChannel):
        type_ = PT_CHANNEL
        id_ = dialog.peer.channel_id
    else:
        TypeError(f'undefined kind of dialog {dialog}')

    return type_, id_


def main():
    client = Client('session', api_key=(API_ID, API_HASH))
    client.start()

    def _get_dialog_name(type_, id_, client):
        try:
            ip = client.resolve_peer(id_)
        except Exception as e:
            print(type_, id_, e)

        if type_ == db.PT_CHANNEL:
            chats = client.send(functions.channels.GetChannels([ip, ]))
            channel = chats.chats[0]
            name = channel.title
        elif type_ == db.PT_CHAT:
            chats = client.send(functions.messages.GetChats([id_]))
            chat = chats.chats[0]
            name = chat.title
        elif type_ == db.PT_USER:
            fu = client.send(functions.users.GetFullUser(ip))
            name = fu.user.first_name
        else:
            TypeError(f'undefined type {type_}')

        return name

    def _extract_dialogs_data(dialogs):
        data = []
        for d in dialogs:
            type_, id_ = get_dialog_type_id(d)
            name = _get_dialog_name(type_, id_, client)
            data.append((type_, id_, name, d))
        return data

    # def _get_offset_date(dslice):
    #     for m in reversed(dslice.messages):
    #         if isinstance(m, types.MessageEmpty):
    #             continue
    #
    #         return m.date
    #
    #     return 0

    # offset_date, limit = 0, 20
    # saved = 0
    # while True:
    dslice = client.send(functions.messages.GetDialogs(0, 0, types.InputPeerEmpty(), 100))
        # if not dslice.dialogs:
        #     break
    dialogs = _extract_dialogs_data(dslice.dialogs)
    db.save_dialogs(dialogs)
        # saved += len(dslice.dialogs)
        # offset_date = _get_offset_date(dslice)
    # logger.info(f'saved {saved} messages')

    show_dialogs()
    id_ = input('id: ')
    # id_ = 296309357
    offset, limit = 0, 100
    saved = 0
    peer = client.resolve_peer(int(id_))
    while True:
        hist = client.send(functions.messages.GetHistory(peer, 0, 0, offset, limit, 0, 0, 0, ))
        if not hist.messages:
            break
        db.save_history(hist.messages, id_)
        saved += len(hist.messages)
        offset += limit
    logger.info(f'saved {saved} messages')
    client.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
