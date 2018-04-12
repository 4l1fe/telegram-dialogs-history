import os
import lmdb
import logging
from pyrogram import Client
from pyrogram.api import functions, types


logger = logging.getLogger('historian')
API_ID = os.environ['API_ID']
API_HASH = os.environ['API_HASH']
DB_ENV = 'data'
DIALOGS_DB = b'dialogs'
MESSAGES_DB = b'messages'
MB = 1024 * 1024


def save_dialogs(dialogs, env):

    def _make_key(peer):
        key = 'dialog:{type}:{id}'
        if isinstance(peer, types.PeerUser):
            type_ = 'user'
            id_ = peer.user_id
        elif isinstance(peer, types.PeerChat):
            type_ = 'chat'
            id_ = peer.chat_id
        elif isinstance(peer, types.PeerChannel):
            type_ = 'channel'
            id_ = peer.channel_id
        else:
            TypeError(f'undefined kind of dialog {peer}')

        return key.format(type=type_, id=id_).encode()

    putted = 0
    db = env.open_db(DIALOGS_DB, dupsort=False)
    with env.begin(db=db, write=True) as txn:
        for dialog in dialogs:
            key = _make_key(dialog.peer)
            value = dialog.write()
            is_putted = txn.put(key, value, overwrite=False)
            if is_putted:
                putted += 1
            logger.debug(dialog)

    return putted


def show_dialogs():pass


def save_history(messages, env):
    import sys
    db = env.open_db(MESSAGES_DB, dupsort=True)
    size_count = 0
    with env.begin(db=db, write=True) as txn:
        for i, message in enumerate(messages):
            key = b'message'
            value = message.write()
            size_count += len(value)
            size = sys.getsizeof(value)
            logger.info(f'{i}, {size_count}, {size}')
            if size > 200:
                logger.warning(f'{size}')
                continue

            txn.put(key, value, append=True)


def main():
    env = lmdb.open(DB_ENV, max_dbs=100, map_size=100*MB)
    client = Client('session', api_key=(API_ID, API_HASH))
    client.start()

    dslice = client.send(functions.messages.GetDialogs(0, 0, types.InputPeerEmpty(), 100, exclude_pinned=False))
    saved = save_dialogs(dslice.dialogs, env)
    logger.info(f'saved {saved} dialogs')

    type_, id_ = input('t i: ').split()
    offset = 0
    limit = 100
    saved = 0
    while True:
        hist = client.send(functions.messages.GetHistory(types.InputPeerChat(int(id_)), 0, 0, offset, limit, 0, 0, 0, ))
        if not hist.messages:
            break
        save_history(hist.messages, env)
        saved += len(hist.messages)
        offset += limit
    logger.info(f'saved {saved} messages')
    client.stop()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
