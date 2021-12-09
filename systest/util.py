from time import sleep


def journalpump_initialized(journalpump):
    retry = 0
    senders = []
    while retry < 3 and not senders:
        sleep(1)
        readers = [reader for _, reader in journalpump.readers.items()]
        senders = []
        if readers:
            for reader in readers:
                senders.extend([sender for _, sender in reader.senders.items()])
        retry += 1

    return journalpump.running and senders
