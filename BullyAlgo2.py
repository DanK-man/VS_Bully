import threading
import queue
from enum import Enum


# Signalarten
class Signal(Enum):
    STOP = 0
    START_ELECT = 1
    ELECT = 2
    OK = 3
    COORD = 4


# Container für die Nachrichten, die herumgeschickt werden
class Message:
    def __init__(self, signal, payload=None, timestamp=0):
        self.signal = signal
        self.payload = payload
        self.timestamp = timestamp


def process(id, mqueues, DEBUG=False, ttimeout=3):
    is_electing = False
    TTIMEOUT = ttimeout
    clock = 0

    def __timeout__():
        print('Timeout!')
        for n in range(len(mqueues)):
            if DEBUG: print('COORD-Nachricht von Prozess ', id, ' an Prozess ', n, ' gesendet.')
            mqueues[n].put(Message(Signal.COORD, id))
        print('~~ Neuer Koordinator: ', id, ' ~~')

    timer_list = [(threading.Timer(TTIMEOUT, __timeout__), clock)]  # Sammelt die Timer
    clock += 1

    while True:
        # Nachricht abholen, ansonsten blockieren
        item = mqueues[id].get()

        # Thread stoppen
        if item.signal == Signal.STOP:
            if len(timer_list) > 0: timer_list[-1][0].cancel()
            return
        # Wahl starten
        elif item.signal == Signal.START_ELECT:
            for n in range(id + 1, len(mqueues)):
                if DEBUG: print('ELECT-Nachricht von Prozess ', id, ' an Prozess ', n, ' gesendet.')
                mqueues[n].put(Message(Signal.ELECT, id, clock), )
            # Timeout für Gewinnen der Wahl setzen
            if DEBUG: print('Prozess ', id, ' startet timer.')
            timer_list.append((threading.Timer(TTIMEOUT, __timeout__), clock))
            timer_list[-1][0].start()
            clock += 1
        elif item.signal == Signal.ELECT:
            if item.payload < id:
                if DEBUG: print('OK-Nachricht von Prozess ', id, ' an Prozess ', item.payload, ' gesendet.', is_electing)
                mqueues[item.payload].put(Message(Signal.OK, id, item.timestamp))
                # Wenn noch keine eigene Wahl gestartet -> neue Wahl
                if not is_electing:
                    mqueues[id].put(Message(Signal.START_ELECT))
                    is_electing = True
        elif item.signal == Signal.OK:
            if DEBUG: print(id, ' hat OK von ', item.payload, ' erhalten.')
            for timer, tstamp in timer_list:
                if tstamp == item.timestamp:
                    timer.cancel()

        # Koordinator gefunden, Wahl abschließen
        elif item.signal == Signal.COORD:
            is_electing = False



num_processes = 1000
ttimeout = 15
my_threads = []
my_queues = [queue.Queue() for _ in range(num_processes)]


# Prozesse initialisieren, Threads starten
for n in range(num_processes):
    thread = threading.Thread(target=process, args=(n, my_queues, False, ttimeout))
    my_threads.append(thread)
    thread.start()
    print('Starte Prozess ', n)

read_in = input('> ')
while read_in != 'exit':

    # Stoppe Prozess
    if read_in.startswith('s'):
        idx = int(read_in[1:])
        print('STOPP-Signal an Prozess ', idx)
        my_queues[idx].put(Message(Signal.STOP))
        my_threads[idx].join()

    # Prozess auswählen, der die Election starten soll
    elif read_in.startswith('e'):
        idx = int(read_in[1:])
        print('START_ELECT-Signal an Prozess ', idx)
        my_queues[idx].put(Message(Signal.START_ELECT))
    read_in = input('> ')

# Alle Prozesse beenden
for q in my_queues:
    q.put(Message(Signal.STOP))
for idx, p in enumerate(my_threads):
    print('Prozess ', idx, ' wird gestoppt...')
    p.join()