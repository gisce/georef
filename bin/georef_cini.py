#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Actualització dels CINIS donat un fitxer ID;CINI
"""
from datetime import datetime
import codecs
import multiprocessing
import sys
import os

from georef.loop import OOOP
from progressbar import ProgressBar, ETA, Percentage, Bar

sys.stdout = codecs.getwriter("utf-8")(sys.stdout)
N_PROC = min(int(os.getenv('N_PROC', multiprocessing.cpu_count())),
             multiprocessing.cpu_count())

def producer(sequence, output_q):
    """Posem els items que serviran per fer l'informe.
    """
    for item in sequence:
        output_q.put(item)


def consumer(input_q, progress_q):
    """Fem l'informe.
    """
    model = O.normalize_model_name(sys.argv[5])
    proxy = getattr(O, model)
    while True:
        item = input_q.get()
        progress_q.put(item)
        model_id, cini = item.split(';')
        try:
            proxy.write([int(model_id)], {'cini': cini})
        except Exception:
            sys.stderr.write("Write error: inexistent model %s\n" % model_id)
            sys.stderr.flush()
        input_q.task_done()
    
def progress(total, input_q):
    """Rendering del progressbar de l'informe.
    """
    widgets = ['GeoRef informe: ', Percentage(), ' ', Bar(), ' ', ETA()]
    pbar = ProgressBar(widgets=widgets, maxval=total).start()
    done = 0
    while True:
        input_q.get()
        done += 1
        pbar.update(done)
        if done >= total:
            pbar.finish()

def main():
    """Funció principal del programa.
    """
    cinis_file = open(sys.argv[6])
    sequence = cinis_file.readlines()
    cinis_file.close()
    start = datetime.now()
    q = multiprocessing.JoinableQueue()
    q2 = multiprocessing.Queue()
    processes = [multiprocessing.Process(target=consumer, args=(q,q2))
                 for x in range(0, N_PROC)]
    processes += [multiprocessing.Process(target=progress,
                                              args=(len(sequence), q2))]

    for proc in processes:
        proc.daemon = True
        proc.start()
        sys.stderr.write("^Starting process PID: %s\n" % proc.pid)
    sys.stderr.flush()
    producer(sequence, q)
    q.join()
    sys.stderr.write("Time Elapsed: %s\n" % (datetime.now() - start))
    sys.stderr.flush()

if __name__ == '__main__':
    try:
        dbname = sys.argv[1]
        port = int(sys.argv[2])
        user = sys.argv[3]
        pwd = sys.argv[4]
        O = OOOP(dbname=dbname, port=port, user=user, pwd=pwd)
        main()
    except KeyboardInterrupt:
        pass
