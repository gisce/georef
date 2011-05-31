# -*- coding: utf-8 -*-
"""Actualització dels CINIS donat un fitxer ID;CINI
"""
from datetime import datetime
import codecs
import multiprocessing
import sys

from georef.loop import OOOP

sys.stdout = codecs.getwriter("utf-8")(sys.stdout)

def producer(sequence, output_q):
    """Posem els items que serviran per fer l'informe.
    """
    for item in sequence:
        output_q.put(item)

def consumer(input_q):
    """Fem l'informe.
    """
    model = O.normalize_model_name(sys.argv[5])
    proxy = getattr(O, model)
    while True:
        item = input_q.get()
        model_id, cini = item.split(';')
        proxy.write([int(model_id)], {'cini': cini})
        input_q.task_done()
        

def main():
    """Funció principal del programa.
    """
    start = datetime.now()
    q = multiprocessing.JoinableQueue()
    processes = [multiprocessing.Process(target=consumer, args=(q,))
                 for x in range(0, multiprocessing.cpu_count())]
    for proc in processes:
        proc.daemon = True
        proc.start()
        sys.stderr.write("^Starting process PID: %s\n" % proc.pid)
    sys.stderr.flush()
    cinis_file = open(sys.argv[6])
    sequence = cinis_file.readlines()
    cinis_file.close()
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
