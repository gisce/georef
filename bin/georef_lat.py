#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Georeferenciaci de línies.
FORMULARIO 5: “Información relativa a la topología y atributos de las líneas
aéreas y cables subterráneos reales existentes a 31 de diciembre de 2010”
"""
import sys
import os
import codecs
import multiprocessing
import pprint
from datetime import datetime

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


def consumer(input_q, output_q, progress_q):
    """Fem l'informe.
    """
    codi_r1 = sys.argv[5][-3:]
    while True:
        item = input_q.get()
        progress_q.put(item)
        linia = O.GiscedataAtLinia.get(item)
        for tram in linia.trams:
            if tram.baixa:
                continue
            if tram.cable and tram.cable.tipus:
                o_cable_codi = tram.cable.tipus.codi
                if o_cable_codi == 'I':
                    if tram.tipus == 1:
                        o_cable_codi = 'D'
                    else:
                        o_cable_codi = 'S'
            else:
                sys.stderr.write("**** ERROR: El tram %s linia %s no té "
                                 "cable o tipus\n" % (tram.name,
                                                      linia.name))
                continue
            output_q.put([
                'R1-%s' % codi_r1.zfill(3),
                '%s-%s' % (linia.name, tram.name),
                tram.origen and tram.origen[:20] or '',
                tram.final and tram.final[:20] or '',
                o_cable_codi,
                linia.tensio,
                tram.circuits or 1,
                round(tram.longitud_cad / 1000.0, 3) or 0,
                tram.cini or '',
                1,
            ])
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
    sequence = []
    search_params = [('name', '!=', '1')]
    sequence += O.GiscedataAtLinia.search(search_params)
    sys.stderr.write("Filtres utilitzats:\n")
    pprint.pprint(search_params, sys.stderr)
    sys.stderr.write("S'han trobat %s línies. Correcte? " % len(sequence))
    sys.stderr.flush()
    raw_input()
    search_params = [('active', '=', 1),
                     ('baixa', '=', 1)]
    trams_indef = O.GiscedataAtTram.search(search_params)
    if trams_indef:
        long_cad = 0
        for tram in O.GiscedataAtTram.read(trams_indef, ['longitud_cad']):
            long_cad += tram['longitud_cad']
        sys.stderr.write("*** ATENCIÓ ***\n")
        sys.stderr.write("S'han trobat %i trams que estan actius i de baixa al "
                         "mateix temps. Sumen %f m.\n" % (len(trams_indef),
                                                          long_cad))
        sys.stderr.write("Si estan marcats com a baixa, no s'inclouran en "
                         "l'informe. Continuar igualment? ")
        sys.stderr.flush()
        raw_input()
    start = datetime.now()
    q = multiprocessing.JoinableQueue()
    q2 = multiprocessing.Queue()
    q3 = multiprocessing.Queue()
    processes = [multiprocessing.Process(target=consumer, args=(q, q2, q3))
                 for x in range(0, N_PROC)]
    processes += [multiprocessing.Process(target=progress,
                                          args=(len(sequence), q3))]
    for proc in processes:
        proc.daemon = True
        proc.start()
        sys.stderr.write("^Starting process PID: %s\n" % proc.pid)
    sys.stderr.flush()
    producer(sequence, q)
    q.join()
    sys.stderr.write("Time Elapsed: %s\n" % (datetime.now() - start))
    sys.stderr.flush()
    while not q2.empty():
        msg = q2.get()
        msg = map(unicode, msg)
        sys.stdout.write('%s\n' % ';'.join(msg))

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
