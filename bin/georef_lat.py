#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Georeferenciaci de línies.
FORMULARIO 5: “Información relativa a la topología y atributos de las líneas
aéreas y cables subterráneos reales existentes a 31 de diciembre de 2010”
"""
import sys
import os
import multiprocessing
import pprint
from datetime import datetime
from optparse import OptionGroup, OptionParser
import csv

from georef.loop import OOOP, __version__
from progressbar import ProgressBar, ETA, Percentage, Bar

N_PROC = min(int(os.getenv('N_PROC', multiprocessing.cpu_count())),
             multiprocessing.cpu_count())
QUIET = False
INTERACTIVE = True

def producer(sequence, output_q):
    """Posem els items que serviran per fer l'informe.
    """
    for item in sequence:
        output_q.put(item)


def consumer(input_q, output_q, progress_q, codi_r1):
    """Fem l'informe.
    """
    codi_r1 = codi_r1[-3:]
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
                if not QUIET:
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
                round((linia.tensio or 0) / 1000.0, 3),
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


def main(file_out, codi_r1):
    """Funció principal del programa.
    """
    sequence = []
    search_params = [('name', '!=', '1')]
    sequence += O.GiscedataAtLinia.search(search_params)
    if not QUIET or INTERACTIVE:
        sys.stderr.write("Filtres utilitzats:\n")
        pprint.pprint(search_params, sys.stderr)
        sys.stderr.write("S'han trobat %s línies.\n" % len(sequence))
        sys.stderr.flush()
    if INTERACTIVE:
        sys.stderr.write("Correcte? ")
        raw_input()
        sys.stderr.flush()
    search_params = [('active', '=', 1),
                     ('baixa', '=', 1)]
    trams_indef = O.GiscedataAtTram.search(search_params)
    if trams_indef:
        long_cad = 0
        for tram in O.GiscedataAtTram.read(trams_indef, ['longitud_cad']):
            long_cad += tram['longitud_cad']
        if not QUIET or INTERACTIVE:
            sys.stderr.write("*** ATENCIÓ ***\n")
            sys.stderr.write("S'han trobat %i trams que estan actius i de baixa al "
                         "mateix temps. Sumen %f m.\n" % (len(trams_indef),
                                                          long_cad))
            sys.stderr.write("Si estan marcats com a baixa, no s'inclouran en "
                         "l'informe.\n")
            sys.stderr.flush()
        if INTERACTIVE:
            sys.stderr.write("Continuar igualment? ")
            raw_input()
            sys.stderr.flush()
    start = datetime.now()
    q = multiprocessing.JoinableQueue()
    q2 = multiprocessing.Queue()
    q3 = multiprocessing.Queue()
    processes = [multiprocessing.Process(target=consumer, args=(q, q2, q3, codi_r1))
                 for x in range(0, N_PROC)]
    if not QUIET:
        processes += [multiprocessing.Process(target=progress,
                                          args=(len(sequence), q3))]
    for proc in processes:
        proc.daemon = True
        proc.start()
        if not QUIET:
            sys.stderr.write("^Starting process PID: %s\n" % proc.pid)
    if not QUIET:
        sys.stderr.flush()
    producer(sequence, q)
    q.join()
    if not QUIET:
        sys.stderr.write("Time Elapsed: %s\n" % (datetime.now() - start))
        sys.stderr.flush()
    fout = open(file_out, 'wb')
    fitxer = csv.writer(fout, delimiter=';', lineterminator='\n')
    while not q2.empty():
        msg = q2.get()
        msg = map(lambda x: type(x)==unicode and x.encode('utf-8') or x, msg)
        fitxer.writerow(msg)

if __name__ == '__main__':
    try:
        parser = OptionParser(usage="%prog [OPTIONS]", version=__version__)
        parser.add_option("-q", "--quiet", dest="quiet", 
                action="store_true", default=False,
                help="No mostrar missatges de status per stderr")
        parser.add_option("--no-interactive", dest="interactive", 
                action="store_false", default=True,
                help="Deshabilitar el mode interactiu")
        parser.add_option("-o", "--output", dest="fout", 
                help="Fitxer de sortida")
        parser.add_option("-c", "--codi-r1", dest="r1",
                help="Codi R1 de la distribuidora")
        
        group = OptionGroup(parser, "Server options")
        group.add_option("-s", "--server", dest="server", default="localhost",
                help=u"Adreça servidor ERP")
        group.add_option("-p", "--port", dest="port", default=8069,
                help="Port servidor ERP")
        group.add_option("-u", "--user", dest="user", default="admin",
                help="Usuari servidor ERP")
        group.add_option("-w", "--password", dest="password", default="admin",
                help="Contrasenya usuari ERP")
        group.add_option("-d", "--database", dest="database",
                help="Nom de la base de dades")
        
        parser.add_option_group(group)
        (options, args) = parser.parse_args()
        QUIET = options.quiet
        INTERACTIVE = options.interactive
        if not options.fout:
            parser.error("Es necessita indicar un nom de fitxer")
        O = OOOP(dbname=options.database, user=options.user, pwd=options.password,
                 port=int(options.port))

        main(options.fout, options.r1)

    except KeyboardInterrupt:
        pass
