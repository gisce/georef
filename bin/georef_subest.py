#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Georeferenciaci de Subestacions amb interruptor.
FORMULARIO 1: “Información relativa a la configuración y el equipoamiento de
las subestaciones existentes a 31 de diciembre de 2011 - Posiciones con
interruptor”
"""
import sys
import multiprocessing
import pprint
import re
import os
from datetime import datetime
from optparse import OptionGroup, OptionParser
import csv

from georef.loop import OOOP
from georef import get_codi_ine
from progressbar import ProgressBar, ETA, Percentage, Bar
from georef import __version__

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
    ctat_ids = O.GiscegisBlocsCtat.search([])
    ct_nodes = {}
    ct_vertex = {}
    ct_expedient = {}
    for ct in O.GiscegisBlocsCtat.read(ctat_ids, ['ct', 'node', 'vertex']):
        if not ct['ct']:
            continue
        if ct['node']:
            ct_nodes[ct['ct'][0]] = ct['node'][1]
        if ct['vertex']:
            v = O.GiscegisVertex.read(ct['vertex'][0], ['x', 'y'])
            ct_vertex[ct['ct'][0]] = (v['x'], v['y'])
    while True:
        item = input_q.get()
        progress_q.put(item)
        subest = O.GiscedataCtsSubestacions.get(item)
        if subest.ct_id.id not in ct_vertex:
            sys.stderr.write("**** ERROR: La subestació %s no té vertex "
                             "definit\n" % subest.name)
            input_q.task_done()
            continue
        if subest.ct_id.id not in ct_nodes:
            sys.stderr.write("**** ERROR: La subestació %s no té node "
                             "definit\n" % subest.name)
            input_q.task_done()
            continue
        if not subest.id_municipi:
            sys.stderr.write("**** ERROR: La subestació %s no té municipi "
                             "definit\n" % subest.name)
            input_q.task_done()
            continue
        vertex = ct_vertex[ct.id]
        node = ct_nodes[ct.id]
        output_q.put([
            'R1-%s' % codi_r1.zfill(3),
            subest.name,
            subest.descripcio[:20],
            round(vertex[0], 3),
            round(vertex[1], 3),
            node,
            subest.id_municipi.state.code,
            get_codi_ine(subest.id_municipi.ine),
            subest.tipus_parc,
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
    search_params = []
    sequence += O.GiscedataCtsSubestacions.search(search_params)
    if not QUIET or INTERACTIVE:
        sys.stderr.write("Filtres utilitzats:\n")
        pprint.pprint(search_params, sys.stderr)
        sys.stderr.write("S'han trobat %s Subestacions.\n" % len(sequence))
        sys.stderr.flush()
    if INTERACTIVE:
        sys.stderr.write("Correcte? ")
        raw_input()
    start = datetime.now()
    q = multiprocessing.JoinableQueue()
    q2 = multiprocessing.Queue()
    q3 = multiprocessing.Queue()
    processes = [multiprocessing.Process(target=consumer, args=(q, q2, q3,
                                                                codi_r1))
                 for x in range(0, N_PROC)]
    if not QUIET:
        processes += [multiprocessing.Process(target=progress,
                                          args=(len(sequence), q3))]
    for proc in processes:
        proc.daemon = True
        proc.start()
        if not QUIET:
            sys.stderr.write("^Starting process PID: %s\n" % proc.pid)
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
        msg = map(lambda x: type(x) == unicode and x.encode('utf-8') or x, msg)
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
        O = OOOP(dbname=options.database, user=options.user,
                 pwd=options.password, port=int(options.port))

        main(options.fout, options.r1)

    except KeyboardInterrupt:
        pass
