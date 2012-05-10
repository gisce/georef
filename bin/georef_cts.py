#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Georeferenciaci de CTS.
FORMULARIO 4: “Información relativa a la configuración y el equipamiento de los
centros de transformación reales existentes a 31 de diciembre de 2010”
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
    search_params = [('blockname.name', 'in', ('SEC_C', 'SEC_B'))]
    s_ids = O.GiscegisBlocsSeccionadorunifilar.search(search_params)
    codis = [x['codi']
             for x in O.GiscegisBlocsSeccionadorunifilar.read(s_ids, ['codi'])]
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
        ct = O.GiscedataCts.get(item)
        if ct.id not in ct_vertex:
            sys.stderr.write("**** ERROR: El CT %s no té vertex definit\n" %
                             ct.name)
            input_q.task_done()
            continue
        if ct.id not in ct_nodes:
            sys.stderr.write("**** ERROR: El CT %s no té node definit\n" %
                             ct.name)
            input_q.task_done()
            continue
        if not ct.id_municipi:
            sys.stderr.write("**** ERROR: El CT %s no té municipi definit\n" %
                             ct.name)
            input_q.task_done()
            continue
        if not ct.id_subtipus or not ct.id_subtipus.categoria_cne:
            sys.stderr.write("**** ERROR: El CT %s no té subtipus o "
                             "categoria_cne definit\n" % ct.name)
            input_q.task_done()
            continue
        vertex = ct_vertex[ct.id]
        node = ct_nodes[ct.id]
        # Calculem el número de SEC_B i SEC_C
        count_sec = 0
        for codi in codis:
            regexp = '%s(-{1}.*)$' % filter(lambda x: str(x).isdigit(), ct.name)
            if re.match(regexp, codi):
                count_sec += 1
        o_tensio_p = int(filter(str.isdigit, ct.tensio_p or '') or 0)
        # Calculem any posada en marxa        
        if not ct.data_pm:
            any_pm = ct.data_industria and ct.data_industria[:4] or ''
        else:
            any_pm=ct.data_pm[:4]
        output_q.put([
            'R1-%s' % codi_r1.zfill(3),
            ct.name,
            ct.descripcio[:20],
            round(vertex[0], 3),
            round(vertex[1], 3),
            node,
            ct.id_municipi.state.code,
            get_codi_ine(ct.id_municipi.ine),
            ct.id_subtipus.categoria_cne.codi,
            round(o_tensio_p / 1000.0, 3),
            ct.potencia,
            count_sec or 1,
            ct.cini or '',
            1,
            any_pm,
            ct.perc_financament
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


def check_module_cne_installed():
    """Comprovem que el mòdul de la CNE està instal·lat.
    """
    if not O.GiscedataCtsSubtipus.fields_get().get('categoria_cne', False):
        sys.stderr.write("**** ERROR. El mòdul 'giscedata_cne' no està "
                         "instal·lat. S'ha d'instal·lar i assignar els "
                         "subtipus abans de llançar aquest script.\n")
        sys.stderr.flush()
        return False
    return True


def main(file_out, codi_r1):
    """Funció principal del programa.
    """
    if not check_module_cne_installed():
        sys.exit(1)
    sequence = []
    search_params = [('id_installacio.name', '!=', 'SE'),
                     ('id_installacio.name', '!=', 'CH')]
    sequence += O.GiscedataCts.search(search_params)
    if not QUIET or INTERACTIVE:
        sys.stderr.write("Filtres utilitzats:\n")
        pprint.pprint(search_params, sys.stderr)
        sys.stderr.write("S'han trobat %s CTS.\n" % len(sequence))
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
