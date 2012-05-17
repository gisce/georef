#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Georeferenciaci de línies BT.
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

from georef.loop import OOOP
from progressbar import ProgressBar, ETA, Percentage, Bar
from georef import __version__

N_PROC = min(int(os.getenv('N_PROC', multiprocessing.cpu_count())),
             multiprocessing.cpu_count())
QUIET = False
INTERACTIVE = True
GEOREF = False

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
        linia = O.GiscedataBtElement.get(item)
        res = O.GiscegisEdge.search([('id_linktemplate', '=', linia.name),
                                     ('layer', 'ilike', '%BT%')])
        if GEOREF:
            coords = {
                'start_x': 0,
                'start_y': 0,
                'end_x': 0,
                'end_y': 0
            }
        if not res:
            if not QUIET:
                sys.stderr.write("**** ERROR: l'element %s (id:%s) no està "
                                 "en giscegis_edges.\n" % (linia.name, linia.id))
                sys.stderr.flush()
            edge = {'start_node': (0, '%s_0' % linia.name),
                    'end_node': (0, '%s_1' % linia.name)}
        elif len(res) > 1:
            if not QUIET:
                sys.stderr.write("**** ERROR: l'element %s (id:%s) està més d'una "
                                 "vegada a giscegis_edges. %s\n" %
                                 (linia.name, linia.id, res))
                sys.stderr.flush()
            edge = {'start_node': (0, '%s_0' % linia.name),
                'end_node': (0, '%s_1' % linia.name)}
        else:
            edge = O.GiscegisEdge.read(res[0], ['start_node', 'end_node'])
            if GEOREF:
                start_node = O.GiscegisNodes.read(edge['start_node'][0], ['vertex'])
                end_node = O.GiscegisNodes.read(edge['end_node'][0], ['vertex'])
                start_v = O.GiscegisVertex.read(start_node['vertex'][0], ['x', 'y'])
                end_v = O.GiscegisVertex.read(end_node['vertex'][0], ['x', 'y'])
                coords = {
                    'start_x': start_v['x'],
                    'start_y': start_v['y'],
                    'end_x': end_v['x'],
                    'end_y': end_v['y']
                }

        if linia.cable and linia.cable.tipus:
            o_cable_codi = linia.cable.tipus.codi
            if (o_cable_codi == 'I' and linia.tipus_linia
                and linia.tipus_linia.name[0] == 'S'):
                o_cable_codi = 'S'
        else:
                if not QUIET:
                    sys.stderr.write("**** ERROR: El tram %s no té cable "
                                     "o tipus\n" % linia.name)
                continue
        # Calculem any posada en marxa
        any_pm = linia.data_alta and linia.data_alta[:4] or ''
        # Propietari
        propietari = linia.propietari and "1" or "0"
        # Coeficient per ajustar longituds de trams
        coeficient = linia.coeficient or 1.0        
        o_voltatge = int(filter(str.isdigit, linia.voltatge or '') or 0)
        output = [
            'R1-%s' % codi_r1.zfill(3),
            linia.name,
            edge['start_node'][1],
            edge['end_node'][1],
            o_cable_codi,
            round(o_voltatge / 1000.0, 3),
            1,
            round(linia.longitud_cad * coeficient / 1000.0, 3) or 0,
            linia.cini or '',
            propietari,
            any_pm,
            linia.perc_financament]
        if GEOREF:
            output.extend([
                coords.get('start_x'),
                coords.get('start_y'),
                coords.get('end_x'),
                coords.get('end_y'),
            ])
        output_q.put(output)
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
    if not QUIET or INTERACTIVE:
        sys.stderr.write("Has executat la consulta SQL:\n")
        sys.stderr.write("UPDATE giscedata_bt_element SET baixa = False "
                         "where baixa is null;\n")
        if GEOREF:
            sys.stderr.write(u"ATENCIÓ! Has inclòs camps de georeferenciació. "
                             u"El fitxer no és vàlid per CNE\n")
            sys.stderr.flush()
    if INTERACTIVE:
        sys.stderr.write("Quan l'hagis executat prem INTRO. ")
        raw_input()
    sequence = []
    search_params = [('baixa', '=', 0),
                     ('cable.tipus.codi', '!=', 'E')]
    sequence += O.GiscedataBtElement.search(search_params)
    if not QUIET or INTERACTIVE:
        sys.stderr.write("Filtres utilitzats:\n")
        pprint.pprint(search_params, sys.stderr)
        sys.stderr.write("S'han trobat %s línies.\n" % len(sequence))
        sys.stderr.flush()
    if INTERACTIVE:
        sys.stderr.write("Correcte? ")
        raw_input()
        sys.stderr.flush()
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
        parser.add_option("-g", "--georef", dest="georef", 
                action="store_true",default=False,
                help=u"Afegeix georefenciació (!no CNE standard)")
        
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
        GEOREF = options.georef
        if not options.fout:
            parser.error("Es necessita indicar un nom de fitxer")
        O = OOOP(dbname=options.database, user=options.user, pwd=options.password,
                 port=int(options.port))

        main(options.fout, options.r1)

    except KeyboardInterrupt:
        pass
