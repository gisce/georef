#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Georeferenciaci de línies BT.
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
        linia = O.GiscedataBtElement.get(item)
        res = O.GiscegisEdge.search([('id_linktemplate', '=', linia.name),
                                     ('layer', 'ilike', '%BT%')])
        if not res:
            sys.stderr.write("**** ERROR: l'element %s (id:%s) no està en "
                             "giscegis_edges.\n" % (linia.name, linia.id))
            sys.stderr.flush()
            edge = {'start_node': (0, '%s_0' % linia.name),
                'end_node': (0, '%s_1' % linia.name)}
        elif len(res) > 1:
            sys.stderr.write("**** ERROR: l'element %s (id:%s) està més d'una "
                             "vegada a giscegis_edges. %s\n" %
                             (linia.name, linia.id, res))
            sys.stderr.flush()
            edge = {'start_node': (0, '%s_0' % linia.name),
                'end_node': (0, '%s_1' % linia.name)}
        else:
            edge = O.GiscegisEdge.read(res[0], ['start_node', 'end_node'])
        if linia.cable and linia.cable.tipus:
            o_cable_codi = linia.cable.tipus.codi
            if (o_cable_codi == 'I' and linia.tipus_linia
                and linia.tipus_linia.name[1] == 'S'):
                o_cable_codi = 'S'
        else:
                sys.stderr.write("**** ERROR: El tram %s no té cable o tipus\n"
                                 % linia.name)
                continue
        o_voltatge = int(filter(str.isdigit, linia.voltatge or '') or 0)
        output_q.put([
            'R1-%s' % codi_r1.zfill(3),
            linia.name,
            edge['start_node'][1],
            edge['end_node'][1],
            o_cable_codi,
            round(o_voltatge / 1000.0, 3),
            1,
            round(linia.longitud_cad / 1000.0, 3) or 0,
            linia.cini or '',
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
    sys.stderr.write("Has executat la consulta SQL:\n")
    sys.stderr.write("UPDATE giscedata_bt_element SET baixa = False "
                     "where baixa is null;\n")
    sys.stderr.write("Quan l'hagis executat prem INTRO. ")
    raw_input()
    sequence = []
    search_params = [('baixa', '=', 0),
                     ('cable.tipus.codi', '!=', 'E')]
    sequence += O.GiscedataBtElement.search(search_params)
    sys.stderr.write("Filtres utilitzats:\n")
    pprint.pprint(search_params, sys.stderr)
    sys.stderr.write("S'han trobat %s línies. Correcte? " % len(sequence))
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
