#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Georeferenciaci de CTS.
FORMULARIO 4: “Información relativa a la configuración y el equipamiento de los
centros de transformación reales existentes a 31 de diciembre de 2010”
"""
import sys
import codecs
import multiprocessing
import pprint
import re
import os
from datetime import datetime

from georef.loop import OOOP
from georef import get_codi_ine
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
    ctat_ids = O.GiscegisBlocsCtat.search([])
    search_params = [('blockname.name', 'in', ('SEC_C', 'SEC_B'))]
    s_ids = O.GiscegisBlocsSeccionadorunifilar.search(search_params)
    codis = [x['codi']
             for x in O.GiscegisBlocsSeccionadorunifilar.read(s_ids, ['codi'])]
    ct_nodes = {}
    ct_vertex = {}
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
            1
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
    cne_module = O.IrModuleModule.filter(name='giscedata_cne')[0]
    if cne_module.state != 'installed':
        sys.stderr.write("**** ERROR. El mòdul 'giscedata_cne' no està "
                         "instal·lat. S'ha d'instal·lar i assignar els "
                         "subtipus abans de llançar aquest script.\n")
        sys.stderr.flush()
        return False
    return True


def main():
    """Funció principal del programa.
    """
    if not check_module_cne_installed():
        sys.exit(1)
    sequence = []
    search_params = [('id_installacio.name', '!=', 'SE'),
                     ('id_installacio.name', '!=', 'CH')]
    sequence += O.GiscedataCts.search(search_params)
    sys.stderr.write("Filtres utilitzats:\n")
    pprint.pprint(search_params, sys.stderr)
    sys.stderr.write("S'han trobat %s CTS. Correcte? " % len(sequence))
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
