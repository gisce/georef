#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Georeferenciaci de línies BT.
FORMULARIO 5: “Información relativa a la topología y atributos de las líneas 
aéreas y cables subterráneos reales existentes a 31 de diciembre de 2010”
"""
import sys
import codecs
import multiprocessing
import pprint
from datetime import datetime

from georef.loop import OOOP

sys.stdout = codecs.getwriter("utf-8")(sys.stdout)

def producer(sequence, output_q):
    """Posem els items que serviran per fer l'informe.
    """
    for item in sequence:
        output_q.put(item)

def consumer(input_q, output_q):
    """Fem l'informe.
    """
    codi_r1 = sys.argv[5][-3:]
    while True:
        item = input_q.get()
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
        
        output_q.put([
            'R1-%s' % codi_r1.zfill(3),
            linia.name,
            edge['start_node'][1],
            edge['end_node'][1],
            linia.cable.tipus.codi,
            linia.voltatge,
            1,
            int(round(linia.longitud_cad)) or 1,
            linia.cini or '',
        ])
        input_q.task_done()

def main():
    """Funció principal del programa.
    """
    start = datetime.now()
    q = multiprocessing.JoinableQueue()
    q2 = multiprocessing.Queue()
    processes = [multiprocessing.Process(target=consumer, args=(q, q2))
                 for x in range(0, multiprocessing.cpu_count())]
    for proc in processes:
        proc.daemon = True
        proc.start()
        sys.stderr.write("^Starting process PID: %s\n" % proc.pid)
    sys.stderr.flush()
    sequence = []
    search_params = [('baixa', '!=', 0),
                     ('baixa', '!=', False),
                     ('cable.tipus.codi', 'not in', ('E', 'I'))]
    sequence += O.GiscedataBtElement.search(search_params)
    sys.stderr.write("Filtres utilitzats:\n")
    pprint.pprint(search_params, sys.stderr)
    sys.stderr.write("S'han trobat %s línies. Correcte? " % len(sequence))
    sys.stderr.flush()
    raw_input()
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
