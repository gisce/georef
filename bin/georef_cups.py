#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Georeferenciació de la demanda.

FORMULARIO 7: 
Información relativa a la georreferenciación de la demanda salvo suministros a distribuidores

codigo empresa
cups
x
y
provincia
municipio
equipo
conexión
potencia adscrita
potencia contratada

"""
import sys
import codecs
import multiprocessing
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
    o_codi_r1 = 'R1-%s' % sys.argv[5][-3:]
    while True:
        item = input_q.get()
        cups = O.GiscedataCupsPs.read(item, ['name', 'id_escomesa', 
                                             'id_municipi'])
        if not cups:
            input_q.task_done()
            continue
        o_name = cups['name']
        o_codi_ine = ''
        if cups['id_municipi']:
            municipi = O.ResMunicipi.read(cups['id_municipi'][0], ['ine', 'state'])
            ine = municipi['ine']
            provincia = O.ResCountryState.read(municipi['state'][0], ['code'])
            o_codi_prov = provincia['code']
            o_codi_ine = ine[2:]

        o_equip = 'MEC'
        o_utmx = ''
        o_utmy = ''
        o_linia = ''
        if cups and cups['id_escomesa']:
            search_params = [('escomesa', '=', cups['id_escomesa'][0])]
            bloc_escomesa_id = O.GiscegisBlocsEscomeses.search(search_params)
            if bloc_escomesa_id:
                bloc_escomesa = O.GiscegisBlocsEscomeses.read(
                                        bloc_escomesa_id[0], ['node', 'vertex'])
                if bloc_escomesa['vertex']:
                    vertex = O.GiscegisVertex.read(bloc_escomesa['vertex'][0],
                                                   ['x', 'y'])
                    o_utmx = round(vertex['x'], 3)
                    o_utmy = round(vertex['y'], 3)
                
                if bloc_escomesa['node']:
                    search_params = [('start_node', '=',
                                      bloc_escomesa['node'][0])]
                    edge_id = O.GiscegisEdge.search(search_params)
                    if not edge_id:
                        search_params = [('end_node', '=',
                                          bloc_escomesa['node'][0])]
                        edge_id = O.GiscegisEdge.search(search_params)
                    if edge_id:
                        edge = O.GiscegisEdge.read(edge_id[0])
                        search_params = [('id', '=', edge['id_linktemplate'])]
                        bt_id = O.GiscedataBtElement.search(search_params)
                        if bt_id:
                            bt = O.GiscedataBtElement.read(bt_id[0],
                                                                ['tipus_linia'])
                            if bt['tipus_linia']:
                                o_linia=bt['tipus_linia'][1][0]

        search_params = [('cups', '=', cups['id'])]
        polissa_id = O.GiscedataPolissa.search(search_params)
        o_potencia = ''
        if polissa_id:
            polissa = O.GiscedataPolissa.read(polissa_id[0], ['potencia'])
            o_potencia = polissa['potencia']
        output_q.put([
           o_codi_r1,
           o_name,
           o_utmx,
           o_utmy,
           o_codi_prov,
           o_codi_ine,
           o_equip,
           o_linia,
           o_potencia,
           o_potencia
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
    search_params = []
    sequence += O.GiscedataCupsPs.search(search_params)
    sys.stderr.write("S'han trobat %s CUPS. Correcte? " % len(sequence))
    sys.stderr.flush()
    raw_input()
    producer(sequence, q)
    q.join() 
    sys.stderr.write("Time Elapsed: %s" % (datetime.now() - start))
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
