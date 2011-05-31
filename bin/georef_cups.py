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
from loop import OOOP

sys.stdout = codecs.getwriter("utf-8")(sys.stdout)

def producer(sequence, output_q):
    """Posem els items que serviran per fer l'informe.
    """
    for item in sequence:
        output_q.put(item)

def consumer(input_q, output_q):
    """Fem l'informe.
    """
    codi_r1 = 'R1-%s' % sys.argv[5][-3:]
    while True:
        item = input_q.get()
        cups = O.GiscedataCupsPs.read(item, ['name', 'id_escomesa', 
                                             'id_municipi'])
        if not cups:
            input_q.task_done()
            continue
        res = []
        res.append(codi_r1)
        res.append(cups['name'])
	if cups['id_municipi']:
		municipi = O.ResMunicipi.read(cups['id_municipi'][0], ['ine', 'state'])
		ine = municipi['ine']
		provincia = O.ResCountryState.read(municipi['state'][0], ['code'])
		res.append(provincia['code'])
		res.append(ine[2:])
	else:
		res.append('')
		res.append('')
        res.append('MEC')

        if cups and cups['id_escomesa']:
            search_params = [('escomesa', '=', cups['id_escomesa'][0])]
            bloc_escomesa_id = O.GiscegisBlocsEscomeses.search(search_params)
            if bloc_escomesa_id:
                bloc_escomesa = O.GiscegisBlocsEscomeses.read(
                                        bloc_escomesa_id[0], ['node', 'vertex'])
                if bloc_escomesa['vertex']:
                    vertex = O.GiscegisVertex.read(bloc_escomesa['vertex'][0],
                                                   ['x', 'y'])
                    res.append(vertex['x'])
                    res.append(vertex['y'])
                else:
                    res.extend([''] * 2)
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
                                res.append(bt['tipus_linia'][1][0])
                            else:
                                res.append('')
        else:
            res.extend([''] * 3)

        search_params = [('cups', '=', cups['id'])]
        polissa_id = O.GiscedataPolissa.search(search_params)
        if polissa_id:
            polissa = O.GiscedataPolissa.read(polissa_id[0], ['potencia'])
            res.extend([polissa['potencia']] * 2)
        else:
            res.extend([''] * 2)
        output_q.put(res)
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
        print "^Starting process PID: %s" % proc.pid
    sequence = []
    search_params = []
    sequence += O.GiscedataCupsPs.search(search_params)
    sys.stderr.write("S'han trobat %s CUPS. Correcte? " % len(sequence))
    sys.stderr.flush()
    raw_input()
    producer(sequence, q)
    q.join() 
    sys.stderr.write("Time Elapsed: %s" % (datetime.now() - start))
    sys.stderr.write("-" * 80)
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
