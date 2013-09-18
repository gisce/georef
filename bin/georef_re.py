#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Georeferenciació de la demanda.

FORMULARIO 8:
Información relativa a generación conectada a sus redes de distribución

codigo empresa
cil
x
y
provincia
municipio
conexión
potencia instalada
energía activa
energía reactiva
"""
import sys
import os
import multiprocessing
from datetime import datetime
from optparse import OptionGroup, OptionParser
import csv

from georef.loop import OOOP
from georef import get_codi_ine, __version__
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
    o_codi_r1 = 'R1-%s' % codi_r1[-3:]
    while True:
        item = input_q.get() 
        progress_q.put(item)
        re = O.GiscedataRe.read(item, ['cups', 'potencia_nominal'])
        if not re:
            input_q.task_done()
            continue
        o_potencia = re['potencia_nominal'] or ''
        cups = O.GiscedataCupsPs.read(re['cups'][0], ['name', 'id_municipi',
                                                        'id_escomesa', 'cne_anual_activa',
                                                        'cne_anual_reactiva'])
        o_cil = '%s%s' % (cups['name'][:20], '001')
        o_ener_activa = cups['cne_anual_activa']
        o_ener_reactiva = cups['cne_anual_reactiva']
        o_estimada = 'N'
        o_codi_ine = ''
        o_codi_prov = ''
        if cups['id_municipi']:
            municipi = O.ResMunicipi.read(cups['id_municipi'][0], ['ine', 'state'])
            ine = municipi['ine']
            if municipi['state']:
                provincia = O.ResCountryState.read(municipi['state'][0], ['code'])
                o_codi_prov = provincia['code']
            o_codi_ine = get_codi_ine(ine)

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
                        edge = O.GiscegisEdge.read(edge_id[0],
                                                   ['id_linktemplate'])
                        search_params = [('name', '=', edge['id_linktemplate'])]
                        bt_id = O.GiscedataBtElement.search(search_params)
                        if bt_id:
                            bt = O.GiscedataBtElement.read(bt_id[0],
                                                                ['tipus_linia'])
                            if bt['tipus_linia']:
                                o_linia = bt['tipus_linia'][1][0]

        output_q.put([
           o_codi_r1,
           o_cil,
           o_utmx,
           o_utmy,
           o_estimada,
           o_codi_prov,
           o_codi_ine,
           o_linia, 
           o_potencia,
           o_ener_activa,
           o_ener_reactiva
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
    sequence += O.GiscedataRe.search(search_params)
    if not QUIET or INTERACTIVE: 
        sys.stderr.write("S'han trobat %s instal·lacions de RE.\n" % len(sequence))
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
        sys.stderr.write("Time Elapsed: %s" % (datetime.now() - start))
        sys.stderr.flush()
    fout = open(file_out,'wb')
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
