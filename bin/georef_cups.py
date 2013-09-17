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
tension
potencia adscrita
potencia contratada
energia anual activa
energia anual reactiva
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


def consumer(input_q, output_q, progress_q, codi_r1, any_p):
    """Fem l'informe.
    """
    o_codi_r1 = 'R1-%s' % codi_r1[-3:]
    while True:
        item = input_q.get()
        progress_q.put(item)
        cups = O.GiscedataCupsPs.read(item, ['name', 'id_escomesa',
                                             'id_municipi',
                                             'cne_anual_activa',
                                             'cne_anual_reactiva'])
        if not cups:
            input_q.task_done()
            continue
        o_name = cups['name'][:20]
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
        o_tensio = ''
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
                                                                ['tipus_linia',
                                                                 'voltatge'])
                            if bt['tipus_linia']:
                                o_linia = bt['tipus_linia'][1][0]
                            o_tensio = float(bt['voltatge']) / 1000.0

        # Calculem l'últim dia de l'any per tenir en compte eles modificacions
        # contractuals (només tindrà efecte a la versió 5) i la polissa activa
        # aquell dia.
        # Per v5, es treuen les polisses que estan en estat validar o esborrany
        search_params = [('cups', '=', cups['id'])] + SEARCH_GLOB
        polissa_id = O.GiscedataPolissa.search(search_params, 0, 0, False,
                                               CONTEXT_GLOB)
        o_potencia = ''
        o_pot_ads = ''
        o_equip = 'MEC'
        if polissa_id:
            fields_to_read = ['potencia']
            if 'butlletins' in O.GiscedataPolissa.fields_get():
                fields_to_read += ['butlletins']
            polissa = O.GiscedataPolissa.read(polissa_id[0], fields_to_read,
                     CONTEXT_GLOB)
            o_potencia = polissa['potencia']
            # Mirem si té l'actualització dels butlletins
            if polissa['butlletins']:
                butlleti = O.GiscedataButlleti.read(polissa['butlletins'][-1],
                                                    ['pot_max_admisible'])
                o_pot_ads = butlleti['pot_max_admisible']
        else:
            #Si no trobem polissa activa, considerem "Contrato no activo (CNA)"
            o_equip = 'CNA'
        #energies consumides
        o_anual_activa = cups['cne_anual_activa'] or 0.0
        o_anual_reactiva = cups['cne_anual_reactiva'] or 0.0
        output_q.put([
           o_codi_r1,
           o_name,
           o_utmx,
           o_utmy,
           o_codi_prov,
           o_codi_ine,
           o_equip,
           o_linia,
           o_tensio,
           o_potencia,
           o_pot_ads or o_potencia,
           o_anual_activa,
           o_anual_reactiva
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


def main(file_out, codi_r1, any_p):
    """Funció principal del programa.
    """
    sequence = []
    search_params = []
    sequence += O.GiscedataCupsPs.search(search_params)
    if not QUIET or INTERACTIVE: 
        sys.stderr.write("S'han trobat %s CUPS.\n" % len(sequence))
        sys.stderr.write("Any %d.\n" % any_p)
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
                                                             codi_r1, any_p))
                 for x in range(0, N_PROC)]
    if not QUIET:
        processes += [multiprocessing.Process(target=progress,
                                          args=(len(sequence), q3))]
    for proc in processes:
        proc.daemon = True
        proc.start()
        if not QUIET:
            sys.stderr.write("^Starting process PID (%s): %s\n" %
                             (proc.name, proc.pid))
    sys.stderr.flush()
    producer(sequence, q)
    q.join()
    if not QUIET:
        sys.stderr.write("Time Elapsed: %s\n" % (datetime.now() - start))
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
        parser.add_option("-y", "--year", dest="any_p",
                          default=(datetime.now().year - 1),
                          help=u"Any per càlculs")

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
        ULTIM_DIA_ANY = ("%d-12-31" % int(options.any_p))
        SEARCH_GLOB = []
        CONTEXT_GLOB = {}
        if 'state' in O.GiscedataPolissa.fields_get():
            SEARCH_GLOB += [('state', 'not in', ('esborrany', 'validar')),
                           ('data_alta', '<=', ULTIM_DIA_ANY),
                           '|',
                           ('data_baixa', '>=', ULTIM_DIA_ANY),
                           ('data_baixa', '=', False)]
            CONTEXT_GLOB['date'] = ULTIM_DIA_ANY
            CONTEXT_GLOB['active_test'] = False
        main(options.fout, options.r1, int(options.any_p))

    except KeyboardInterrupt:
        pass
