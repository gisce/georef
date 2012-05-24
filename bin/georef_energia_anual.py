#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Actualització dels CAMPS ENERGIA donat un fitxer amb
el següent format:
CUPS[20];activa;reactiva
Les energies en KW/h
"""
from datetime import datetime
from optparse import OptionGroup, OptionParser
import codecs
import multiprocessing
import sys
import os

from georef.loop import OOOP
from progressbar import ProgressBar, ETA, Percentage, Bar
from georef import __version__

sys.stdout = codecs.getwriter("utf-8")(sys.stdout)
N_PROC = min(int(os.getenv('N_PROC', multiprocessing.cpu_count())),
             multiprocessing.cpu_count())


def producer(sequence, output_q):
    """Posem els items que serviran per fer l'informe.
    """
    for item in cups_list:
        output_q.put(item)


def consumer(input_q, progress_q):
    """Fem l'informe.
    """
    while True:
        item = input_q.get()
        progress_q.put(item)
        res, cups, e_activa, e_reactiva = item
        try:
            O.GiscedataCupsPs.write(res,
                                    {'cne_anual_activa': e_activa,
                                     'cne_anual_reactiva': e_reactiva})
        except Exception:
            sys.stderr.write("Write error: cups %s can not be modified\n" %
                             cups)
            sys.stderr.flush()
        input_q.task_done()


def progress(total, input_q):
    """Rendering del progressbar de l'informe.
    """
    widgets = ['GeoRef load annual energy: ',
               Percentage(), ' ', Bar(), ' ', ETA()]
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
    start = datetime.now()
    q = multiprocessing.JoinableQueue()
    q2 = multiprocessing.Queue()
    n_fils = unifil and 1 or N_PROC
    processes = [multiprocessing.Process(target=consumer, args=(q, q2))
                 for x in range(0, n_fils)]
    processes += [multiprocessing.Process(target=progress,
                                              args=(len(sequence), q2))]

    for proc in processes:
        proc.daemon = True
        proc.start()
        sys.stderr.write(u"^Starting process PID: %s\n" % proc.pid)
    sys.stderr.flush()
    producer(sequence, q)
    q.join()
    sys.stderr.write(u"Time Elapsed: %s\n" % (datetime.now() - start))
    sys.stderr.flush()

if __name__ == '__main__':
    try:
        parser = OptionParser(usage="%prog [OPTIONS]", version=__version__)
        parser.add_option("-f", "--input", dest="filename",
                help=u"Fitxer d'entrada")

        group = OptionGroup(parser, u"Server options")
        group.add_option("-s", "--server", dest="server", default="localhost",
                help=u"Servidor ERP")
        group.add_option("-p", "--port", dest="port", default=8069,
                help=u"Port servidor ERP")
        group.add_option("-u", "--user", dest="user", default="admin",
                help=u"Usuari servidor ERP")
        group.add_option("-w", "--password", dest="password", default="admin",
                help=u"Contrasenya usuari ERP")
        group.add_option("-d", "--database", dest="database",
                help=u"Nom de la base de dades")

        parser.add_option_group(group)
        (options, args) = parser.parse_args()

        port = int(options.port)
        user = options.user
        if not options.password:
            parser.error(u"Password required for user %s" % options.user)
        pwd = options.password
        if not options.database:
            parser.error(u"Database name required")
        dbname = options.database
        if not options.filename:
            parser.error(u"Input file required")
        filename = options.filename
        energies_file = open(filename)
        sequence = energies_file.readlines()
        energies_file.close()
        O = OOOP(dbname=dbname, port=port, user=user, pwd=pwd)
        # mirem versió ERP per desactivar multifil (temes de concurrència v4)
        # Per fer-ho mirem la versió del mòdul base ( la mare dels ous )
        res = O.IrModuleModule.search([('name', '=', 'base')])
        rs = O.IrModuleModule.read(res, ['latest_version'])
        ver = rs[0]['latest_version']
        m_ver = int(ver[0])
        if m_ver < 5:
            unifil = True
        else:
            unifil = False

        cups_list = []
        for item in sequence:
            cups, e_activa, e_reactiva = item.split(';')
            res = O.GiscedataCupsPs.search([('name', 'like', cups)])
            if not res:
                sys.stderr.write(u"ERROR : cups %s not found\n" % cups)
                sys.stderr.flush()
                continue
            elif len(res) > 1:
                sys.stderr.write(u"ERROR : cups %s found "
                                 u"more than once (%d)\n" % (cups, len(res)))
                sys.stderr.flush()
                continue
            else:
                cups_list.append((res, cups, e_activa, e_reactiva))
        main()
    except KeyboardInterrupt:
        pass
