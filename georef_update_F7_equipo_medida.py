#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

cups_smt = []
smt_count = 0


def main():
    if len(sys.argv) < 3:
        sys.stderr.write(
""" ------------------------------------------------------------------
Modificació d'equip de mesura F7 1/2012 CNE:
Has de passar el fitxer F7 i el fitxer amb els cups SMT (telegestió)
Aquest 2on fitxer ha de contenir una línia per cups
%s F7.txt CUPS.txt
                                     (c) Gisce-IT
-------------------------------------------------------------------
""" % sys.argv[0])
        exit(0)

    fpc = open(sys.argv[2])
    for lin in fpc:
        cups = lin[:-1].split(';')
        cups_smt.append(cups[0])
    fpc.close()
    sys.stderr.write("S'han trobat %d CUPS SMT\n" % len(cups_smt))

    f7 = open(sys.argv[1])
    smt_count = 0
    for lin in f7:
        cups = lin[7:27]
        if cups_smt.count(cups) > 0:
            smt_count = smt_count + 1
            print lin[:-1].replace(';MEC;', ';SMT;')
        else:
            print lin[:-1]

    f7.close()
    sys.stderr.write("S'han actualitzat %d/%d CUPS SMT\n" %
        (smt_count, len(cups_smt)))


if __name__ == "__main__":
    main()
