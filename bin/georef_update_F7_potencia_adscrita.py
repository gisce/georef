#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys

cups_list = {}
item_count = 0
item_total = 0


def main():
    if len(sys.argv) < 3:
        sys.stderr.write(
""" ------------------------------------------------------------------
Modificació de la potencia adscrita F7 1/2012 CNE:
Has de passar el fitxer F7 i el fitxer amb cups i potencia_adscrita
Aquest 2on fitxer ha de contenir una línia per cups
Cada línia ha de ser: cups[20];potència_adscrita
Els decimals separats per '.' (punt)

%s F7.txt CUPS.txt
                                     (c) Gisce-IT
-------------------------------------------------------------------
""" % sys.argv[0])
        exit(0)

    fpc = open(sys.argv[2])
    for lin in fpc:
        cups, potencia = lin[:-1].split(';')
        cups_list[cups] = potencia
    fpc.close()
    sys.stderr.write("Es modificarà la potència de %d CUPS \n" %
                        len(cups_list))

    f7 = open(sys.argv[1])
    item_count = 0
    item_total = 0
    for lin in f7:
        r1, cups, x, y, c1, c2, equipo, tipo, V, pa, pc, ea, er = \
            lin[:-1].split(';')
        item_total = item_total + 1

        if cups_list.keys().count(cups) > 0:
            # És al fitxer de cups
            new_pa = cups_list[cups]
            if float(new_pa) == 0:
                # Potència adscrita = 0
                sys.stderr.write(
                "WARN: Al cups %s li asignarem P. Adscrita 0\n" % cups)
            elif float(new_pa) < pc != "" and float(pc) or 0:
                # Potència adscrita < Pència contractada
                sys.stderr.write(
"""WARN: Al cups %s li asignarem P. Adscrita < P. Contractada (%s < %s )\n"""
                % (cups, new_pa, pc))
            sys.stdout.write("%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%s\n" %
                    (r1, cups, x, y, c1, c2, equipo, tipo,
                    V, new_pa, pc, ea, er))
            item_count = item_count + 1
        else:
            # NO és al fitxer de cups
            new_pa = pa
            sys.stderr.write(
"""WARN: El cups %s és al fitxer de Potències Adscrites\n""" %
            cups)
            sys.stdout.write("%s\n" % lin[:-1])

    f7.close()
    sys.stdout.flush()
    sys.stderr.write("""
    S'han actualitzat %d/%d Potències Adscrites de fitxer amb %d cups\n""" %
        (item_count, item_total, len(cups_list)))
    sys.stderr.flush()

if __name__ == "__main__":
    main()
