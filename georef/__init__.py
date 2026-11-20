# -*- coding: utf-8 -*-
import sys
import io

from georef.codis_ine import CODIS_INE

__version__ = '0.12.8'

try:
    text_type = unicode
except NameError:
    text_type = str


def csv_row(values):
    """Return CSV values suitable for the active Python runtime."""
    if sys.version_info[0] < 3:
        return [value.encode('utf-8') if isinstance(value, text_type) else value
                for value in values]
    return list(values)


def open_csv(filename):
    """Open a CSV output with the mode expected by Python 2/3 csv."""
    if sys.version_info[0] < 3:
        return open(filename, 'wb')
    return io.open(filename, 'w', encoding='utf-8', newline='')

def get_codi_ine(ine):
    """Retorna el codi INE només si el que li passem té 3 caràcters a part de
    l'estat.
    """
    codi_ine = ine[2:]
    if len(codi_ine) == 3:
        if ine not in CODIS_INE:
            sys.stderr.write("**** ERROR: No s'ha trobat el checksum de "
                             "l 'INE %s al fitxer codis_ine.py\n"
                             % ine)
            sys.stderr.flush()
        codi_ine += CODIS_INE.get(ine, '')
    return codi_ine
