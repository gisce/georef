# -*- coding: utf-8 -*-
import sys

import georef.loop
from georef.codis_ine import CODIS_INE

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
