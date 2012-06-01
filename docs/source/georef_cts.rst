Script de generació F4 (CTS) CNE segons Circular 1/2012 des de GISCE-ERP
=========================================================================
Introducció:
-------------

Aquest script genera l'informe F4 de la circular 1/2012 de la CNE.
Per fer-ho utilitza les dades de Gisce-ERP.

* El paràmetre `-c`_ ens permet epecificar el codi de l'empresa distribuïra segons 
la taula publicada a la circular 1/2012 de la CNE

* El fitxer de sortida es pot especificar mitjançant el paràmetre `p_fout`_

Paràmetres:
-----------

Els paràmetres que se li poden passar són:

Generals:
^^^^^^^^^

.. _p_version: 

* **--version**
  Mostra la versió de l'script i surt 
  El *MINOR* és l'any per el qual es genera l'informe, p.e. 0.12.x és la versió 
  del 2012
  
.. _p_help: 

* **-h, --help**
  Mostra l'ajuda i surt 

.. _p_quiet: 

* **-q, --quiet**
  No mostra missatges d'error per la sortida d'error
  
.. _p_interactive: 

* **--no-interactive**
  Deshabilita el mode interactiu. 
  Abans de generar el fitxer demana confirmació. 
  Mostra quants CTS ha trobat i la cerca que ha realitzat

.. _p_fout: -o
* **-o FOUT, --output=FOUT**
  Indica el fitxer de sortida **FOUT** on s'escriurà l'informe. És **oblicatori**
  
Exemples:
--------- 