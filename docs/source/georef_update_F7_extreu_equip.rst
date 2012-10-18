georef_update_F7_extreu_equip.py: Extreu **CUPS** d'un F7 
========================================================================
Introducció:
-------------

Aquest script permet manipular un fitxer F7 de georeferenciació 1/2012 ja generat
per eliminar-ne registres.
Per fer-ho utilitza un fitxer extern amb un CUPS per línia. 
Aquest CUPS ha de contenir només el 20 primers caracters.

p.e. 

.. code::

   ES0172000010100012YG
   ES0172000010100069PS
   ...

Paràmetres:
-----------

Només accepta 2 paràmetres:

.. option:: F7

    El primer paràmetre és el fitxer F7 que es processarà

.. option:: cups
    
    El segon paràmetre serà el llistat de cups

Exemples:
---------

* Per extreure els registres del fitxer **F7.txt** dels CUPS llistats en el fitxer **cups.txt**::

  > georef_update_F7_extreu_equip.py F7.txt cups.txt


