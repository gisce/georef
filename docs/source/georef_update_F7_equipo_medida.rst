georef_update_F7_equipo_medida.py: Modificació **equipo_medida** d'un F7 
========================================================================
Introducció:
-------------

Aquest script permet manipular un fitxer F7 de georeferenciació 1/2012 ja generat
i canviar el cap "Equipo Medida". 
Concretament, canvia els que estan com a MEC (Electromecánicos) a SMT (Telegestión)
Per fer-ho utilitza un fitxer extern amb un CUPS per línia. 
Aquest CUPS ha de contenir només el 20 primers caracters.

p.e. 

.. code::
   ES0172000010100012YG
   ES0172000010100069PS
   ...

Paràmetres: