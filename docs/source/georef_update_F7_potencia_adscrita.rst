georef_update_F7_potencia_adscrita.py: Modificació de la **potència adscrita** d'un F7 
==================================================================================
Introducció:
-------------

Aquest script permet manipular un fitxer F7 de georeferenciació 1/2012 ja generat
i el camp "Potència Adscrita". 
Per fer-ho utilitza un fitxer extern amb un parell CUPS/potència separat per
*;* per cada CUPS que es vulgui modificar **cups[20];potencia(0.0)**. 
El CUPS ha de contenir només el 20 primers caracters i la potència utilitza
nomes el **.** (punt) com a separador de decimals

p.e. 

.. code::

   ES0172000010100012YG;3.35
   ES0172000010100069PS;8.8
   ...

Paràmetres:
-----------

Només accepta 2 paràmetres:

.. option:: F7

    El primer paràmetre és el fitxer F7 que es processarà

.. option:: cups
    
    El segon paràmetre serà el llistat de cups i potències

Exemples:
---------

* Modificació de la **potència adscrita** dels CUPS del fitxer **F7.txt** llistats en el fitxer **cups.txt**
  amb la potència definida en els segon camp::

  > georef_update_F7_potencia_adscrita.py F7.txt cups.txt


