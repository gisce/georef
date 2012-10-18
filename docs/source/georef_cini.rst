georef_cini.py: Script per actualitzar els cini's diferents models de GISCE-ERP
===============================================================================
Introducció:
-------------

Aquest script Actualitza el cini d'un model de GISCE-ERP a partir s'un fitxer CSV 
que conté els id de l'element en el model i el seu CINI separats per *;*

p.e. 

.. code::

   234;I22452H
   25344;I22452H
   ...

Paràmetres:
-----------

Els paràmetres que s'han de passar per ordre i de la següent forma: 

::

   > georef_cini.py DBNAME PORT USER PWD Model fitxer 

1. *DBNAME* 

   Base de dades de l'ERP

2. *PORT*

   Port on escolta l'ERP

3. *USER* 

   Usuari de l'ERP

4. *PWD*

   Password de l'usuari de l'ERP

5. *Model*

   Model de l'ERP del qual es volen actualitzar els CINI's. 
   Per exemple *giscedata_cups_ps*. El camp en el model s'ha de dir **cini**

6. *fitxer*

   Fitxer CSV amb la parella *id;cini*

Exemples:
--------- 

* Actualització dels CINI's de CUPS (*giscedata_cups_ps*) a partir de les dades CSV 
  del fitxer */tmp/cups_cini.txt* a la base de dades *distri* amb usuari *admin*, 
  password *admin* d'un erp escoltant en el port *8069*::   

  > georef_cini.py distri 8069 admin admin giscedata_cups_ps /tmp/cups_cini.txt
   


