georef_cts.py: Script de generació F1 i F1bis (SE) CNE segons Circular 1/2012 des de GISCE-ERP
==============================================================================================
Introducció:
-------------

Aquest script genera l'informe F1 i F1bis de la circular 1/2012 de la CNE.
Per fer-ho utilitza les dades de Gisce-ERP.

* El paràmetre :option:`-c` ens permet epecificar el codi de l'empresa distribuïra segons 
  la taula publicada a la circular 1/2012 de la CNE

* El fitxer de sortida es pot especificar mitjançant el paràmetre :option:`-o`

* El paràmetre :option:`--interruptor` genera F1 (posicions amb interruptor)

* El paràmetre :option:`--no-interruptor` genera F1bis (posicions sense interruptor)

Paràmetres:
-----------

Els paràmetres que se li poden passar són:

Generals:
^^^^^^^^^

.. option:: --version

   Mostra la versió de l'script i surt 
   El *MINOR* és l'any per el qual es genera l'informe, p.e. 0.12.x és la versió 
   del 2012

.. option:: -h, --help

   Mostra l'ajuda i surt 

.. option:: -q, --quiet

   No mostra missatges d'error per la sortida d'error

.. option:: --no-interactive

   Deshabilita el mode interactiu. 
   Abans de generar el fitxer demana confirmació. 
   Mostra quants CTS ha trobat i la cerca que ha realitzat

.. option:: -o FOUT, --output=FOUT

   Indica el fitxer de sortida **FOUT** on s'escriurà l'informe. És **obligatori**

.. option:: -c R1, --codi-r1=R1

   Codi R1 de la distribuidora segons la taula de la circular 1/2012 de la CNE. 
   Ha de tenir tres caracters encara que el número sigui inferior a 100, p.e 052.
   Agafa només els 3 darrers caracters. 

.. option:: --interruptor

   Selecciona només de les posicions amb interruptor de les subestacions per
   generar el fitxer F1

.. option:: --no-interruptor

   Selecciona només de les posicions sense interruptor de les subestacions per
   generar el fitxer F1bis

Servidor ERP:
^^^^^^^^^^^^^

.. option:: -s SERVER, --server=SERVER

   Adreça del servidor ERP. Per defecte **localhost**
   
.. option:: -p PORT, --port=PORT

   Port del servidor ERP. Per defecte **8069**
   
.. option:: -u USER, --user=USER

   Usuari del servidor ERP. Usuari per defecte **admin**
   
.. option:: -w PASSWORD, --password=PASSWORD

   Password del servidor ERP, Password per defecte **admin**

.. option:: -d DATABASE, --database=DATABASE

   Nom de la base de dades postgresql


Exemples:
--------- 
* Generació del fitxer F1 (SE interruptor) de la base de dades **distri** amb usuari **admin** 
  i password **admin** al fitxer **/tmp/F1.txt** amb codi R1 *052*. 
  Els paràmetres *usuari* i *pwd* no es passen perquè *admin* n'és el valor per defecte:: 

   > georef_subest.py --interruptor -o /tmp/F1.txt -d distri -c 052

* Generació del fitxer F1bis (SE sense interruptor) de la base de dades **distri** amb usuari **admin** 
  i password **admin** al fitxer **/tmp/F1.txt** amb codi R1 *052*. 
  Els paràmetres *usuari* i *pwd* no es passen perquè *admin* n'és el valor per defecte:: 

   > georef_subest.py --no-interruptor -o /tmp/F1bis.txt -d distri -c 052


