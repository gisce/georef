georef_energia_anual.py: Script de càrrega de l'energia anual d'un CUPS a GISCE--ERP
====================================================================================
Introducció:
-------------

Aquest script omple els camps de GISCE-ERP que emmagatzemen l'energia consumida 
per un cups durant l'any de l'informe a partir d'un fitxer CSV especialment preparat
extret del sistema de facturació del client.
Aquesta informació és necessària per a la generació de l'informe F7 (CUPS)
El fitxer CSV ha de seguir els següent format:
 
.. code::

   cups[20];energia_anual_activa;energia_anual_reactiva
   
On:
* Les energies són un número que pot ser decimal en kWh amb els decimals separats per punt (*.*)
* Només s'han de passar els 20 primers caracters del CUPS

Un exemple:

.. code::
  
   ES0336000000110335CW0F;40225;9491.25
   ES0336000000110340CF0F;1335;0
   ES0336000000110360KG0F;0;0
   ...
  

* El paràmetre :option:`-f` ens permet epecificar el fitxer que volem carregar 
  
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

.. option:: -f FILENAME, --output=FILENAME

   Indica el fitxer CSV amb les dades a carregar. És **obligatori**

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

* Per càrrega de les energies anuals del fitxer */tmp/cups_energia.txt* a la base de dades 
  **distri** amb usuari **admin** i password **admin** podem executar la següent comanda.  
  Els paràmetres *usuari* i *pwd* no es passen perquè *admin* n'és el valor per defecte:: 

   > georef_energia_anual.py -f /tmp/cups_energia.txt -d distri
   