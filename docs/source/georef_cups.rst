georef_lbt.py: Script de generació F5-BT (LBT) CNE segons Circular 1/2012 des de GISCE-ERP
==========================================================================================
Introducció:
-------------

Aquest script genera l'informe F5 Baixa Tensió de la circular 1/2012 de la CNE.
Per fer-ho utilitza les dades de Gisce-ERP.

* El paràmetre :option:`-c` ens permet epecificar el codi de l'empresa distribuïra segons 
  la taula publicada a la circular 1/2012 de la CNE

* El fitxer de sortida es pot especificar mitjançant el paràmetre :option:`-o`

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

.. option:: -y ANY_P, --year=ANY_P

   Any que s'utilitzarà per calcular les potències contractades. 
   S'utilitzarà la pòlissa activa el 31 de Desembre de l'any seleccionat. 
   Any per defecte, any passat, p.e. 2011. 
   
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

* Generació del fitxer F5-BT (Línies de Baixa Tensió) de la base de dades **distri** 
  amb usuari **admin** i password **admin** al fitxer **/tmp/F5b.txt** amb codi R1 *052*. 
  Els paràmetres *usuari* i *pwd* no es passen perquè *admin* n'és el valor per defecte:: 

   > georef_lbt.py -o /tmp/F5b.txt -d distri -c 052
   