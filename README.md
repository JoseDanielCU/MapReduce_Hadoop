Este proyecto utiliza un clúster Hadoop (específicamente un nodo maestro) para ejecutar tareas de MapReduce.

ACCESO AL NODO MAESTRO DE HADOOP (SSH)

Se accede al nodo principal con SSH. Nota: La clave privada y el DNS público del nodo principal se proporcionan por separado.

Comando de Conexión: ssh -i (Key de acceso) Hadoop@(DNS público del nodo principal)

PREPARACIÓN DEL ENTORNO DEL NODO MAESTRO

Una vez dentro, actualiza el sistema e instala Git.

Actualización del Sistema: sudo yum update -y

Instalación de Git: sudo yum install git -y

CLONACIÓN Y CONFIGURACIÓN DEL REPOSITORIO

Clonar el Repositorio: git clone https://github.com/JoseDanielCU/MapReduce_Hadoop.git

Instalar Dependencias de la API: cd MapReduce_Hadoop/Api/ sudo pip install -r requirements.txt

EJECUCIÓN DE LOS SCRIPTS DEL PROYECTO

Antes de correr, otorga permisos de ejecución a los scripts:

Otorgar Permisos de Ejecución (chmod): chmod +x Upload_Data.sh chmod +x Fetch_and_Convert.sh chmod +x Run_Job.sh

Correr los Scripts: Ejecuta los scripts en el siguiente orden, siguiendo las indicaciones dadas dentro de cada archivo:

Upload_Data.sh (Sube datos a HDFS)

Fetch_and_Convert.sh (Obtiene y prepara datos)

Run_Job.sh (Inicia el trabajo de MapReduce)
