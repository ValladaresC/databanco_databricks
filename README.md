# databanco_databricks
### Proyecto con diferentes transacciones bancarias en archivos .json (data dummy) y procesada en Databricks
## Primera Parte: Administración de Databricks
### Mediante Unity Catalog se configuró el Metastore para que el Workspace de Databricks estuviese asociado a un Azure Data Lake Storage específico para que todos los catalógos, esquemas y tablas creados puedan ser almacenados en dicho contenededor, tal como se observa en el esquema. 
<p align="center">
  <img width="460" height="600" src="https://github.com/user-attachments/assets/923417b7-4328-4cb0-a3a0-bda58773d48d">
</p>

### A continuación presento los pasos a seguir para este proceso de configuración del Metastore con Unity Catalog:
### 1. Creación del Storage Account, recordando tildar la opción avanzada de Enable Hierarchical Namespace con el fin de convertirlo en un Data Lake Storage. Adicional crearle su respectivo contenedor y carpeta (donde desea almacenar catalógos, esquemas y tablas del Workspace de Databricks)
### 2. Creación del Sevicio de Azure Databricks
### 3. Creación del Servicio Access Connector for Azure Databricks
### 4. Ingresamos al ADLS (Azure Data Lake Storage creado en el paso 1) y vamos a la opcion Access Control (IAM) - Add Role Assigment y buscamos el rol de Storage Blob Data Contributor, seleccionamos y vamos a la pestaña de Members - Select Members y colocamos el nombre del conector que creamos en el paso 3, revisar y aceptar.
### 5. Accedemos a Unity Catalog mediante *https://accounts.azuredatabricks.net/users* y borramos el Metastore creado por defecto.
### 6. Creación del nuevo Metastore con un nuevo nombre.
### 7. En el apartado ADLS Gen2 Path se debe colocar lo siguiente con la misma estructura que se muestra a continuación: ***containername@storageaccountname.dfs.core.windows.net/foldername***
### 8. Colocar el ID Connector que hemos creado (Access Control ID). Para este paso debemos acceder al Access Connector for Azure Databricks y seleccionamos el conector creado y al lado derecho aparecerá el ID (ResourceID), lo copiamos y pegamos en el lugar solicitado. 
### 9. Una vez llenada toda la información pulsamos el boton Create.
### 10. Seleccionamos el/los workspace de Databricks que deseo que esten direccionados al Metastore creado y pulsamos el boton Assign. 
### 11. Verificamos el usuario o administrador, Catalog - Metastore creado - Configurations - Usuario - Edit y verificamos o buscamos que este el usuario que coincida con el usuario de Azure Databricks y guardamos los cambios.
### Si queremos agregar otro workspace a este mismo Metastore podemos entrar a la pestaña de Workspace - Assign Workspace - Seleccionar - Assign.
## Segunda Parte: Procesamiento de Datos en Azure Databricks
### El procesamiento de los datos fue realizado en Databricks bajo el siguiente esquema:
![image](https://github.com/user-attachments/assets/011279c4-fd67-459b-839e-8c3d5b089f9f)
### 1. Fuente de Datos:
### Se realizó el Notebook $${\color{red}Data Dummy al DBFS}$$ para la generación de data dummy con el fin de simular la fuente de datos correspondiente a 3000 archivos .json con diferentes estructuras y que muestran transacciones bancarias realizadas desde diferentes fuentes: app, web y atm (cajeros).
![image](https://github.com/user-attachments/assets/dfb0632d-be26-41c8-b7c8-c834902e8d44)
### La data generada es almacenada en el Databricks File Systems (DBFS) en las siguientes rutas: "/dbfs/temp/trx_app", "/dbfs/temp/trx_web" y "/dbfs/temp/trx_atm".
### 2. Carga, Transformación y Preparación
### 2.1 Se realizó el Notebook $${\color{red}Notebook Bronze}$$ el cual lee los archivos .json guardados en el DBFS y reestructura la información contenida en cada archivo .json para que tenga la misma estructura y luego poder unirlos en un solo dataframe. Posteriormente, se unen los dataframes de las diferentes transacciones en uno solo llamado trx_bronze_df y se le agregan dos columnas de fecha de auditoria y, por último se genera la tabla delta trx_bronze en el catálogo y esquema generado para tal fin "catalgbanco.capabronze.trx_bronze". 
### Como ultima tarea, este notebook mueve todos los archivos .json a otra carpeta respado, de esta manera quedarán vacias las carpetas "/dbfs/temp/trx_app", "/dbfs/temp/trx_web" y "/dbfs/temp/trx_atm" en espera de otra ingesta por lotes de archivos y así evitar duplicidad o reprocesamiento de la data anterior. 
### Nota: Se realizó un Notebook $${\color{red}Notebook Bronze post first ingesta}$$ el cual toma en cuenta todos aquellos archivos nuevos que lleguen al DBFS luego de la primera ingesta y unirá la nueva tabla delta con la tabla delta anterior (o la generada en la primera ingesta).
### 2.2 Se realizó el Notebook $${\color{red}Notebook Silver}$$ el cual carga la tabla delta de la capa Bronze, selecciona todas las columnas del struct 'transaccion' y columnas adicionales para generar un dataframe expandido. Luego filtra el dataframe expandido por cada transacción (APP, WEB y ATM) y genera la tabla delta perteneciente a cada transacción guardandola en el catálogo y esquema destinado para tal fin: "catalgbanco.capasilver.trx_app_silver", "catalgbanco.capasilver.trx_web_silver" y "catalgbanco.capasilver.trx_atm_silver".
### 2.3 Se realizó el Notebook $${\color{red}Notebook Gold}$$ el cual mediante sparl.sql ejecuta dos queries alimentados desde las tablas de la capa silver para luego generar dos tablas delta y guardarlas en el catálogo y esquema destinados para tal fin: "catalgbanco.capagold.trx_resumen" y "catalgbanco.capagold.trx_resumen2" en las cuales se hacen una agrupación por el tipo de transacción, monto y fecha de transacción.
### 3. 

