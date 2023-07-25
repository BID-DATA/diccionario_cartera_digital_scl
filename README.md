# Cartera digital del Sector Social - IDB

[![IDB Logo](https://scldata.iadb.org/assets/iadb-7779368a000004449beca0d4fc6f116cc0617572d549edf2ae491e9a17f63778.png)](https://scldata.iadb.org)

## Descripción y antecedentes

El repositorio cartera_digital_scl contiene los scripts necesarios para generar la información utilizada en el dashborad de cartera digital. El objetivo de la cartera digital es contar con una herramienta que mida de forma precisa y actualizada las operaciones digitales del Sector Social. Esta se compone de dos partes **1. cartera en ejecución** y **2. pipeline**

Para catalogar las operaciones de la **cartera en ejecución** se utiliza como herramienta principal un código de análisis de texto, el código se conecta a la base de datos del banco para obtener la información de las operaciones. Después de esto, el código realiza un procesamiento de texto y búsqueda de palabras, por medio de los cuales asignaremos una clasificación de acuerdo a un diccionario previamente establecido. Esto se encuentra en el código principal del repositorio titulado *cartera_digital_completo_scl.py*

En segundo lugar, para la clasificación del **pipeline** se utiliza el Checklist digital de SCL disponible para su llenado [aquí](https://www.jotform.com/form/211395055225047). Esta información se combina con la información de la base de datos del banco en el *pipeline - diccionario.py*.
 
La primera versión del diccionario utilizado para catalogar las operaciones, fue creado por el departamento de CSC del BID y luego actualizado por el front office del Sector Social. Entre los grupos se han determinado palabras clave para clasificar el tipo de operación al que se refiere el texto, es decir, clasificar en un texto tipo DIGITAL, NO DIGITAL
 
El Dashboard resultante puede ser consultado [aquí](https://app.powerbi.com/groups/c85d5888-24d0-4d69-bbfa-8e82d4e880f0/reports/4446a94f-aace-4def-9a4d-a9497b103488/ReportSection0ab4c65391af2642b8ae?ctid=9dfb1a05-5f1d-449a-8960-62abcb479e7d&experience=power-bi)

Antes de empezar a trabajar en este proyecto es importante entender la categorización que hace el Sector Social en cuanto a las operaciones digitales del sector. En particular, se clasifican las operaciones digitales con base en sus componentes. Esto se explica detalladamente en el siguiente diagrama. 

![Diagrama Cartera Digital](https://github.com/BID-DATA/diccionario_cartera_digital_scl/Inputs/operaciones_clasificacion.png)

## Estructura de trabajo
Este repositorio tiene una estructura estandarizada para el flujo de trabajo y contribución. Esta se estructura de la siguiente manera. 

  * Preliminares
  * Git Workflow
  * Documentación
  * Proceso de actualización

### Preliminares

Antes de empezar a trabajar o contribuir en la elaboración de la cartera digital se deben de seguir los siguientes pasos. 

**1. Permisos necesarios**

1. Primero, para ejecutar el código, necesitas tener instalado el ODBC driver de Data Marketplace. Deberás solicitar la instalación a ITE. Ticket [aquí](https://iadb.service-now.com/sp?id=sc_cat_item&sys_id=d7b9f6d70f8d3100d15605cce1050eb4&category=Technology%20services) Selecciona **Denodo ODBC Driver** en Software name.

2. Además, para la elaboración del pipeline, necesitas acceso a la cuenta de JotForm donde se almacena la información que llenan los especialistas en el checklist digital. Solo el administrador actual de este repositorio debe tener acceso a esta cuenta. Al asumir la administración de este repositorio, solicita la transferencia de la cuenta de JotForm. Asegúrate de cambiar las contraseñas para garantizar la seguridad de la información.

3. Adicionalmente, se debe tener acceso a la carpeta de teams de cartera digital, ya que ahí se encuentran los distintos documentos necesarios para el funcionamiento del código (diccionarios/triage). Para 2 y 3 pedir permisos a Cristina Pombo.

**2. Clonar el repositorio**

### Git Workflow ###

Para mantener estructurado y estandarizado, el repositorio tiene dos branches principales: **1. Master**
y **2. Development**. Esto ayuda a mantener la estructura de trabajo y minimizar los errores. En particular, estas dos ramas tienen las siguientes funciones.

**1. Master:** La versión contenida en esta Branch es la versión revisada más actualizada. Esta Branch está aprobada para ejecutarse, por lo que no se debe modificar a menos que todos los cambios sean previamente aprobados en la Branch de Development. 

**2. Development:** En esta Branch se realizan pruebas y cambios a los scripts. De esta Branch se desligan las Branch de cada una de los features que se generan para el trabajo en los scripts. 

Debido a que podría darse el caso de un trabajo de forma paralela entre varios desarrolladores, se requiere que cada uno trabaje con una branch personal donde se solucione o trabaje en el feature requerido y se deben seguir los siguientes pasos: 

1) Para trabajar en el feature, se debe crear una Branch que sea la copia de la versión de Development. La Branch debe tener el nombre estandarizado “type-task”.

   **a.	Type:** hace referencia a el proceso que se va a llevar a cabo (un feature, fix, refactor, test, docs, chore)
    
   **b.	Task:** Hace referencia a una breve descripción de la tarea a realizar
    
2) Una vez terminado el proceso de modificación o ajuste de los scripts se debe realizar el pull request para realizar el merge. Se debe tener en cuenta que el merge siempre se debe solicitar para realiza en la Branch de Development. 
3) Una vez se realiza la solitud de merge, se revisa y verifica que no existan errores en el nuevo pull antes de aceptar el merge a la branch principal. 

### Proceso de actualización

Las operaciones de la cartera en ejecución y pipeline sufren cambios con frecuencia, lo que requiere que el dashboard se actualice al menos una vez cada trimestre. Para mantener el dashboard al día, deberás seguir los siguientes pasos:

1. **Actualizar la cartera:** 
    - **Input:** Tener instalado el ODBC driver de Data Marketplace. (Ticket con ITE)
    - **Proceso:** Ejecuta el código `cartera_digital_completo_scl.py` una vez por trimestre.
    - **Output:** `output.xlsx`, un archivo que contiene las operaciones de SCL clasificadas como digitales y no digitales.

2. **Actualizar el pipeline:** 

Este proceso tiene dos partes 1. código 2. seguimiento de operaciones digitales. Así que se dividirán los pasos en a. y b.
   
  - **Input:** 
    
a. Tener instalado el ODBC driver de Data Marketplace. (Ticket con ITE) y el archivo Excel que almacena los resultados del checklist digital (triage digital) (Cartera Digital SCL>General>documents>inputs>`Triage_digital.xlsx`). 
**Nota** El archivo de triage se actualiza automáticamente por medio del flujo de power automate, no es necesario actualizarlo. Flujo de power automate de checklist [Aquí]()
    
b. Flujo de power automate para dar seguimiento a operaciones digitales. [Aquí](https://make.powerautomate.com/environments/Default-9dfb1a05-5f1d-449a-8960-62abcb479e7d/flows/6aa46342-35f9-49b9-8089-c26fd414040d/details)
    
  - **Proceso:** 
    
a. Ejecuta el código `pipeline - diccionario.py` una vez al mes.

b. Se actualiza manualmente el archivo `output-pipe-tabla.xlsx` con el output del siguiente punto 1 (asegurarse que todo el rango quede dentro de la tabla porque eso es lo que lee el power automate). 
    
  - **Output:** 
    
a. `output-pipe.xlsx`, un archivo que contiene las operaciones en pipeline de SCL clasificadas como digitales y no digitales.
    
b. `output-pipe-tabla.xlsx` archivo que sirve de input para el flujo de power automate que genera correo a los usuarios 30 días antes de la fecha de su POD. 
    
- **Nota:** Para **1** y **2** debes de asegurarte de cambiar las contraseñas necesarias a las tuyas (las del BID)
    en `cartera_digital_completo_scl.py` y `pipeline - diccionario.py`.
- **Nota 2:** Las operaciones en pipelina eventualmente pasaran a la cartera en ejecución. Para estas operaciones, el paso 1 en el que se clasifica la cartera en ejecución utilizando el diccionario ya no será necesario porque los especialistas ya habrán determinado si la operación es digital o no. De este modo, la clasificación realizada por los especialistas se considera más confiable y es la que se adoptará como resultado final.

3. **Ejecutar el código de KPI en R:** 
  - **Input:** 
    
  a. El archivo `DIV - revision operaciones.xlsx` para dividir las operaciones de SPH entre salud y protección social, 
      
  b. Los archivos `output.xlsx` y `output-pipe.xlsx` generados en los pasos anteriores (1 y 2).
      
  c. El archivo `KPI_division_seguimiento.xlsx` dentro de la carpeta de General>Seguimiento es donde las divisiones harán una actualización de sus KPIs.
      
  d. Esa información debe de ser trasladada (manualmente) al archivo de Input `KPI_division.xlsx` que se encuentra dentro de este repositorio
     
  - **Nota:** Antes de ejecutar este script, debes verificar si las divisiones ya han actualizado sus KPIs.
  - **Proceso:** Ejecuta el script `tabla_kpis.R` una vez por trimestre.
  - **Output:** `KPI.xlsx` Tabla final de KPIs. 
    
    Todos los archivos de Output mencionados en los pasos 1-4 se encuentran en la carpeta de Teams en la siguiente ruta (Cartera Digital SCL>General>cartera digital>Dashboard) 
    
    
4. **Actualizar PowerBi:** 
  - **Input:** Toda la información generada en los pasos anteriores.
  - **Proceso:** Abre el archivo `CarteraDigital_Report.pbix` en el folder General/cartera digital/dashboard y actualiza.      - **Output:** Dashboard actualizado en PowerBi.

5. **Revisar el dashboard resultante:** 
  - **Proceso:** Verifica que la información se ha actualizado y visualizado correctamente.

6. **Publicar el dashboard en PowerBi:** 
  - **Proceso:** Publica el dashboard actualizado. Dentro del archivo de powerbi le das publicar en SCL - Social Digital. Esto publicará el dasboard en nuestro workspace en línea y estará disponible para ser visto por todas las personas en el banco.
  - **Output:** Dashboard publicado y revisado.

- Actualmente la persona encargada de este repositorio es María Reyes Retana (mariarey@iadb.org)
