# Cartera digital del sector social

## Descripción
El repositorio cartera_digital_scl contiene los scripts necesarios para generar la información utilizada en el dashborad de cartera digital. En primer lugar, el código se conecta a la base de datos del banco para obtener la información de las operaciones. 

Después de esto el código realiza un procesamiento de texto y búsqueda de palabras, por medio de los cuales asignaremos una clasificación de acuerdo a un diccionario previamente establecido.
 
La primera versión de este diccionario ha sido creado el departamento de CSC del BID y luego actualizado por el front office del sector social. Entre los grupo se han determinado palabras clave para clasificar el tipo de ##operacion## al que se refiere el texto, es decir, clasificar en un texto tipo DIGITAL, NO DIGITAL
 
El Dashboard resultante puede ser consultado [aquí](https://app.powerbi.com/groups/me/reports/292b5455-fb3f-4e0a-a719-babd34bf4c2f/ReportSection5810b828c73cd57c2b25?ctid=9dfb1a05-5f1d-449a-8960-62abcb479e7d)

## Estructura de trabajo
Este repositorio tiene una estructura estandarizada para el flujo de trabajo y contribución. Esta se estructura de la siguiente manera. 

  * Preliminares
  * Git Workflow
  * Documentación

### Preliminares

Antes de empezar a trabajar o contribuir en la elaboración de la cartera digital se deben de seguir los siguientes pasos. 

**1. Permisos necesarios**

Es necesarios tener acceso a la base IBM Db2 del Banco. Para tal efecto, es necesario levantar un ticket con el equipo de seguridad. 

**2. Clonar el repositorio**

#### Git Workflow ####

Para mantener estructurado y estandarizado, el repositorio tiene dos branches principales: **1) Master**
y **2) Development**. Esto ayuda a mantener la estructura de trabajo y minimizar los errores. En particular, estas dos ramas tienen las siguientes funciones.

**1) Master:** La versión contenida en esta Branch es la versión revisada más actualizada. Esta Branch está aprobada para ejecutarse, por lo que no se debe modificar a menos que todos los cambios sean previamente aprobados en la Branch de Development. 

**2) Development:** En esta Branch se realizan pruebas y cambios a los scripts. De esta Branch se desligan las Branch de cada una de los features que se generan para el trabajo en los scripts. 

Debido a que podría darse el caso de un trabajo de forma paralela entre varios desarrolladores, se requiere que cada uno trabaje con una branch personal donde se solucione o trabaje en el feature requerido y se deben seguir los siguientes pasos: 

1) Para trabajar en el feature, se debe crear una Branch que sea la copia de la versión de Development. La Branch debe tener el nombre estandarizado “type-task”.

   **a.	Type:** hace referencia a el proceso que se va a llevar a cabo (un feature, fix, refactor, test, docs, chore)
    
   **b.	Task:** Hace referencia a una breve descripción de la tarea a realizar
    
2) Una vez terminado el proceso de modificación o ajuste de los scripts se debe realizar el pull request para realizar el merge. Se debe tener en cuenta que el merge siempre se debe solicitar para realiza en la Branch de Development. 
3) Una vez se realiza la solitud de merge, se revisa y verifica que no existan errores en el nuevo pull antes de aceptar el merge a la branch principal. 