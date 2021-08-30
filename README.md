# diccionario_cartera_digital_scl

El presente código ha sido creado con el fin de realizar un procesamiento de texto y búsqueda de palabras, por medio de los cuales asignaremos una clasificación de acuerdo a un diccionario previamente establecido.
 
La primera versión de este diccionario ha sido creado el departamento de CSC del BID y luego actualizado por el front office del sector social. Entre los grupo se han determinado palabras clave para clasificar el tipo de ##operacion## al que se refiere el texto, es decir, clasificar en un texto tipo DIGITAL, NO DIGITAL
 
El codigo usa como fuente de datos las operaciones de Convergancia del Banco que son automaticamente extraidas utilizando la libreria ibm_db. Luego es analizado y clasificado el texto de las operaciones y exportado para alimentar el Dashboard de cartera digital del sector social: https://app.powerbi.com/groups/me/reports/a9483e19-bfb1-474b-89e7-f479af7fba65/ReportSection0ab4c65391af2642b8ae
