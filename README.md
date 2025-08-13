# ETL de Turnos Médicos - SQL Server HIS a PostgreSQL

Herramienta para extraer, transformar y cargar (ETL) datos de turnos médicos desde un Sistema de Información
Hospitalaria (HIS) en SQL Server hacia una base de datos analítica en PostgreSQL. El proceso incluye la creación del
esquema de destino y la migración optimizada de los datos.

## - Propósito

Este proyecto realiza dos tareas principales:

1. Prepara la base de datos de destino PostgreSQL creando el esquema necesario de forma transaccional
2. Ejecuta el proceso ETL que:
    - Extrae datos de pacientes y turnos del HIS en SQL Server
    - Transforma y normaliza la información
    - Carga los datos en el nuevo esquema PostgreSQL

## Proceso de Limpieza de Datos

1. Extracción: conecta a SQL Server mediante PDO y obtiene un dataset de turnos con paciente, usuario y hasta 11 prestaciones por turno.
2. Confirmación: solicita confirmación explícita porque el proceso elimina y recrea el esquema de destino.
3. Transacción: inicia una transacción en PostgreSQL para garantizar atomicidad.
4. Recreación de esquema: invoca un servicio que hace DROP/CREATE del esquema, creando tablas e índices necesarios.
5. Transformación y carga:
    - Normaliza nombres y apellidos (trim, colapso de espacios, mayúsculas).
    - Infere el sexo a partir del primer nombre (si es posible).
    - Utiliza cachés en memoria para evitar duplicados de pacientes (por documento) y de prestaciones (por nombre).
    - Inserta pacientes (si no existen), turnos asociados y relaciones N:N entre turnos y prestaciones.

6. Cierre: confirma la transacción (commit), muestra barra de progreso y total procesado; ante errores, revierte (rollback) e informa el detalle.

## - Utilidades destacadas

- limpiarNombres: normaliza cadenas de nombres/apellidos.
- inferirSexo: intenta determinar el sexo probable a partir del nombre, con manejo de errores tomando en cuenta la importancia del sexo para los pacientes.
- Normaliza las prestaciones de un turno ya que las misma se encuentran en columnas diferentes de la tabla turno en lugar de usar una tabla asociativa.

## Prerrequisitos

- PHP 7.4 o superior
- Composer
- PostgreSQL 12 o superior
- SQL Server 2012 o superior
- Extensiones PHP:
    - pdo_pgsql
    - sqlsrv
    - pdo_sqlsrv

## Uso

~~~bash
symfony console app:etl-his-turnos

Inicio del Proceso de ETL de Turnos del HIS
===========================================

 ADVERTENCIA: Este proceso eliminará TODOS los datos existentes en la base de datos de análisis y recreará su estructura. ¿Desea continuar? (yes/no) [no]:
 > yes

1. Extracción de Datos
----------------------

 Obteniendo datos desde SQL Server...

                                                                                                                        
 [OK] 1000 registros extraídos.                                                                                           
                                                                                                                        

2. Transformación y Carga de Datos
----------------------------------

 Transacción iniciada en la base de datos de destino.
 Recreando esquema de la base de datos destino.
 Creación del esquema de la base de datos destino finalizada.
 10/10 [▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓] 100%

                                                                                                                        
 [OK] Proceso completado. Se han procesado e insertado 10 registros.  
~~~
