# Proyecto de Motor de Ingesta

## Instalación

1. Clona el repositorio en tu máquina local.
   ```bash
   git clone <URL_DEL_REPOSITORIO>
   ```

## Puesta en Marcha

1. Asegúrate de que todos los servicios necesarios estén en funcionamiento.
2. Descarga el archivo wheel del proyecto desde el repositorio.
3. Lleva el archivo wheel generado a tu clúster de Databricks.
4. Instala el archivo wheel en tu clúster de Databricks.
5. Asegúrate de que las siguientes librerías estén instaladas en tu clúster de Databricks:
   - confluent_kafka
   - fastavro
   - httpx
   - attrs
   - authlib
   - faker
6. Sube el notebook de la carpeta `notebooks` a tu workspace de Databricks.
7. Ejecuta el notebook para verificar que todo funciona correctamente.

## Arquitectura del Proyecto

(Describe aquí la arquitectura del proyecto, incluyendo los componentes principales y cómo interactúan entre sí.)
