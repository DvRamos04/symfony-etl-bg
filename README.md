# symfony-etl-bg

Este repositorio es la **pieza de backend** de una prueba técnica: un **ETL en Symfony** que descarga datos públicos, los transforma, los inserta en MySQL y deja archivos **.csv / .json**. Al final, los ficheros se **cifran (AES-256-CBC)** y se **suben por SFTP** usando **clave o password** (lo dejé funcionando de las dos maneras; elige en `.env`).

> **Lo que me importaba lograr:** que un evaluador pueda clonar, poner su `.env`, ejecutar **un solo comando** y ver la base llena, con archivos generados y (si quiere) subida por SFTP.

---

## Qué hace exactamente

1. **Extract**: descarga `https://dummyjson.com/users` (para no depender de credenciales ni APIs raras).
2. **Transform**:
   - CSV de detalle `etl_YYYYMMDD.csv` (id, nombre, ciudad, “os” que aquí mapeo al título de empresa).
   - CSV de resumen `summary_YYYYMMDD.csv` con conteos por género, por rangos de edad y por “os”.
3. **Load**: inserta en MySQL:
   - `etl_execution` (una fila por corrida),
   - `etl_detail` (detalle por usuario),
   - `etl_summary` (los totales por sección).
4. **Cifrado**: genera `.enc` para `data_YYYYMMDD.json`, `etl_YYYYMMDD.csv`, `summary_YYYYMMDD.csv` con **AES-256-CBC**. Claves en `.env` (base64).
5. **SFTP**: si `SFTP_ENABLE=1`, sube los **.enc** al servidor configurado. Soporta:
   - **Clave privada (rsa/ppk)**,
   - **Password**.

---

## Requisitos

- PHP 8.x (yo lo corrí con `C:\xampp\php\php.exe`)
- Composer
- MySQL (local con XAMPP está perfecto)
- Opcional: un SFTP al que puedas subir (si no tienes, el ETL igual corre y solo saltará la subida).

---

## Variables de entorno (archivo `.env`)
```env
# BD
DB_HOST=127.0.0.1
DB_PORT=3306
DB_NAME=etl_backend
DB_USER=root
DB_PASSWORD=

# Hora
TIMEZONE=America/El_Salvador

# Cifrado (se generan si faltan; 32 bytes key / 16 bytes IV en base64)
CRYPTO_KEY_B64=...
CRYPTO_IV_B64=...

# SFTP (opcional)
SFTP_ENABLE=1
SFTP_HOST=127.0.0.1
SFTP_PORT=22
SFTP_USER=sftpuser
SFTP_PASS=
SFTP_PRIVATE_KEY_PATH=C:/Users/fbram/Downloads/Projects/symfony-etl-bg/storage/keys/id_rsa
SFTP_PRIVATE_KEY_PASSPHRASE=
SFTP_REMOTE_DIR=/upload
