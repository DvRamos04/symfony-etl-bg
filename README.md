# symfony-etl-bg (Backend)
- ETL diario: descarga usuarios, genera JSON + CSV (detalle y summary), inserta en MySQL y cifra (.enc).
- Autocreación de BD y tablas si no existen.

## Uso
1) `composer install`
2) `copy .env.example .env` (ajusta DB si usas password)
3) Ejecuta: `"C:\xampp\php\php.exe" bin/console etl:run`

## Task Scheduler (Windows)
- Programa: `C:\xampp\php\php.exe`
- Args: `C:\Users\fbram\Downloads\Projects\symfony-etl-bg\bin\console etl:run --env=prod`
- Iniciar en: `C:\Users\fbram\Downloads\Projects\symfony-etl-bg`
