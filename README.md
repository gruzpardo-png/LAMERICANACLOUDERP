# LA ERP Cloud — Producción y Etiquetado La Americana

MVP inicial para convertir el software local de etiquetas de La Americana en un ERP web.

Incluye:

- Login por usuario y terminal.
- Dashboard diario de producción.
- Módulo de etiquetado por kilo o precio objetivo.
- Historial de etiquetas online.
- Exportación CSV.
- Lotes/fardos.
- Importación de tabla de precios desde Excel.
- API para agente local de balanza e impresora.
- Agente local Windows opcional para COM + Xprinter.

## 1. Estructura

```text
lamericana_erp_cloud/
├── main.py                    # ERP web FastAPI
├── requirements.txt           # Dependencias del servidor Render
├── render.yaml                # Blueprint opcional de Render
├── templates/                 # Pantallas HTML
├── static/                    # CSS y JS
└── agent/
    ├── local_agent.py         # Agente local para balanza e impresora
    ├── requirements-agent.txt # Dependencias del agente Windows
    └── run_agent.bat          # Ejemplo de ejecución en Windows
```

## 2. Ejecutar localmente

Desde la carpeta del proyecto:

```bash
python -m venv .venv
.venv\Scripts\activate       # Windows
# source .venv/bin/activate   # Mac/Linux
pip install -r requirements.txt
uvicorn main:app --reload
```

Abrir:

```text
http://127.0.0.1:8000
```

Usuario inicial:

```text
usuario: gustavo
clave: 1176
terminal: T1
```

## 3. Subir a GitHub

```bash
git init
git add .
git commit -m "MVP LA ERP Cloud"
git branch -M main
git remote add origin https://github.com/TU_USUARIO/lamericana-erp-cloud.git
git push -u origin main
```

## 4. Deploy en Render — forma manual

1. Entrar a Render.
2. Crear una base de datos PostgreSQL.
3. Crear un Web Service conectado al repositorio de GitHub.
4. Configurar:

```text
Language: Python 3
Build Command: pip install -r requirements.txt
Start Command: uvicorn main:app --host 0.0.0.0 --port $PORT
```

5. Agregar variables de entorno:

```text
DATABASE_URL = internal database URL de Render PostgreSQL
SECRET_KEY = una clave larga aleatoria
AGENT_TOKEN = un token largo para conectar el agente local
APP_TIMEZONE = America/Santiago
```

## 5. Deploy en Render — con Blueprint

También puedes usar el archivo `render.yaml`.

En Render:

1. New > Blueprint.
2. Selecciona el repo.
3. Render creará el Web Service y la base PostgreSQL.
4. Revisa que `DATABASE_URL`, `SECRET_KEY` y `AGENT_TOKEN` queden creadas.

## 6. Importar precios

Entra como admin:

```text
/precios
```

Sube un Excel con hojas:

```text
vestuario
hogar
zapatillas
bolsos
```

Cada hoja debe tener columnas:

```text
Código Producto | Descripción | Precio Venta Bruto
```

## 7. Agente local Windows

El ERP en Render no puede leer directamente el puerto COM ni imprimir en una Xprinter conectada a tu PC. Para eso se usa el agente local.

En el PC de la tienda:

```bash
cd agent
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements-agent.txt
```

Edita `run_agent.bat`:

```bat
set ERP_URL=https://TU-SERVICIO.onrender.com
set AGENT_TOKEN=EL_TOKEN_DE_RENDER
set TERMINAL=T1
set COM_PORT=COM3
set BAUDRATE=9600
set PRINT_MODE=dry_run
set PRINTER_KEYWORD=XPRINTER
python local_agent.py
```

Primero prueba con:

```text
PRINT_MODE=dry_run
```

Eso no imprime; guarda PNG de prueba en `labels_out`.

Cuando esté correcto:

```text
PRINT_MODE=windows
```

## 8. Flujo operativo

1. El operador entra al ERP web.
2. Selecciona familia y método de cálculo.
3. El agente local envía peso de la balanza a la nube.
4. El ERP calcula precio y código.
5. Si se marca imprimir, queda una orden pendiente.
6. El agente local toma la orden e imprime en la Xprinter.
7. El historial queda guardado online.

## 9. Próximas mejoras recomendadas

- Módulo completo de usuarios y permisos.
- Vista de producción por lote con rentabilidad estimada.
- Control de merma.
- Etiquetas reimpresas con motivo de auditoría.
- Inventario por tienda/bodega.
- Integración con Jumpseller.
- Integración con facturación o venta POS.
