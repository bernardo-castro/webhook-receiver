# 📡 WhatsApp Webhook Receiver & Inspector

Una API ligera, rápida y moderna diseñada específicamente para **recibir, inspeccionar y visualizar en tiempo real todos los paquetes y eventos de WhatsApp Cloud API (Meta)**, con un sistema integrado de **Auto-Ping Keep-Alive cada 1 minuto** para evitar que se apague en el plan gratuito de **Render**.

---

## ✨ Características Principales

1. **Recepción de Webhooks de Meta WhatsApp Cloud API**:
   - `GET /webhook`: Endpoint de verificación con soporte para `hub.mode`, `hub.verify_token` y `hub.challenge`.
   - `POST /webhook`: Receptor de eventos (mensajes entrantes de texto, imágenes, audios, videos, botones interactivos, estados de entrega `sent`/`delivered`/`read`/`failed`, etc.).
   - Responde inmediatamente `200 OK` para cumplir con las exigencias de Meta.

2. **Dashboard Visual en Tiempo Real (SSE)**:
   - Visualización instantánea sin recargar la página mediante *Server-Sent Events* (SSE).
   - Indicador de estado en vivo con animación de pulso.
   - Filtros por categoría: *Todos*, *Mensajes*, *Estados de Entrega*, *Sistema*.
   - Buscador en tiempo real por número de teléfono, texto, ID de mensaje o tipo.
   - Visualizador interactivo de JSON con coloreado de sintaxis y botón de un clic para copiar el JSON crudo.
   - Simulador integrado para probar eventos de WhatsApp (texto, multimedia, botones, estados) con 1 clic.
   - Exportación de todos los paquetes capturados a formato `.json`.

3. **Auto-Ping Keep-Alive (OnRender Anti-Sleep)**:
   - Temporizador en segundo plano que emite una solicitud `GET /api/ping` cada **1 minuto** (configurable).
   - Detecta automáticamente la URL pública asignada por Render (`RENDER_EXTERNAL_URL`) o permite configurarla dinámicamente desde el panel.
   - Mantiene el servicio despierto 24/7 en el plan gratuito de Render.
   - Registro en vivo de latencia y estado de cada ping en el dashboard.

---

## 🚀 Inicio Rápido Local

### 1. Instalar dependencias
```bash
npm install
```

### 2. Iniciar el servidor
```bash
npm start
```
O en modo desarrollo con auto-recarga:
```bash
npm run dev
```

El servidor estará disponible en: [http://localhost:3000](http://localhost:3000)

---

## 🌐 Despliegue en Render (Paso a Paso)

### Opción A: Despliegue mediante repositorio de GitHub / GitLab

1. Sube esta carpeta a tu repositorio de GitHub o GitLab.
2. En tu panel de [Render Dashboard](https://dashboard.render.com/), haz clic en **New +** → **Web Service**.
3. Conecta tu repositorio.
4. Configura los siguientes campos:
   - **Name**: `whatsapp-webhook-receiver` (o el nombre que prefieras)
   - **Environment**: `Node`
   - **Build Command**: `npm install`
   - **Start Command**: `npm start`
   - **Plan**: `Free`
5. En la sección **Environment Variables**, añade:
   - `WEBHOOK_VERIFY_TOKEN`: Tu token secreto (ej: `194bca0fac0c4e645513770521c9ea8c2ec2f0848ee5c1328cafe1371b891a0f`)
   - `PING_INTERVAL_MS`: `60000` *(opcional, por defecto es 1 minuto)*
6. Haz clic en **Create Web Service**.

> Render asignará una URL pública como `https://whatsapp-webhook-receiver.onrender.com`. El servicio detectará automáticamente su URL pública y comenzará el auto-ping de 1 minuto hacia sí mismo.

---

## ⚙️ Configuración en Meta for Developers

1. Ve a [Meta for Developers](https://developers.facebook.com/) → Tu App → **WhatsApp** → **Configuración** (o Webhooks).
2. En la sección **Webhook**, haz clic en **Editar**:
   - **URL de devolución de llamada (Callback URL)**:
     ```
     https://tu-app-en-render.onrender.com/webhook
     ```
   - **Token de verificación (Verify Token)**:
     ```
     194bca0fac0c4e645513770521c9ea8c2ec2f0848ee5c1328cafe1371b891a0f
     ```
     *(O el token que hayas configurado en tus variables de entorno)*
3. Haz clic en **Verificar y guardar**. Verás la confirmación inmediata en tu dashboard.
4. En **Campos de webhook**, haz clic en **Administrar** y suscríbete al campo `messages`.

---

## 🛠️ Endpoints de la API

| Método | Endpoint | Descripción |
|---|---|---|
| `GET` | `/webhook` | Verificación del Webhook de Meta (`hub.challenge`) |
| `POST` | `/webhook` | Recepción de paquetes de WhatsApp |
| `GET` | `/api/events/stream` | Canal en tiempo real (Server-Sent Events) |
| `GET` | `/api/packets` | Lista de paquetes capturados |
| `GET` | `/api/export` | Descarga de paquetes en archivo JSON |
| `DELETE` | `/api/packets` | Limpiar historial de paquetes |
| `GET` | `/api/ping` o `/health` | Endpoint de salud y Keep-Alive ping |
| `POST` | `/api/ping/trigger` | Disparar ping manual de prueba |
| `POST` | `/api/ping/config` | Actualizar URL o estado del auto-ping |
| `POST` | `/api/simulate` | Inyectar paquete simulado para pruebas |
