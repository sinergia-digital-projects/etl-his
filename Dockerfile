# Imagen oficial de PHP CLI (Debian Bookworm)
FROM php:8.2-cli

# Variables para evitar prompts
ENV ACCEPT_EULA=Y \
    DEBIAN_FRONTEND=noninteractive

# Dependencias del sistema:
# - libpq-dev / unixodbc-dev: cabeceras para compilar extensiones de Postgres y SQL Server
# - msodbcsql18: driver ODBC de Microsoft (requerido por sqlsrv/pdo_sqlsrv)
# - utilidades: curl, ca-certificates, gnupg (para agregar repo de Microsoft)
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg2 \
    libpq-dev \
    unixodbc-dev \
  && rm -rf /var/lib/apt/lists/*

# Agrega el repositorio de Microsoft para Debian 12 (Bookworm) y instala msodbcsql18
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
  && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
     > /etc/apt/sources.list.d/microsoft-prod.list \
  && apt-get update \
  && apt-get install -y --no-install-recommends msodbcsql18 \
  && rm -rf /var/lib/apt/lists/*

# Instala extensiones de PHP para SQL Server (sqlsrv, pdo_sqlsrv) vía PECL,
# y para PostgreSQL (pdo_pgsql, pgsql) desde las fuentes de PHP
RUN pecl install sqlsrv pdo_sqlsrv \
  && docker-php-ext-enable sqlsrv pdo_sqlsrv \
  && docker-php-ext-install -j"$(nproc)" pdo_pgsql pgsql

# (Opcional) Instala Composer si vas a usarlo dentro del contenedor
# RUN php -r "copy('https://getcomposer.org/installer', 'composer-setup.php');" \
#   && php composer-setup.php --install-dir=/usr/local/bin --filename=composer \
#   && rm composer-setup.php

# Directorio de trabajo por defecto
WORKDIR /app

# Comando por defecto (puedes cambiarlo en docker-compose o al ejecutar docker run)
CMD ["php", "-v"]
